// ops/dedupe.mjs
// Deduplicate Notion DB rows by UID, stamp "Last deduped" on kept Primary.
// Resilient to column-name casing/spacing differences and adds clear logs.

import { Client } from "@notionhq/client";

// ---------- Env ----------
const NOTION_TOKEN = process.env.NOTION_TOKEN;
const NOTION_DB_ID = process.env.NOTION_DB_ID;

if (!NOTION_TOKEN || !NOTION_DB_ID) {
  console.error("Missing NOTION_TOKEN or NOTION_DB_ID.");
  process.exit(1);
}

const UID_PROP_ENV = process.env.UID_PROP || "uid";
const PRIMARY_PROP_ENV = process.env.PRIMARY_PROP || "Primary";
const LAST_DEDUPE_ENV = process.env.LAST_DEDUPE_PROP || "Last deduped";

const AUTO_ARCHIVE = process.env.AUTO_ARCHIVE === "1";
const ARCHIVE_STATUS_NAME = process.env.ARCHIVE_STATUS_NAME || "Archived";

const notion = new Client({ auth: NOTION_TOKEN });

// ---------- Utils ----------
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const trimLower = (s) => (s || "").trim().toLowerCase();

function getPlainText(prop) {
  if (!prop) return "";
  switch (prop.type) {
    case "title": return (prop.title || []).map(t => t.plain_text).join("");
    case "rich_text": return (prop.rich_text || []).map(t => t.plain_text).join("");
    case "email": return prop.email || "";
    case "number": return prop.number != null ? String(prop.number) : "";
    case "select": return prop.select?.name || "";
    case "multi_select": return (prop.multi_select || []).map(s => s.name).join(",") || "";
    case "url": return prop.url || "";
    default: return "";
  }
}

// ---------- DB schema helpers ----------
async function getDbSchema(dbId) {
  const db = await notion.databases.retrieve({ database_id: dbId });
  console.log("Full DB response:", JSON.stringify(db, null, 2)); // 👈 log everything
  if (!db.properties) {
    console.log("⚠️ No properties found in DB response.");
    return {};
  }
  console.log("Schema dump:", Object.keys(db.properties));
  return db.properties;
}

/** Resolve a property name in the DB schema (case/space tolerant). Optionally enforce type. */
function resolvePropName(props, desiredName, wantedType /* e.g., "date" or "checkbox" */) {
  const keys = Object.keys(props);
  // Exact name first
  if (keys.includes(desiredName)) {
    if (!wantedType || props[desiredName]?.type === wantedType) return desiredName;
  }
  // Case/space-insensitive fallback
  const target = trimLower(desiredName);
  for (const k of keys) {
    if (trimLower(k) === target) {
      if (!wantedType || props[k]?.type === wantedType) return k;
    }
  }
  return null;
}

// ---------- Fetch all pages ----------
async function fetchAllPages(dbId) {
  const out = [];
  let cursor;
  let hasMore = true;
  while (hasMore) {
    const resp = await notion.databases.query({
      database_id: dbId,
      start_cursor: cursor,
      page_size: 100,
      sorts: [{ timestamp: "created_time", direction: "ascending" }],
    });
    out.push(...resp.results);
    hasMore = resp.has_more;
    cursor = resp.next_cursor;
  }
  return out;
}

// ---------- Update helpers ----------
async function markPrimaryAndStamp({
  primaryPage,
  olderPages,
  primaryProp,
  lastDedupedPropName, // may be null if not found
}) {
  const nowISO = new Date().toISOString();

  const primaryProps = {
    [primaryProp]: { checkbox: true },
  };
  if (lastDedupedPropName) {
    primaryProps[lastDedupedPropName] = { date: { start: nowISO } };
  }

  await notion.pages.update({ page_id: primaryPage.id, properties: primaryProps });
  await sleep(180);

  for (const p of olderPages) {
    const props = { [primaryProp]: { checkbox: false } };
    if (AUTO_ARCHIVE) {
      props["Status"] = { select: { name: ARCHIVE_STATUS_NAME } }; // only works if "Status" exists
    }
    await notion.pages.update({ page_id: p.id, properties: props });
    await sleep(120);
  }
}

// ---------- Main ----------
async function run() {
  console.log("Starting Notion dedupe…");
  console.log(
    JSON.stringify(
      {
        db: NOTION_DB_ID,
        uidPropDesired: UID_PROP_ENV,
        primaryPropDesired: PRIMARY_PROP_ENV,
        lastDedupedDesired: LAST_DEDUPE_ENV,
        autoArchive: AUTO_ARCHIVE,
        archiveStatusName: ARCHIVE_STATUS_NAME,
      },
      null,
      2
    )
  );

  // Resolve property names from schema (handles case mismatches)
  const props = await getDbSchema(NOTION_DB_ID);

  const UID_PROP = resolvePropName(props, UID_PROP_ENV);
  const PRIMARY_PROP = resolvePropName(props, PRIMARY_PROP_ENV, "checkbox");
  const LAST_DEDUPE_PROP = resolvePropName(props, LAST_DEDUPE_ENV, "date");

  if (!UID_PROP) {
    console.error(`UID property "${UID_PROP_ENV}" not found in DB schema.`);
    process.exit(1);
  }
  if (!PRIMARY_PROP) {
    console.error(`Primary checkbox property "${PRIMARY_PROP_ENV}" not found in DB schema.`);
    process.exit(1);
  }
  if (!LAST_DEDUPE_PROP) {
    console.warn(
      `Date property "${LAST_DEDUPE_ENV}" not found (or not a Date). Will update Primary only.`
    );
  }

  console.log(
    JSON.stringify(
      {
        uidPropResolved: UID_PROP,
        primaryPropResolved: PRIMARY_PROP,
        lastDedupedResolved: LAST_DEDUPE_PROP || null,
      },
      null,
      2
    )
  );

  const pages = await fetchAllPages(NOTION_DB_ID);
  console.log(`Fetched ${pages.length} pages.`);

  // Group by UID
  const groups = new Map();
  for (const page of pages) {
    const prop = page.properties?.[UID_PROP];
    const uid = getPlainText(prop).trim();
    if (!uid) continue;
    if (!groups.has(uid)) groups.set(uid, []);
    groups.get(uid).push(page);
  }
  console.log(`Found ${groups.size} UID groups.`);

  let touched = 0;
  for (const [uid, list] of groups.entries()) {
    // Created ascending already; newest is last
    const primary = list[list.length - 1];
    const older = list.slice(0, -1);

    try {
      await markPrimaryAndStamp({
        primaryPage: primary,
        olderPages: older,
        primaryProp: PRIMARY_PROP,
        lastDedupedPropName: LAST_DEDUPE_PROP, // null = skip stamping
      });

      console.log(
        `[uid: ${uid}] keep=${primary.id} older=${older.map(p => p.id).join(",") || "—"} ${LAST_DEDUPE_PROP ? "(stamped Last deduped)" : "(no Last deduped column)"}`
      );
      touched++;
      if (touched % 25 === 0) console.log(`Processed ${touched} groups…`);
    } catch (e) {
      console.error(`[uid: ${uid}] update failed:`, e?.message || e);
    }
  }

  console.log(`Done. Processed ${touched} groups.`);
}

run().catch((e) => {
  console.error("Fatal error:", e?.message || e);
  process.exit(1);
});
