// Runs on Node 20+ (no packages). Uses fetch to call Notion API.
const {
  NOTION_TOKEN,          // secret: Notion integration token
  NOTION_DB_ID,          // your database id
  UID_PROP = "uid",      // text/rich_text property name holding your cs_... id
  PRIMARY_PROP = "Primary", // checkbox property name
  NOTION_VERSION = "2022-06-28"
} = process.env;

if (!NOTION_TOKEN || !NOTION_DB_ID) {
  console.error("Missing NOTION_TOKEN or NOTION_DB_ID"); process.exit(1);
}

const H = {
  "Authorization": `Bearer ${NOTION_TOKEN}`,
  "Notion-Version": NOTION_VERSION,
  "Content-Type": "application/json"
};

async function notionQuery(cursor) {
  const body = {
    page_size: 100,
    sorts: [{ timestamp: "created_time", direction: "descending" }],
    ...(cursor ? { start_cursor: cursor } : {})
  };
  const res = await fetch(`https://api.notion.com/v1/databases/${NOTION_DB_ID}/query`, {
    method: "POST", headers: H, body: JSON.stringify(body)
  });
  if (!res.ok) throw new Error(`Query failed: ${res.status} ${await res.text()}`);
  return res.json();
}

async function notionUpdate(page_id, props) {
  const res = await fetch(`https://api.notion.com/v1/pages/${page_id}`, {
    method: "PATCH", headers: H, body: JSON.stringify({ properties: props })
  });
  if (!res.ok) throw new Error(`Update failed: ${res.status} ${await res.text()}`);
  return res.json();
}

function readUid(page) {
  const p = page.properties?.[UID_PROP];
  if (!p) return "";
  if (p.type === "rich_text") return (p.rich_text || []).map(t=>t.plain_text).join("").trim();
  if (p.type === "title")     return (p.title || []).map(t=>t.plain_text).join("").trim();
  if (p.type === "url")       return p.url || "";
  if (p.type === "email")     return p.email || "";
  if (p.type === "number")    return String(p.number ?? "");
  if (p.type === "formula")   return p.formula?.string || "";
  return "";
}

function isPrimary(page) {
  const p = page.properties?.[PRIMARY_PROP];
  return p?.type === "checkbox" ? !!p.checkbox : false;
}

async function sleep(ms){ return new Promise(r=>setTimeout(r, ms)); }

async function run() {
  // 1) fetch all pages
  const pages = [];
  let cursor; 
  do {
    const data = await notionQuery(cursor);
    pages.push(...data.results);
    cursor = data.has_more ? data.next_cursor : undefined;
  } while (cursor);

  // 2) group by uid (ignore empties)
  const groups = new Map();
  for (const p of pages) {
    const uid = readUid(p);
    if (!uid) continue;
    if (!groups.has(uid)) groups.set(uid, []);
    groups.get(uid).push(p);
  }

  // 3) for each uid with >1, mark newest Primary=true, others false
  let touched = 0, uids = 0;
  for (const [uid, group] of groups) {
    uids++;
    if (group.length <= 1) continue; // nothing to do

    // group is already in created_time DESC because of query sort
    const newest = group[0], older = group.slice(1);

    // newest → Primary = true
    if (!isPrimary(newest)) {
      await notionUpdate(newest.id, { [PRIMARY_PROP]: { checkbox: true } });
      await sleep(350);
      touched++;
    }
    // older → Primary = false
    for (const p of older) {
      if (isPrimary(p)) {
        await notionUpdate(p.id, { [PRIMARY_PROP]: { checkbox: false } });
        await sleep(350);
        touched++;
      }
    }
  }

  console.log(`Examined ${uids} UIDs. Updated ${touched} pages.`);
}

run().catch(e => { console.error(e); process.exit(1); });
