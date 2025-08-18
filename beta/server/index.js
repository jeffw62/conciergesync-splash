// Minimal beta auth + sessions for ConciergeSync
// Node 18+ (uses crypto.randomUUID). Run with: node server/index.js
// Static /beta is served, API mounted at /api.

import express from "express";
import cookieParser from "cookie-parser";
import crypto from "crypto";
import path from "path";
import { fileURLToPath } from "url";
import { sendEmail } from "./mailer.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
app.use(express.json());
app.use(cookieParser());

// ---------- Config (env) ----------
const SESSION_SECRET = process.env.SESSION_SECRET || "dev-change-me";
const ORIGIN = process.env.PUBLIC_ORIGIN || "http://localhost:3000";
const PORT = Number(process.env.PORT || 3000);
const DEV_ECHO_EMAIL = process.env.DEV_ECHO_EMAIL === "1"; // show codes in server logs
const CODE_TTL_SEC = 10 * 60; // 10 minutes
const LINK_TTL_SEC = 15 * 60; // 15 minutes
const SESSION_MAX_AGE_SEC = 30 * 24 * 60 * 60; // 30 days

// ---------- In-memory stores (ok for beta) ----------
const OTP_STORE = new Map();    // tokenId -> { email, codeHash, exp, tries }
const LINK_STORE = new Map();   // linkId  -> { email, exp }
const USERS = new Map();        // email   -> { email, firstName?, lastName?, createdAt }

setInterval(() => {
  const now = Date.now() / 1000;
  for (const [id, rec] of OTP_STORE) if (rec.exp <= now) OTP_STORE.delete(id);
  for (const [id, rec] of LINK_STORE) if (rec.exp <= now) LINK_STORE.delete(id);
}, 60_000);

// ---------- Tiny rate limiter (per IP & per email) ----------
const buckets = new Map(); // key -> { count, reset }
function limit(key, max, windowSec) {
  const now = Date.now() / 1000;
  const b = buckets.get(key) ?? { count: 0, reset: now + windowSec };
  if (now > b.reset) { b.count = 0; b.reset = now + windowSec; }
  b.count += 1;
  buckets.set(key, b);
  return b.count <= max;
}

// ---------- Helpers ----------
const b64u = (b) => Buffer.from(b).toString("base64url");
function signSession(payload) {
  const body = b64u(JSON.stringify(payload));
  const sig = crypto.createHmac("sha256", SESSION_SECRET).update(body).digest("base64url");
  return `${body}.${sig}`;
}
function verifySession(token) {
  if (!token) return null;
  const [body, sig] = String(token).split(".");
  const good = crypto.createHmac("sha256", SESSION_SECRET).update(body).digest("base64url");
  if (sig !== good) return null;
  try {
    const data = JSON.parse(Buffer.from(body, "base64url").toString("utf8"));
    if (data.exp && data.exp < Math.floor(Date.now() / 1000)) return null;
    return data;
  } catch { return null; }
}
const setSessionCookie = (res, data) => {
  const cookie = signSession(data);
  res.cookie("cs_session", cookie, {
    httpOnly: true, secure: true, sameSite: "lax", path: "/",
    maxAge: SESSION_MAX_AGE_SEC * 1000,
  });
};
const clearSessionCookie = (res) =>
  res.cookie("cs_session", "", { httpOnly: true, secure: true, sameSite: "lax", path: "/", maxAge: 0 });

const normalizeEmail = (s) => String(s || "").trim().toLowerCase();
const codeHash = (code) => crypto.createHash("sha256").update(String(code)).digest("hex");
const randomDigits = (n=6) => {
  // Cryptographically strong 6-digit code with leading zeros allowed
  const max = 10 ** n;
  const num = crypto.randomInt(0, max);
  return num.toString().padStart(n, "0");
};

// ---------- API: start (login/register/link) ----------
app.post("/api/auth/start", async (req, res) => {
  const ip = req.headers["x-forwarded-for"]?.toString().split(",")[0]?.trim() || req.socket.remoteAddress || "ip";
  if (!limit(`ip:${ip}`, 30, 60)) return res.status(429).json({ ok:false, error:"Too many requests. Please wait a minute." });

  const { mode, email, profile } = req.body || {};
  const e = normalizeEmail(email);
  if (!e || !/^\S+@\S+\.\S+$/.test(e)) return res.status(400).json({ ok:false, error:"Valid email required." });

  if (!limit(`email:${e}`, 10, 60)) return res.status(429).json({ ok:false, error:"Please try again shortly." });

  // Ensure user record exists on register; on login it's created on verify if missing.
  if (mode === "register") {
    if (!USERS.has(e)) USERS.set(e, { email: e, ...profile, createdAt: new Date().toISOString() });
  }

  if (mode === "link") {
    // magic link
    const linkId = crypto.randomUUID();
    const exp = Math.floor(Date.now() / 1000) + LINK_TTL_SEC;
    LINK_STORE.set(linkId, { email: e, exp });
    const link = `${ORIGIN}/api/auth/callback?token=${encodeURIComponent(linkId)}`;
    await sendEmail({
      to: e,
      subject: "Your ConciergeSync magic link",
      html: `<p>Click to sign in:</p><p><a href="${link}">${link}</a></p><p>This link expires in 15 minutes.</p>`
    });
    if (DEV_ECHO_EMAIL) console.log(`[DEV] Magic link for ${e}: ${link}`);
    return res.json({ ok: true });
  }

  // email code (login/register)
  const code = randomDigits(6);
  const tokenId = crypto.randomUUID();
  const exp = Math.floor(Date.now() / 1000) + CODE_TTL_SEC;
  OTP_STORE.set(tokenId, { email: e, codeHash: codeHash(code), exp, tries: 0 });

  await sendEmail({
    to: e,
    subject: "Your ConciergeSync sign-in code",
    html: `
      <p>Your code is:</p>
      <p style="font-size:28px;font-weight:800;letter-spacing:6px">${code}</p>
      <p>This code expires in 10 minutes.</p>
    `
  });
  if (DEV_ECHO_EMAIL) console.log(`[DEV] OTP for ${e}: ${code} (tokenId ${tokenId})`);

  return res.json({ ok: true, tokenId });
});

// ---------- API: verify OTP ----------
app.post("/api/auth/verify", async (req, res) => {
  const { tokenId, code } = req.body || {};
  const rec = OTP_STORE.get(String(tokenId || ""));
  if (!rec) return res.status(400).json({ ok:false, error:"Code expired or invalid. Please request a new one." });

  const now = Math.floor(Date.now() / 1000);
  if (rec.exp < now) { OTP_STORE.delete(String(tokenId)); return res.status(400).json({ ok:false, error:"Code expired. Request a new one." }); }
  rec.tries += 1;
  if (rec.tries > 6) { OTP_STORE.delete(String(tokenId)); return res.status(429).json({ ok:false, error:"Too many attempts." }); }

  if (codeHash(code) !== rec.codeHash) return res.status(400).json({ ok:false, error:"Incorrect code." });

  OTP_STORE.delete(String(tokenId));
  // Upsert user
  if (!USERS.has(rec.email)) USERS.set(rec.email, { email: rec.email, createdAt: new Date().toISOString() });

  // Issue session
  const session = { sub: rec.email, iat: Math.floor(Date.now() / 1000), exp: Math.floor(Date.now()/1000) + SESSION_MAX_AGE_SEC };
  setSessionCookie(res, session);
  return res.json({ ok: true });
});

// ---------- API: magic link callback ----------
app.get("/api/auth/callback", (req, res) => {
  const token = String(req.query.token || "");
  const rec = LINK_STORE.get(token);
  if (!rec) return res.status(400).send("Link invalid or expired.");
  LINK_STORE.delete(token);
  const session = { sub: rec.email, iat: Math.floor(Date.now()/1000), exp: Math.floor(Date.now()/1000)+SESSION_MAX_AGE_SEC };
  setSessionCookie(res, session);
  res.redirect("/beta/02-agree.html?enter=1");
});

// ---------- Session helpers ----------
app.get("/api/auth/me", (req, res) => {
  const sess = verifySession(req.cookies.cs_session);
  if (!sess) return res.json({ ok:false });
  const user = USERS.get(sess.sub) || { email: sess.sub };
  res.json({ ok:true, user:{ email: user.email, firstName: user.firstName || null, lastName: user.lastName || null } });
});
app.post("/api/auth/logout", (req, res) => { clearSessionCookie(res); res.json({ ok:true }); });

// ---------- Static site ----------
app.use("/beta", express.static(path.join(__dirname, "..", "beta"), { extensions: ["html"] }));
app.get("/", (_req, res) => res.redirect("/beta/01-index.html"));

app.listen(PORT, () => {
  console.log(`ConciergeSync beta auth on ${ORIGIN} (PORT ${PORT})`);
});
