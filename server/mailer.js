import nodemailer from "nodemailer";

export async function sendEmail({ to, subject, text, html }) {
  const SMTP_URL = process.env.SMTP_URL;           // e.g. "smtp://user:pass@smtp.mailgun.org:587"
  const FROM = process.env.MAIL_FROM || "ConciergeSync <no-reply@conciergesync.ai>";

  if (!SMTP_URL) {
    // Dev mode: don't send—just echo to server logs.
    console.log("\n--- DEV EMAIL (not sent) ---");
    console.log("To:", to);
    console.log("Subject:", subject);
    console.log("Text:", text || "");
    console.log("----------------------------\n");
    return { ok: true, dev: true };
  }

  const transporter = nodemailer.createTransport(SMTP_URL);
  await transporter.sendMail({ from: FROM, to, subject, text, html });
  return { ok: true };
}
