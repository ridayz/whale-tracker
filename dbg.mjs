import { DatabaseSync } from "node:sqlite";
const db = new DatabaseSync("data.sqlite");
console.log("last-hour:", db.prepare("SELECT COUNT(*) c FROM transfers WHERE ts > datetime('now','-1 hour')").get().c);
console.log("newest:", db.prepare("SELECT ts, token, usd_value FROM transfers ORDER BY ts DESC LIMIT 3").all());
