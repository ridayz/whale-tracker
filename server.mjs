import http from "node:http";
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";

const DIR = path.dirname(fileURLToPath(import.meta.url));
const PORT = 8933;
const now = () => new Date().toISOString();
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// Thresholds (USD)
const WHALE_TX_USD = Number(process.env.WHALE_TX_USD || 1000);
const WHALE_INFLOW_USD = Number(process.env.WHALE_INFLOW_USD || 10000);
const POLL_MS = 45000;

const { DatabaseSync } = await import("node:sqlite");
const db = new DatabaseSync(path.join(DIR, "data.sqlite"));
db.exec(`CREATE TABLE IF NOT EXISTS wallets(address TEXT PRIMARY KEY, chain TEXT DEFAULT 'solana', label TEXT DEFAULT 'retail', tag_manual TEXT DEFAULT '', balance_usd REAL DEFAULT 0, last_sig TEXT DEFAULT '', updated_at TEXT);
CREATE TABLE IF NOT EXISTS transfers(sig TEXT PRIMARY KEY, wallet TEXT, chain TEXT DEFAULT 'solana', token TEXT, amount REAL, usd_value REAL, direction TEXT, ts TEXT);
CREATE INDEX IF NOT EXISTS idx_transfers_wallet_ts ON transfers(wallet, ts);
CREATE TABLE IF NOT EXISTS alerts(id INTEGER PRIMARY KEY AUTOINCREMENT, wallet TEXT, transfer_sig TEXT, rule TEXT, msg TEXT, ack INTEGER DEFAULT 0, ts TEXT);
CREATE TABLE IF NOT EXISTS prices(token TEXT PRIMARY KEY, price REAL, updated_at TEXT);`);

function seed() {
  if (!db.prepare("SELECT COUNT(*) c FROM prices").get().c) {
    db.prepare("INSERT INTO prices VALUES(?,?,?)").run("WIF", 0.231, now());
    db.prepare("INSERT INTO prices VALUES(?,?,?)").run("BONK", 0.000025, now());
    db.prepare("INSERT INTO prices VALUES(?,?,?)").run("POPCAT", 0.8, now());
  }
}
seed();

const send = (res, code, obj, type = "application/json") => { res.writeHead(code, { "Content-Type": type }); res.end(typeof obj === "string" ? obj : JSON.stringify(obj)); };
const body = (req) => new Promise((r) => { let d = ""; req.on("data", (c) => d += c); req.on("end", () => { try { r(JSON.parse(d || "{}")); } catch { r({}); } }); });

// Mint asli token meme Solana (untuk harga real via DexScreener, tanpa key)
const TOKEN_MINTS = {
  WIF: "EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm",
  BONK: "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB4M",
  POPCAT: "7GCihgDB8FEcEwUQx9ioCaaxpTe8TvUoLM2jEjyTwk7",
};
const poolCache = {}; // symbol -> pool address (auto-discovery via GeckoTerminal)
const MINT2SYM = Object.fromEntries(Object.entries(TOKEN_MINTS).map(([s, m]) => [m, s]));
let memeCache = { ts: 0, data: [] };
let prevMeme = {}; // mint -> {liq, flags[]} buat deteksi perubahan antar fetch
// Audit keamanan via RugCheck gratis (tanpa key, Solana): mint/freeze authority, holder, rugged, risks.
// Ini sumber industrinya — BUKAN angka karangan. Flow Dex (SELL-DOM/DUMP/LIQ-DRAIN) hanya data deskriptif.
async function pollRug() {
  const queue = db.prepare("SELECT * FROM tracked WHERE chain='solana' ORDER BY last_polled ASC LIMIT 3").all();
  for (const q of queue) {
    try {
      const r = await fetch(`https://api.rugcheck.xyz/v1/tokens/${q.mint}/report`);
      if (r.status === 429) { await sleep(15000); break; }
      if (!r.ok) continue;
      const j = await r.json();
      const risks = j?.risks || [];
      const prev = db.prepare("SELECT risks FROM rug WHERE mint=?").get(q.mint);
      const prevNames = new Set(JSON.parse(prev?.risks || "[]").map((x) => x.name));
      db.prepare("INSERT OR REPLACE INTO rug VALUES(?,?,?,?,?,?,?,?)").run(
        q.mint, j.rugged ? 1 : 0, j.score ?? null, JSON.stringify(risks),
        j.topHolders?.[0]?.pct ?? null,
        j.mintAuthority ? "ON" : "OFF", j.freezeAuthority ? "ON" : "OFF", now()
      );
      for (const rk of risks.filter((x) => x.level === "danger" && !prevNames.has(x.name))) {
        db.prepare("INSERT INTO alerts(wallet,rule,msg,ts) VALUES(?,?,?,?)").run(q.mint, "rug-danger", `🔴 ${q.symbol}: ${rk.name} — ${String(rk.description || "").slice(0, 120)}`, now());
      }
      if (j.rugged) db.prepare("INSERT INTO alerts(wallet,rule,msg,ts) VALUES(?,?,?,?)").run(q.mint, "rug-rugged", `💀 ${q.symbol} flagged RUGGED oleh RugCheck`, now());
    } catch (e) { console.log("[rug-err]", q.symbol, e.message); }
    await sleep(2000);
  }
}
let metaCache = { ts: 0, data: [] };
let ofCache = {}; // symbol -> {ts, data} orderflow L2 (KuCoin, tanpa key)
// L2 per symbol di KuCoin (gratis, tanpa key)
const KC_SYM = { WIF: "WIF-USDT", BONK: "BONK-USDT", POPCAT: "POPCAT-USDT", BTC: "BTC-USDT", XAU: "PAXG-USDT" };
// Fallback Coinbase (gratis, tanpa key) kalau KuCoin rate-limit
const CB_SYM = { WIF: "WIF-USD", BONK: "BONK-USD", BTC: "BTC-USD", XAU: "PAXG-USD" };
let wallPrev = {}; // symbol -> walls snapshot sebelumnya (deteksi spoof: tembok muncul/hilang)
// Orderflow + heatmap L2: walls institusi, imbalance, prints gede, spoof
async function orderflow(sym) {
  const kc = KC_SYM[sym];
  if (!kc) return { error: "unknown symbol" };
  const c = ofCache[sym];
  if (c && Date.now() - c.ts < 15000) return c.data;
  const calcFlow = (tr) => {
    let bv = 0, sv = 0, nb = 0, ns = 0, last = null;
    const prints = [];
    for (const t of tr) {
      const usd = Number(t.price) * Number(t.size);
      last = Number(t.price) || last;
      if (t.side === "buy") { bv += usd; nb++; } else { sv += usd; ns++; }
      if (usd >= 1000) prints.push({ price: Number(t.price), usd: Math.round(usd), side: t.side, ts: t.time });
    }
    return { buyVol: Math.round(bv), sellVol: Math.round(sv), nBuy: nb, nSell: ns, imbalance: bv + sv > 0 ? Math.round((bv / (bv + sv)) * 100) : 50, prints: prints.slice(0, 10), last };
  };
  const degenerate = (f) => f && (f.nBuy + f.nSell) >= 20 && (f.nBuy === 0 || f.nSell === 0);
  const out = { symbol: sym, bids: [], asks: [], walls: [], flow: null, spoof: [], src: "", last: null, bestBid: null, bestAsk: null };
  const WALL_USD = sym === "BTC" || sym === "XAU" ? 100000 : 10000;
  const px = (lvl) => ({ price: Number(lvl[0]), size: Number(lvl[1]), usd: Number(lvl[0]) * Number(lvl[1]) });
  const calcWalls = () => {
    const walls = [...out.bids.map((x) => ({ ...x, side: "BID" })), ...out.asks.map((x) => ({ ...x, side: "ASK" }))].filter((x) => x.usd >= WALL_USD);
    out.walls = walls;
    const prevKeys = new Set((wallPrev[sym] || []).map((w) => w.price + w.side));
    for (const w of walls) {
      if (!prevKeys.has(w.price + w.side)) {
        out.spoof.push({ event: "WALL-MUNCUL", ...w });
        if (w.usd >= WALL_USD * 2) db.prepare("INSERT INTO alerts(wallet,rule,msg,ts) VALUES(?,?,?,?)").run(sym, "wall", `🧱 WALL ${w.side} $${Math.round(w.usd).toLocaleString()} ${sym} @${w.price}`, now());
      }
    }
    const curKeys = new Set(walls.map((w) => w.price + w.side));
    for (const w of (wallPrev[sym] || [])) {
      if (!curKeys.has(w.price + w.side)) out.spoof.push({ event: "WALL-HILANG", ...w });
    }
    wallPrev[sym] = walls;
  };
  try {
    const b = await fetch(`https://api.kucoin.com/api/v1/market/orderbook/level2_20?symbol=${kc}`);
    if (b.ok) {
      const bj = await b.json();
      out.bids = (bj?.data?.bids || []).slice(0, 20).map(px);
      out.asks = (bj?.data?.asks || []).slice(0, 20).map(px);
      out.src = "kucoin";
      calcWalls();
    }
    const h = await fetch(`https://api.kucoin.com/api/v1/market/histories?symbol=${kc}`);
    if (h.ok) {
      const hj = await h.json();
      out.flow = calcFlow((hj?.data || []).slice(0, 50));
      // Data aneh (100% satu sisi, size seragam = feed bot/ladder) → jangan dipercaya, ambil Coinbase
      if (degenerate(out.flow)) { out.flow = null; out.src += "|kucoin-flow-suspect"; }
    }
  } catch (e) { console.log("[of-err]", sym, e.message); }
  if ((!out.bids.length || !out.flow) && CB_SYM[sym]) {
    // Fallback Coinbase
    try {
      const cb = CB_SYM[sym];
      const b = await fetch(`https://api.exchange.coinbase.com/products/${cb}/book?level=2`);
      if (b.ok && !out.bids.length) {
        const bj = await b.json();
        out.bids = (bj?.bids || []).slice(0, 20).map(px);
        out.asks = (bj?.asks || []).slice(0, 20).map(px);
        out.src = (out.src ? out.src + "+" : "") + "coinbase-book";
        calcWalls();
      }
      const h = await fetch(`https://api.exchange.coinbase.com/products/${cb}/trades?limit=50`);
      if (h.ok && !out.flow) {
        out.flow = calcFlow(await h.json());
        out.src = (out.src ? out.src + "+" : "") + "coinbase-trades";
      }
    } catch (e) { console.log("[of-cb-err]", sym, e.message); }
  }
  ofCache[sym] = { ts: Date.now(), data: out };
  if (out.bids.length) { out.bestBid = out.bids[0].price; out.bestAsk = out.asks[0]?.price ?? null; }
  if (out.flow?.last) out.last = out.flow.last;
  else if (out.bids.length) out.last = (out.bids[0].price + (out.asks[0]?.price || out.bids[0].price)) / 2;
  return out;
}
let macroCache = { ts: 0, data: [] };
// Macro gratis (tanpa key): BTC (WBTC) + XAU (PAXG gold) via DexScreener — Binance diblokir dari sini
async function macro() {
  if (Date.now() - macroCache.ts < 60000 && macroCache.data.length) return macroCache.data;
  const out = [];
  try {
    const r = await fetch("https://api.dexscreener.com/latest/dex/tokens/0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C,0x45804880De22913D8fF876C39cB87a9E06530e3fC");
    if (r.ok) {
      const j = await r.json();
      const pick = (sym) => (j?.pairs || []).filter((p) => p.baseToken.symbol.includes(sym)).sort((a, b) => (b.liquidity?.usd || 0) - (a.liquidity?.usd || 0))[0];
      const btc = pick("WBTC"), xau = pick("PAXG");
      if (btc) out.push({ label: "BTC", icon: "🟠", price: Number(btc.priceUsd), change24h: Number(btc.priceChange?.h24 || 0), vol: btc.volume?.h24 || 0 });
      if (xau) out.push({ label: "XAU", icon: "🟡", price: Number(xau.priceUsd), change24h: Number(xau.priceChange?.h24 || 0), vol: xau.volume?.h24 || 0 });
    }
  } catch (e) { console.log("[macro-err]", e.message); }
  if (!out.length) {
    try { // fallback CoinGecko gratis
      const g = await fetch("https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,pax-gold&vs_currencies=usd&include_24hr_change=true");
      if (g.ok) {
        const gj = await g.json();
        if (gj.bitcoin) out.push({ label: "BTC", icon: "🟠", price: gj.bitcoin.usd, change24h: gj.bitcoin.usd_24h_change || 0, vol: 0 });
        if (gj["pax-gold"]) out.push({ label: "XAU", icon: "🟡", price: gj["pax-gold"].usd, change24h: gj["pax-gold"].usd_24h_change || 0, vol: 0 });
      }
    } catch {}
  }
  // Tempel imbalance orderflow (L2 real) biar BTC/XAU ada sinyalnya juga, bukan cuma harga
  try {
    for (const m of out) {
      const of = await orderflow(m.label);
      if (of?.flow) {
        m.imb = of.flow.imbalance;
        m.flowNote = `flow ${of.flow.nBuy}v${of.flow.nSell} · src ${of.src}`;
        const up = (m.change24h || 0) >= 0;
        if (up && of.flow.imbalance >= 60) m.regime = "RISK-ON 🟢";
        else if (!up && of.flow.imbalance <= 40) m.regime = "RISK-OFF 🔴";
        else m.regime = "CAMPURAN 🟡";
      }
    }
  } catch {}
  if (out.length) macroCache = { ts: Date.now(), data: out };
  return out.length ? out : macroCache.data;
}
// CTO feed (gratis): takeover komunitas = sering mendahului pump → auto-track + alert
async function pollCTO() {
  try {
    const r = await fetch("https://api.dexscreener.com/community-takeovers/latest/v1");
    if (!r.ok) return;
    const j = await r.json();
    for (const c of (Array.isArray(j) ? j : []).filter((x) => x.chainId === "solana").slice(0, 10)) {
      if (!c.tokenAddress) continue;
      const known = db.prepare("SELECT 1 FROM tracked WHERE mint=?").get(c.tokenAddress);
      trackMint(c.tokenAddress, "?");
      if (!known) {
        db.prepare("INSERT INTO alerts(wallet,rule,msg,ts) VALUES(?,?,?,?)").run(c.tokenAddress, "cto-new", `🆕 CTO: ${c.tokenAddress.slice(0, 8)}… ${String(c.description || "").slice(0, 80)}`, now());
      }
    }
  } catch (e) { console.log("[cto-err]", e.message); }
}
db.exec(`CREATE TABLE IF NOT EXISTS tracked(mint TEXT PRIMARY KEY, symbol TEXT, chain TEXT DEFAULT 'solana', last_polled TEXT DEFAULT '')`);
try { db.prepare("ALTER TABLE tracked ADD COLUMN chain TEXT DEFAULT 'solana'").run(); } catch {}
db.exec(`CREATE TABLE IF NOT EXISTS config(key TEXT PRIMARY KEY, value TEXT)`);
db.exec(`CREATE TABLE IF NOT EXISTS rug(mint TEXT PRIMARY KEY, rugged INTEGER, score INTEGER, risks TEXT, topPct REAL, mintAuth TEXT, freezeAuth TEXT, updated_at TEXT)`);
function getCfg(k, fb = "") {
  try { return db.prepare("SELECT value v FROM config WHERE key=?").get(k)?.v ?? process.env[k] ?? fb; }
  catch { return process.env[k] ?? fb; }
}
function shyftKey() { return getCfg("SHYFT_KEY", ""); }
function trackMint(mint, symbol, chain = "solana") {
  try { db.prepare("INSERT OR IGNORE INTO tracked(mint,symbol,chain) VALUES(?,?,?)").run(mint, symbol && symbol !== "?" ? symbol : mint.slice(0, 8), chain); } catch {}
}
// GeckoTerminal network per chain (robinhood & co belum didukung → price-only via Dex)
const GECKO_NET = { solana: "solana", ethereum: "eth", bsc: "bsc", base: "base", polygon: "polygon_pos", arbitrum: "arbitrum", avalanche: "avax" };
let sigCache = { ts: 0, data: [] };
// Sinyal informatif: agregat trade real 60 mnt terakhir per token → GASS BUY / GASS SELL / WATCH
async function signals() {
  if (Date.now() - sigCache.ts < 30000 && sigCache.data.length) return sigCache.data;
  const out = [];
  try {
    const since = new Date(Date.now() - 60 * 60 * 1000).toISOString();
    const since15 = new Date(Date.now() - 15 * 60 * 1000).toISOString();
    const rows = db.prepare("SELECT token, direction, COUNT(*) n, COALESCE(SUM(usd_value),0) v, COALESCE(SUM(CASE WHEN usd_value>=? THEN 1 ELSE 0 END),0) wn, COALESCE(SUM(CASE WHEN usd_value>=? THEN usd_value ELSE 0 END),0) wv FROM transfers WHERE ts>? GROUP BY token, direction").all(WHALE_TX_USD, WHALE_TX_USD, since);
    const rows15 = db.prepare("SELECT token, direction, COALESCE(SUM(usd_value),0) v FROM transfers WHERE ts>? GROUP BY token, direction").all(since15);
    const m15 = {};
    for (const r of rows15) {
      m15[r.token] = m15[r.token] || { b: 0, s: 0 };
      if (r.direction === "IN") m15[r.token].b += r.v; else m15[r.token].s += r.v;
    }
    const byTok = {};
    for (const r of rows) {
      byTok[r.token] = byTok[r.token] || { symbol: r.token, buyVol: 0, sellVol: 0, nBuy: 0, nSell: 0, wBuy: 0, wSell: 0, wBuyVol: 0, wSellVol: 0 };
      const t = byTok[r.token];
      if (r.direction === "IN") { t.buyVol += r.v; t.nBuy += r.n; t.wBuy += r.wn; t.wBuyVol += r.wv; }
      else { t.sellVol += r.v; t.nSell += r.n; t.wSell += r.wn; t.wSellVol += r.wv; }
    }
    for (const sym of Object.keys(byTok)) {
      const t = byTok[sym];
      const net = t.buyVol - t.sellVol;
      const tot = t.buyVol + t.sellVol;
      const pressure = tot > 0 ? t.buyVol / tot : 0.5;
      let signal = "WATCH", strength = 1;
      const reasons = [];
      if (t.wBuy > 0) reasons.push(`${t.wBuy}x whale buy $${Math.round(t.wBuyVol).toLocaleString()}`);
      if (t.wSell > 0) reasons.push(`${t.wSell}x whale sell $${Math.round(t.wSellVol).toLocaleString()}`);
      reasons.push(`buy pressure ${Math.round(pressure * 100)}% (${t.nBuy} vs ${t.nSell} trade)`);
      if (net > 0) reasons.push(`net inflow +$${Math.round(net).toLocaleString()}/jam`);
      else if (net < 0) reasons.push(`net outflow -$${Math.round(Math.abs(net)).toLocaleString()}/jam`);
      if (net > 0 && pressure >= 0.6 && (t.wBuy > 0 || net >= WHALE_TX_USD)) signal = "GASS BUY";
      else if (net < 0 && pressure <= 0.4 && (t.wSell > 0 || Math.abs(net) >= WHALE_TX_USD)) signal = "GASS SELL";
      const mag = Math.abs(net);
      strength = mag >= 100000 ? 5 : mag >= 50000 ? 4 : mag >= 25000 ? 3 : mag >= 10000 ? 2 : 1;
      if ((t.wBuy + t.wSell) >= 3 && strength < 5) strength++;
      // Anti exit-liquidity: harga rata-rata entry whale 1 jam terakhir vs harga sekarang.
      // Kalau whale udah +150% dan kamu baru mau masuk = kamu likuiditasnya. Sinyal BUY dibatalkan.
      let whaleAvgChg = null;
      try {
        const ab = db.prepare("SELECT COALESCE(SUM(usd_value),0) su, COALESCE(SUM(amount),0) sa FROM transfers WHERE token=? AND direction='IN' AND ts>?").get(sym, since);
        const px = db.prepare("SELECT price FROM prices WHERE token=?").get(sym)?.price;
        if (ab.sa > 0 && px > 0) {
          whaleAvgChg = Math.round(((px - ab.su / ab.sa) / (ab.su / ab.sa)) * 100);
          if (whaleAvgChg >= 150 && signal === "GASS BUY") {
            signal = "WATCH";
            reasons.push(`⚠️ whale avg udah +${whaleAvgChg}% — entry sekarang = jadi exit liq, skip`);
          } else if (whaleAvgChg >= 150) {
            reasons.push(`⚠️ whale avg +${whaleAvgChg}% — telat, jangan kejar`);
          } else if (whaleAvgChg <= 20) {
            reasons.push(`✅ whale avg ${whaleAvgChg >= 0 ? "+" : ""}${whaleAvgChg}% — sejajar sama paus`);
          }
        }
      } catch {}
      // Kilat 15 mnt: gerak tajam barusan → override dengan sinyal kilat
      const k = m15[sym] || { b: 0, s: 0 };
      const kTot = k.b + k.s, kNet = k.b - k.s;
      const kPress = kTot > 0 ? k.b / kTot : 0.5;
      let kilat = "";
      const tooLate = (whaleAvgChg ?? 0) >= 150; // jangan kejar yang udah terbang duluan
      if (!tooLate && kTot >= 3000 && kNet > 0 && kPress >= 0.7) kilat = "⚡KILAT BUY";
      else if (kTot >= 3000 && kNet < 0 && kPress <= 0.3) kilat = "⚡KILAT SELL";
      out.push({ ...t, symbol: sym, signal: kilat || signal, strength: kilat ? Math.min(5, strength + 1) : strength, reasons, netFlow: Math.round(net), net15m: Math.round(kNet), whaleAvgChg });
    }
    out.sort((a, b) => (b.signal === "WATCH") - (a.signal === "WATCH") || Math.abs(b.netFlow) - Math.abs(a.netFlow));
  } catch (e) { console.log("[sig-err]", e.message); }
  if (out.length) sigCache = { ts: Date.now(), data: out };
  return out.length ? out : sigCache.data;
}
// List meme coin gaya DexScreener + aktivitas whale dari DB lokal
async function memeList() {
  if (Date.now() - memeCache.ts < 60000 && memeCache.data.length) return memeCache.data;
  const out = [];
  try {
    // 1. Trending multi-chain: boosts + profiles latest (gratis, tanpa key) + token tracked
    // boosts top = campur solana/robinhood/eth/bsc/base — JANGAN filter chain biar yang rame ketangkep
    let found = []; // {mint, chain}
    const pushFound = (arr) => { for (const x of arr) { if (x.tokenAddress && !found.some((f) => f.mint === x.tokenAddress)) found.push({ mint: x.tokenAddress, chain: x.chainId }); } };
    try {
      const b = await fetch("https://api.dexscreener.com/token-boosts/top/v1");
      if (b.ok) pushFound(await b.json());
    } catch {}
    try {
      const p = await fetch("https://api.dexscreener.com/token-profiles/latest/v1");
      if (p.ok) pushFound(await p.json());
    } catch {}
    for (const s of Object.keys(TOKEN_MINTS)) found.push({ mint: TOKEN_MINTS[s], chain: "solana" });
    // Trending dulu (boosts = paling panas), fixed majors selalu ikut, total max 30
    const hot = found.filter((f) => !Object.values(TOKEN_MINTS).includes(f.mint));
    const fixed = found.filter((f) => Object.values(TOKEN_MINTS).includes(f.mint));
    const mints = [...new Set([...fixed.map((f) => f.mint), ...hot.map((f) => f.mint)])].slice(0, 30);
    // 2. Batch pair data (1 request untuk max 30 mint, semua chain)
    const r = await fetch(`https://api.dexscreener.com/latest/dex/tokens/${mints.join(",")}`);
    if (r.ok) {
      const j = await r.json();
      const byMint = {};
      const pairCount = {};
      for (const p of (j?.pairs || [])) {
        const m = p.baseToken.address;
        pairCount[m] = (pairCount[m] || 0) + 1;
        if (!byMint[m] || (p.liquidity?.usd || 0) > (byMint[m].liquidity?.usd || 0)) byMint[m] = p;
      }
      const since = new Date(Date.now() - 24 * 3600 * 1000).toISOString();
      for (const m of mints) {
        const p = byMint[m];
        if (!p) continue;
        const sym = p.baseToken.symbol;
        trackMint(m, sym, p.chainId); // daftarkan semua yang ketemu biar trade-nya ikut ke-poll
        const w = db.prepare("SELECT COUNT(*) c, COALESCE(SUM(usd_value),0) v FROM transfers WHERE token=? AND usd_value>=? AND ts>?").get(sym, WHALE_TX_USD, since);
        out.push({
          mint: m, symbol: sym, name: p.baseToken.name, chain: p.chainId,
          verified: !!MINT2SYM[m], // ✅ mint canonical yang kita pin (anti token palsu se-nama)
          iconUrl: p.info?.imageUrl || `https://dd.dexscreener.com/ds-data/tokens/${p.chainId}/${m}.png`,
          pairCount: pairCount[m] || 1,
          priceUsd: p.priceUsd, priceChange: p.priceChange,
          txns: p.txns, volumeH24: p.volume?.h24, liquidityUsd: p.liquidity?.usd,
          dex: p.dexId, url: p.url, pairCreatedAt: p.pairCreatedAt || null,
          whaleTx24h: w.c, whaleVol24h: Math.round(w.v),
          rugFlags: [],
        });
      }
      // Flag flow deskriptif (data Dex apa adanya, BUKAN vonis rug):
      // SELL-DOM = dominasi sell 1 jam terakhir, DUMP-20 = harga -20%/5mnt, LIQ-DRAIN = liq anjlok >30%
      for (const t of out) {
        const flags = [];
        const buys1 = t.txns?.h1?.buys || 0, sells1 = t.txns?.h1?.sells || 0;
        if (sells1 >= 20 && sells1 >= buys1 * 2) flags.push("SELL-DOM");
        if ((t.priceChange?.m5 ?? 0) <= -20) flags.push("DUMP-20");
        const prev = prevMeme[t.mint];
        if (prev && prev.liq > 0 && t.liquidityUsd != null) {
          if (t.liquidityUsd < prev.liq * 0.7) flags.push("LIQ-DRAIN");
        }
        t.rugFlags = flags;
        const newFlags = flags.filter((f) => !(prev?.flags || []).includes(f));
        for (const f of newFlags) {
          db.prepare("INSERT INTO alerts(wallet,rule,msg,ts) VALUES(?,?,?,?)").run(t.mint, "flow-flag", `📊 ${f} ${t.symbol} [${t.chain}] — h1 sell ${sells1} vs buy ${buys1}, liq $${Math.round(t.liquidityUsd || 0).toLocaleString()}`, now());
        }
        prevMeme[t.mint] = { liq: t.liquidityUsd || 0, flags };
      }
      // Tempel hasil audit RugCheck (kalau ada) ke tiap baris
      try {
        for (const t of out) {
          if (t.chain !== "solana") continue;
          const rz = db.prepare("SELECT * FROM rug WHERE mint=?").get(t.mint);
          if (rz) t.rug = { rugged: !!rz.rugged, score: rz.score, topPct: rz.topPct, mintAuth: rz.mintAuth, freezeAuth: rz.freezeAuth, risks: JSON.parse(rz.risks || "[]") };
        }
      } catch {}
      out.sort((a, b) => b.whaleTx24h - a.whaleTx24h || (b.volumeH24 || 0) - (a.volumeH24 || 0));
    }
  } catch (e) { console.log("[meme-err]", e.message); }
  if (out.length) memeCache = { ts: Date.now(), data: out };
  return out.length ? out : memeCache.data;
}

async function priceOf(token) {
  const row = db.prepare("SELECT * FROM prices WHERE token=?").get(token);
  const fresh = row && Date.now() - new Date(row.updated_at).getTime() < 300000;
  if (fresh) return row.price;
  // DexScreener gratis tanpa key — pakai mint address (bukan search, biar tepat)
  try {
    const mint = TOKEN_MINTS[token];
    if (mint) {
      const ctrl = new AbortController();
      const to = setTimeout(() => ctrl.abort(), 8000);
      const r = await fetch(`https://api.dexscreener.com/latest/dex/tokens/${mint}`, { signal: ctrl.signal });
      clearTimeout(to);
      if (r.ok) {
        const j = await r.json();
        const pair = (j?.pairs || []).filter((p) => p.chainId === "solana").sort((a, b) => (b.liquidity?.usd || 0) - (a.liquidity?.usd || 0))[0];
        const p = Number(pair?.priceUsd);
        if (p > 0) { db.prepare("INSERT OR REPLACE INTO prices VALUES(?,?,?)").run(token, p, now()); return p; }
      }
    }
  } catch {}
  return row ? row.price : 0;
}

// Poll trade real per pool via GeckoTerminal (gratis, tanpa key) → alert whale buy/sell
// Round-robin 6 token/siklus dari semua mint yang pernah ketemu (tracked) biar semua kebagian
// Prioritas: yang ada aktivitas whale dulu (naik ke atas antrian), sisanya fair
async function pollDexTrades() {
  for (const s of Object.keys(TOKEN_MINTS)) trackMint(TOKEN_MINTS[s], s, "solana");
  const queue = db.prepare("SELECT * FROM tracked ORDER BY last_polled ASC LIMIT 3").all();
  for (const q of queue) {
    const symbol = q.symbol, mint = q.mint, chain = q.chain || "solana";
    const gnet = GECKO_NET[chain];
    try {
      const poolKey = chain + ":" + symbol;
      let pool = poolCache[poolKey];
      if (!pool) {
        if (!gnet) { db.prepare("UPDATE tracked SET last_polled=? WHERE mint=?").run(now(), mint); continue; } // chain ga didukung Gecko → price-only
        const ctrl = new AbortController();
        const to = setTimeout(() => ctrl.abort(), 8000);
        const r = await fetch(`https://api.geckoterminal.com/api/v2/networks/${gnet}/tokens/${mint}/pools?page=1`, { signal: ctrl.signal });
        clearTimeout(to);
        if (!r.ok) continue;
        const j = await r.json();
        pool = j?.data?.[0]?.attributes?.address;
        if (!pool) continue;
        poolCache[poolKey] = pool;
      }
      await sleep(800);
      // 1 halaman saja di loop cepat (cepat); 2 halaman di loop lambat
      const ctrl2 = new AbortController();
      const to2 = setTimeout(() => ctrl2.abort(), 8000);
      const t = await fetch(`https://api.geckoterminal.com/api/v2/networks/${gnet}/pools/${pool}/trades?page=1`, { signal: ctrl2.signal });
      clearTimeout(to2);
      if (t.status === 429) { await sleep(15000); continue; }
      if (!t.ok) continue;
      const tj = await t.json();
      const list = tj?.data || [];
      for (const tr of list.slice(0, 15)) {
        const a = tr?.attributes || {};
        const sig = chain + ":" + (a.tx_hash || "") + ":" + (a.kind || "");
        if (!sig || sig === (chain + "::")) continue;
        if (db.prepare("SELECT 1 FROM transfers WHERE sig=?").get(sig)) continue;
        const usd = Number(a.volume_in_usd || 0);
        const maker = a.tx_from_address || "unknown";
        db.prepare("INSERT OR IGNORE INTO wallets(address,chain,label,updated_at) VALUES(?,?,?,?)").run(maker, chain, usd >= WHALE_TX_USD ? "whale" : "retail", now());
        db.prepare("INSERT OR IGNORE INTO transfers VALUES(?,?,?,?,?,?,?,?)").run(sig, maker, chain, symbol, Number(a.from_token_amount || 0), usd, a.kind === "sell" ? "OUT" : "IN", a.block_timestamp || now());
        if (usd >= WHALE_TX_USD) {
          db.prepare("INSERT INTO alerts(wallet,transfer_sig,rule,msg,ts) VALUES(?,?,?,?,?)").run(maker, sig, "dex-whale-trade", `DEX ${a.kind?.toUpperCase()} $${Math.round(usd).toLocaleString()} ${symbol} [${chain}] @${maker.slice(0, 8)}`, now());
        }
      }
      db.prepare("UPDATE tracked SET last_polled=? WHERE mint=?").run(now(), mint);
    } catch (e) { console.log("[dex-err]", symbol, e.message); }
    await sleep(800);
  }
}

const TOKENS = ["WIF", "BONK", "POPCAT"];

// Whale discovery via Solana RPC — Shyft RPC kalau ada key (limit lega), fallback publik
async function rpc(method, params) {
  const key = shyftKey();
  const url = key ? `https://rpc.shyft.to?api_key=${key}` : "https://api.mainnet-beta.solana.com";
  const ctrl = new AbortController();
  const to = setTimeout(() => ctrl.abort(), 10000);
  try {
    const r = await fetch(url, {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ jsonrpc: "2.0", id: 1, method, params }),
      signal: ctrl.signal,
    });
    clearTimeout(to);
    if (!r.ok) return null;
    const j = await r.json();
    return j?.result ?? null;
  } catch { clearTimeout(to); return null; }
}

async function pollHolders() {
  for (const symbol of Object.keys(TOKEN_MINTS)) {
    try {
      const price = await priceOf(symbol);
      const res = await rpc("getTokenLargestAccounts", [TOKEN_MINTS[symbol]]);
      const accs = res?.value || [];
      for (const a of accs.slice(0, 10)) {
        const amt = Number(a.uiAmountString || 0);
        const usd = amt * price;
        db.prepare("INSERT INTO wallets(address,chain,label,balance_usd,updated_at) VALUES(?,?,?,?,?) ON CONFLICT(address) DO UPDATE SET balance_usd=excluded.balance_usd, label=excluded.label, updated_at=excluded.updated_at")
          .run(a.address, "solana", usd >= 100000 ? "whale" : "retail", Math.round(usd * 100) / 100, now());
      }
    } catch (e) { console.log("[holder-err]", symbol, e.message); }
    await sleep(1500);
  }
}

async function pollWallet(w) {
  if (shyftKey()) {
    // Real mode: Shyft transaction history (best-effort, backoff sederhana)
    try {
      const r = await fetch(`https://api.shyft.to/sol/v1/transaction/history?network=mainnet-beta&account=${w.address}&tx_num=5`, { headers: { "x-api-key": shyftKey() } });
      if (r.status === 429) { await sleep(10000); return; }
      if (r.ok) {
        const j = await r.json();
        const txs = j?.result || [];
        // History mentah Shyft tanpa nilai parsed →_parsing nilai butuh Enhanced API.
        // Jangan masukin transfer $0 (ngotorin sinyal); catat kursor aja.
        if (txs[0]) db.prepare("UPDATE wallets SET last_sig=?, updated_at=? WHERE address=?").run(txs[0].signatures?.[0] || "", now(), w.address);
        return;
      }
    } catch (e) { console.log("[poll-err]", w.address.slice(0, 8), e.message); }
  }
  // Tanpa key: tidak ada simulasi — hanya aturan real dari data Dex/Gecko.
  // Inflow 1h rule (dari transfer real)
  const inflow = db.prepare("SELECT COALESCE(SUM(usd_value),0) s FROM transfers WHERE wallet=? AND direction='IN' AND ts > ?").get(w.address, new Date(Date.now() - 3600 * 1000).toISOString()).s;
  if (inflow >= WHALE_INFLOW_USD) {
    const dup = db.prepare("SELECT 1 FROM alerts WHERE wallet=? AND rule='whale-inflow-1h' AND ts > ?").get(w.address, new Date(Date.now() - 3600 * 1000).toISOString());
    if (!dup) db.prepare("INSERT INTO alerts(wallet,rule,msg,ts) VALUES(?,?,?,?)").run(w.address, "whale-inflow-1h", `INFLOW 1h $${Math.round(inflow).toLocaleString()} @${w.address.slice(0, 8)}`, now());
  }
  // Auto-label dari balance real
  db.prepare("UPDATE wallets SET label=?, updated_at=? WHERE address=?").run(w.tag_manual || (w.balance_usd > 100000 ? "whale" : "retail"), now(), w.address);
}

let polling = false; // cegah siklus numpuk (DB locked + rate-limit jebol)
const lastRun = { fast: 0, slow: 0, fastMs: 0, slowMs: 0 };
// Loop CEPAT (45 dtk): trade Gecko + CTO — ini yang bikin sinyal sensitif, murah & cepat
async function pollFast() {
  if (polling) return;
  polling = true;
  const t0 = Date.now();
  try { await pollDexTrades(); await pollCTO(); }
  catch (e) { console.log("[fast-err]", e.message); }
  finally { lastRun.fast = Date.now(); lastRun.fastMs = Date.now() - t0; polling = false; }
}
// Loop LAMBAT (8 mnt): holders + audit rug + history wallet — berat, jarang berubah
let slowing = false;
async function pollSlow() {
  if (slowing || polling) return;
  slowing = true;
  const t0 = Date.now();
  try {
    await pollHolders();
    await pollRug();
    const wallets = db.prepare("SELECT * FROM wallets").all().slice(0, 15);
    for (const w of wallets) {
      try { await pollWallet(w); } catch (e) { console.log("[poll-fail]", e.message); }
      await sleep(1200);
    }
  } catch (e) { console.log("[slow-err]", e.message); }
  finally { lastRun.slow = Date.now(); lastRun.slowMs = Date.now() - t0; slowing = false; }
}
setInterval(pollFast, 90000); setInterval(pollSlow, 8 * 60 * 1000); pollSlow(); pollFast(); // slow dulu biar ga di-skip guard

function serveStatic(req, res) {
  let p = req.url.split("?")[0];
  if (p === "/") p = "/index.html";
  const f = path.join(DIR, "public", decodeURIComponent(p).replace(/^\//, ""));
  if (!f.startsWith(path.join(DIR, "public"))) return send(res, 403, { error: "forbidden" });
  if (!fs.existsSync(f) || fs.statSync(f).isDirectory()) return null;
  const ext = path.extname(f);
  const ct = ext === ".html" ? "text/html" : ext === ".js" ? "text/javascript" : ext === ".css" ? "text/css" : "application/octet-stream";
  res.writeHead(200, { "Content-Type": ct }); fs.createReadStream(f).pipe(res);
  return true;
}

const server = http.createServer(async (req, res) => {
  const u = new URL(req.url, "http://x");
  if (req.method === "GET" && !u.pathname.startsWith("/api/")) {
    if (serveStatic(req, res)) return;
    const idx = path.join(DIR, "public", "index.html");
    res.writeHead(200, { "Content-Type": "text/html" }); fs.createReadStream(idx).pipe(res); return;
  }
  try {
    if (u.pathname === "/api/wallets" && req.method === "GET") return send(res, 200, db.prepare("SELECT * FROM wallets ORDER BY balance_usd DESC").all());
    if (u.pathname === "/api/wallets" && req.method === "POST") {
      const b = await body(req);
      if (!b.address) return send(res, 400, { error: "address required" });
      db.prepare("INSERT OR REPLACE INTO wallets VALUES(?,?,?,?,?,?,?)").run(b.address, b.chain || "solana", b.label || "retail", b.tag_manual || "", b.balance_usd || 0, "", now());
      return send(res, 201, { ok: true });
    }
    if (u.pathname === "/api/transfers" && req.method === "GET") {
      const w = u.searchParams.get("wallet");
      const q = w ? db.prepare("SELECT * FROM transfers WHERE wallet=? ORDER BY ts DESC LIMIT 100").all(w)
        : db.prepare("SELECT * FROM transfers ORDER BY ts DESC LIMIT 100").all();
      return send(res, 200, q);
    }
    if (u.pathname === "/api/alerts" && req.method === "GET") {
      const ack = u.searchParams.get("ack");
      const q = ack === null ? db.prepare("SELECT * FROM alerts ORDER BY id DESC LIMIT 100").all()
        : db.prepare("SELECT * FROM alerts WHERE ack=? ORDER BY id DESC LIMIT 100").all(Number(ack));
      return send(res, 200, q);
    }
    const mAck = u.pathname.match(/^\/api\/alerts\/(\d+)\/ack$/);
    if (mAck && req.method === "POST") { db.prepare("UPDATE alerts SET ack=1 WHERE id=?").run(mAck[1]); return send(res, 200, { ok: true }); }
    if (u.pathname === "/api/prices" && req.method === "GET") return send(res, 200, db.prepare("SELECT * FROM prices").all());
    if (u.pathname === "/api/meme-list" && req.method === "GET") return send(res, 200, await memeList());
    if (u.pathname === "/api/signals" && req.method === "GET") return send(res, 200, await signals());
    if (u.pathname === "/api/token" && req.method === "GET") {
      const mint = u.searchParams.get("mint");
      if (!mint) return send(res, 400, { error: "mint required" });
      const list = await memeList();
      const info = list.find((x) => x.mint === mint) || null;
      const sym = info?.symbol;
      const rug = db.prepare("SELECT * FROM rug WHERE mint=?").get(mint);
      const tx = sym ? db.prepare("SELECT * FROM transfers WHERE token=? ORDER BY usd_value DESC LIMIT 15").all(sym) : [];
      const sig = (await signals()).find((s) => s.symbol === sym) || null;
      return send(res, 200, {
        info, signal: sig, transfers: tx,
        rug: rug ? { ...rug, risks: JSON.parse(rug.risks || "[]") } : null,
      });
    }
    if (u.pathname === "/api/orderflow" && req.method === "GET") return send(res, 200, await orderflow(u.searchParams.get("symbol") || "WIF"));
    if (u.pathname === "/api/status" && req.method === "GET") return send(res, 200, { fastSecAgo: Math.round((Date.now() - lastRun.fast) / 1000), fastMs: lastRun.fastMs, slowSecAgo: Math.round((Date.now() - lastRun.slow) / 1000), slowMs: lastRun.slowMs, polling, slowing });
    const mRug = u.pathname.match(/^\/api\/rug\/(.+)$/);
    if (mRug && req.method === "GET") {
      const rz = db.prepare("SELECT * FROM rug WHERE mint=?").get(mRug[1]);
      if (!rz) return send(res, 404, { error: "belum diaudit — tunggu siklus pollRug" });
      return send(res, 200, { ...rz, risks: JSON.parse(rz.risks || "[]") });
    }
    if (u.pathname === "/api/macro" && req.method === "GET") return send(res, 200, await macro());
    if (u.pathname === "/api/metas" && req.method === "GET") {
      if (Date.now() - metaCache.ts > 300000 || !metaCache.data.length) {
        try {
          const r = await fetch("https://api.dexscreener.com/metas/trending/v1");
          if (r.ok) metaCache = { ts: Date.now(), data: await r.json() };
        } catch {}
      }
      return send(res, 200, metaCache.data);
    }
    if (u.pathname === "/api/config" && req.method === "GET") return send(res, 200, { whaleTxUsd: WHALE_TX_USD, whaleInflowUsd: WHALE_INFLOW_USD, mode: shyftKey() ? "real-shyft" : "free-real", hasShyftKey: !!shyftKey(), pollMs: POLL_MS, dexTokens: Object.keys(TOKEN_MINTS) });
    if (u.pathname === "/api/config" && req.method === "POST") {
      const b = await body(req);
      if (b.shyftKey !== undefined) {
        if (String(b.shyftKey).trim()) db.prepare("INSERT OR REPLACE INTO config VALUES(?,?)").run("SHYFT_KEY", String(b.shyftKey).trim());
        else db.prepare("DELETE FROM config WHERE key='SHYFT_KEY'").run();
      }
      return send(res, 200, { ok: true, hasShyftKey: !!shyftKey(), mode: shyftKey() ? "real-shyft" : "free-real" });
    }
    return send(res, 404, { error: "unknown route" });
  } catch (e) { return send(res, 500, { error: String(e.message || e) }); }
});

server.listen(PORT, () => console.log(`[whale-tracker] http://localhost:${PORT}`));
