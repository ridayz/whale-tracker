# Whale Tracker — Meme Coin (Solana)

Port **8933**. Node stdlib + `node:sqlite`, tanpa npm deps.

## Cara jalan
```
cd whale-tracker
node server.mjs
# buka http://localhost:8933
```

## Mode
- **Demo (default)**: tanpa API key, transfer disimulasi tiap 45 detik, harga dari seed + DexScreener best-effort.
- **Real**: set `SHYFT_KEY` env untuk baca history transaksi asli via Shyft (`/sol/v1/transaction/history`). Harga live via DexScreener gratis (tanpa key).

## Aturan whale
- `WHALE_TX_USD` default 10000 — 1 transfer >= threshold → alert `whale-tx`.
- `WHALE_INFLOW_USD` default 50000 — inflow 1 jam >= threshold → alert `whale-inflow-1h`.
- Label otomatis: balance > $100k → `whale`, bisa override via `tag_manual`.

## API
- `GET /api/config`, `GET /api/wallets`, `POST /api/wallets {address,chain}`
- `GET /api/transfers?wallet=`, `GET /api/alerts?ack=0`, `POST /api/alerts/:id/ack`
- `GET /api/prices`
