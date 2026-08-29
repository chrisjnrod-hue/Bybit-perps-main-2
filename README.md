# Bybit MACD MTF Scanner

This repository contains a Node.js app that:
- Scans Bybit USDT perpetual instruments (A→Z).
- Seeds and caches klines for root TFs (default 60m, 240m, D).
- Detects MACD flips on root TFs.
- Subscribes to MTF kline feeds (5m,15m,60m,240m,D) via batched WebSocket connections.
- Applies MTF alignment rules and filtering (market cap, 24h USDT volume, 24h volume change %).
- Optionally opens trades on Bybit v5 (OPENTRADE) and manages event-driven breakeven modes (off / fixed / trailing_lower_highs).
- Persists state in SQLite, sends Telegram alerts including raw exchange values.

Important: test thoroughly on Bybit testnet. Do NOT enable OPENTRADE on mainnet until you have validated qty/conversion and behavior.

Build & Start for Render:
- Build command: `npm install`
- Start command: `npm start`

See src/ for all code. Fill .env (based on .env.example) before running.