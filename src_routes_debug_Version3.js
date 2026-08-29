const express = require('express');
const router = express.Router();
const db = require('../db').get();
const wsManager = require('../services/bybitWs');
const pino = require('pino');
const logger = pino();

router.get('/health', (req, res) => res.json({ ok: true, ts: Date.now() }));

router.get('/symbols', (req, res) => {
  const rows = db.prepare('SELECT * FROM symbols ORDER BY symbol COLLATE NOCASE ASC LIMIT 1000').all();
  res.json({ count: rows.length, rows });
});

router.get('/klines/:symbol/:tf', (req, res) => {
  const { symbol, tf } = req.params;
  const rows = db.prepare('SELECT * FROM klines WHERE symbol=? AND timeframe=? ORDER BY open_time DESC LIMIT 200').all(symbol, tf);
  res.json({ symbol, timeframe: tf, rows });
});

router.get('/trades', (req, res) => {
  const rows = db.prepare('SELECT * FROM trades ORDER BY opened_at DESC LIMIT 100').all();
  res.json(rows);
});

router.get('/ws/subscriptions', (req, res) => {
  const subs = {};
  for (let i = 0; i < wsManager.connections.length; i++) {
    const c = wsManager.connections[i];
    subs[i] = { id: c.id, symbols: Array.from(c.symbols || []), topics: Array.from(c._topics || []) };
  }
  res.json(subs);
});

module.exports = router;