// src/routes/debug.js
/**
 * Debug routes (safe, lazy DB access).
 *
 * Important: this file intentionally does NOT call dbModule.get() at module load time,
 * because the DB is initialized later in src/index.js. Each handler calls getDb()
 * to obtain the DB instance and returns a 503 if it's not ready yet.
 */

const express = require('express');
const router = express.Router();
const pino = require('pino');
const logger = pino();

const dbModule = require('../db'); // do NOT call dbModule.get() here
const wsManager = require('../services/bybitWs');
const poller = require('../services/poller');
const config = require('../config');

let manualIntervalId = null;

function getDbOrThrow() {
  const db = dbModule.get();
  if (!db) {
    const err = new Error('Database not initialized');
    err.code = 'DB_NOT_READY';
    throw err;
  }
  return db;
}

function msToNext5m() {
  const d = new Date();
  const m = d.getUTCMinutes();
  const deltaM = 5 - (m % 5);
  const next = new Date(d);
  next.setUTCMinutes(m + deltaM);
  next.setUTCSeconds(0);
  next.setUTCMilliseconds(0);
  return Math.max(0, next - d);
}

// Health
router.get('/health', (req, res) => {
  res.json({ ok: true, ts: Date.now() });
});

// Status summary
router.get('/status', (req, res) => {
  try {
    const db = getDbOrThrow();
    const symbolCount = db.prepare('SELECT COUNT(*) as c FROM symbols').get().c || 0;
    const tradeCount = db.prepare('SELECT COUNT(*) as c FROM trades').get().c || 0;
    res.json({ ok: true, ts: Date.now(), symbolCount, tradeCount, env: { NODE_ENV: process.env.NODE_ENV || null, PORT: process.env.PORT || null } });
  } catch (err) {
    if (err.code === 'DB_NOT_READY') return res.status(503).json({ ok: false, error: err.message });
    logger.error({ err }, 'status handler error');
    res.status(500).json({ ok: false, error: String(err) });
  }
});

// List symbols
router.get('/symbols', (req, res) => {
  try {
    const db = getDbOrThrow();
    const rows = db.prepare('SELECT symbol, base, quote, fetched_at, price, market_cap, volume_24h, prev_volume_24h FROM symbols ORDER BY symbol COLLATE NOCASE ASC LIMIT 2000').all();
    res.json({ ok: true, count: rows.length, rows });
  } catch (err) {
    if (err.code === 'DB_NOT_READY') return res.status(503).json({ ok: false, error: err.message });
    logger.error({ err }, 'symbols handler error');
    res.status(500).json({ ok: false, error: String(err) });
  }
});

// Single symbol details
router.get('/symbols/:symbol', (req, res) => {
  try {
    const db = getDbOrThrow();
    const { symbol } = req.params;
    const row = db.prepare('SELECT * FROM symbols WHERE symbol = ?').get(symbol);
    if (!row) return res.status(404).json({ ok: false, error: 'not found' });
    res.json({ ok: true, row });
  } catch (err) {
    if (err.code === 'DB_NOT_READY') return res.status(503).json({ ok: false, error: err.message });
    logger.error({ err }, 'symbol detail handler error');
    res.status(500).json({ ok: false, error: String(err) });
  }
});

// Klines for a symbol/timeframe
router.get('/klines/:symbol/:tf', (req, res) => {
  try {
    const db = getDbOrThrow();
    const { symbol, tf } = req.params;
    const rows = db.prepare('SELECT open_time, open, high, low, close, volume FROM klines WHERE symbol = ? AND timeframe = ? ORDER BY open_time DESC LIMIT 500').all(symbol, tf);
    res.json({ ok: true, symbol, timeframe: tf, count: rows.length, rows });
  } catch (err) {
    if (err.code === 'DB_NOT_READY') return res.status(503).json({ ok: false, error: err.message });
    logger.error({ err }, 'klines handler error');
    res.status(500).json({ ok: false, error: String(err) });
  }
});

// Trades list
router.get('/trades', (req, res) => {
  try {
    const db = getDbOrThrow();
    const rows = db.prepare('SELECT * FROM trades ORDER BY opened_at DESC LIMIT 200').all();
    res.json({ ok: true, count: rows.length, rows });
  } catch (err) {
    if (err.code === 'DB_NOT_READY') return res.status(503).json({ ok: false, error: err.message });
    logger.error({ err }, 'trades handler error');
    res.status(500).json({ ok: false, error: String(err) });
  }
});

// WS subscriptions
router.get('/ws/subscriptions', (req, res) => {
  try {
    const connections = {};
    for (let i = 0; i < wsManager.connections.length; i++) {
      const c = wsManager.connections[i];
      connections[i] = { id: c.id, symbols: Array.from(c.symbols || []), topics: Array.from(c._topics || []) };
    }
    res.json({ ok: true, connections });
  } catch (err) {
    logger.error({ err }, 'ws subscriptions handler error');
    res.status(500).json({ ok: false, error: String(err) });
  }
});

// Time until next aligned 5m scan
router.get('/scan/next', (req, res) => {
  try {
    const ms = msToNext5m();
    res.json({ ok: true, msToNext5m: ms, nextAt: new Date(Date.now() + ms).toISOString() });
  } catch (err) {
    logger.error({ err }, 'scan/next handler error');
    res.status(500).json({ ok: false, error: String(err) });
  }
});

// Trigger a manual full scan once (paginated)
router.post('/scan', async (req, res) => {
  try {
    // Ensure DB ready before heavy operations
    getDbOrThrow();
    await poller.scanOnce();
    res.json({ ok: true, message: 'scanOnce executed; check logs for details' });
  } catch (err) {
    if (err.code === 'DB_NOT_READY') return res.status(503).json({ ok: false, error: err.message });
    logger.error({ err }, 'manual scan error');
    res.status(500).json({ ok: false, error: String(err) });
  }
});

// Trigger a scan for a single symbol's root TFs
router.post('/scan/symbol', async (req, res) => {
  try {
    getDbOrThrow();
    const body = req.body || {};
    const symbol = body.symbol;
    if (!symbol) return res.status(400).json({ ok: false, error: 'symbol required in JSON body' });
    // poller.scanSymbolRoots should exist in poller module
    if (typeof poller.scanSymbolRoots !== 'function') {
      return res.status(500).json({ ok: false, error: 'poller.scanSymbolRoots not available' });
    }
    await poller.scanSymbolRoots(symbol);
    res.json({ ok: true, message: `scanSymbolRoots executed for ${symbol}` });
  } catch (err) {
    if (err.code === 'DB_NOT_READY') return res.status(503).json({ ok: false, error: err.message });
    logger.error({ err }, 'scan/symbol error');
    res.status(500).json({ ok: false, error: String(err) });
  }
});

// Start/stop manual interval scanner for testing
router.post('/scan/startInterval', (req, res) => {
  try {
    if (manualIntervalId) return res.status(400).json({ ok: false, error: 'manual interval already running' });
    const body = req.body || {};
    const secs = Math.max(5, Number(body.intervalSeconds) || 30);
    manualIntervalId = setInterval(() => {
      logger.info('Manual interval triggered scanOnce');
      poller.scanOnce().catch(err => logger.error({ err }, 'manual interval scanOnce error'));
    }, secs * 1000);
    res.json({ ok: true, message: 'manual interval started', intervalSeconds: secs });
  } catch (err) {
    logger.error({ err }, 'startInterval error');
    res.status(500).json({ ok: false, error: String(err) });
  }
});

router.post('/scan/stopInterval', (req, res) => {
  try {
    if (!manualIntervalId) return res.status(400).json({ ok: false, error: 'no manual interval running' });
    clearInterval(manualIntervalId);
    manualIntervalId = null;
    res.json({ ok: true, message: 'manual interval stopped' });
  } catch (err) {
    logger.error({ err }, 'stopInterval error');
    res.status(500).json({ ok: false, error: String(err) });
  }
});

// Seed specific symbols
router.post('/seed', async (req, res) => {
  try {
    getDbOrThrow();
    const body = req.body || {};
    const symbols = Array.isArray(body.symbols) ? body.symbols : null;
    if (!symbols || !symbols.length) return res.status(400).json({ ok: false, error: 'symbols array required' });
    for (const s of symbols) {
      for (const tf of (config.ROOT_TFS || [])) {
        if (typeof poller.seedKlinesForSymbol === 'function') {
          await poller.seedKlinesForSymbol(s, tf);
        }
      }
    }
    res.json({ ok: true, message: `Seeded ${symbols.length} symbols` });
  } catch (err) {
    if (err.code === 'DB_NOT_READY') return res.status(503).json({ ok: false, error: err.message });
    logger.error({ err }, 'seed handler error');
    res.status(500).json({ ok: false, error: String(err) });
  }
});

module.exports = router;
