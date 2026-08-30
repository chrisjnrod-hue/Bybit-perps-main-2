// src/routes/debug.js
const express = require('express');
const router = express.Router();
const pino = require('pino');
const logger = pino();
const dbModule = require('../db');
const db = dbModule.get();
const wsManager = require('../services/bybitWs');
const poller = require('../services/poller');

// Manual interval id for temporary scanning via API
let manualIntervalId = null;

function msToNext5m() {
  const d = new Date();
  const m = d.getUTCMinutes();
  const s = d.getUTCSeconds();
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

// Service status summary
router.get('/status', (req, res) => {
  try {
    const symbolCount = db.prepare('SELECT COUNT(*) as c FROM symbols').get().c || 0;
    const tradeCount = db.prepare('SELECT COUNT(*) as c FROM trades').get().c || 0;
    res.json({
      ok: true,
      ts: Date.now(),
      symbolCount,
      tradeCount,
      heartbeat: true
    });
  } catch (err) {
    logger.error({ err }, 'status error');
    res.status(500).json({ ok: false, error: String(err) });
  }
});

// List symbols
router.get('/symbols', (req, res) => {
  try {
    const rows = db.prepare('SELECT symbol, base, quote, fetched_at, price, market_cap, volume_24h, prev_volume_24h FROM symbols ORDER BY symbol COLLATE NOCASE ASC LIMIT 2000').all();
    res.json({ ok: true, count: rows.length, rows });
  } catch (err) {
    logger.error({ err }, 'symbols error');
    res.status(500).json({ ok: false, error: String(err) });
  }
});

// Get single symbol details
router.get('/symbols/:symbol', (req, res) => {
  try {
    const { symbol } = req.params;
    const row = db.prepare('SELECT * FROM symbols WHERE symbol = ?').get(symbol);
    if (!row) return res.status(404).json({ ok: false, error: 'not found' });
    res.json({ ok: true, row });
  } catch (err) {
    logger.error({ err }, 'symbol detail error');
    res.status(500).json({ ok: false, error: String(err) });
  }
});

// Klines for a symbol/timeframe
router.get('/klines/:symbol/:tf', (req, res) => {
  try {
    const { symbol, tf } = req.params;
    const rows = db.prepare('SELECT open_time, open, high, low, close, volume FROM klines WHERE symbol = ? AND timeframe = ? ORDER BY open_time DESC LIMIT 500').all(symbol, tf);
    res.json({ ok: true, symbol, timeframe: tf, count: rows.length, rows });
  } catch (err) {
    logger.error({ err }, 'klines error');
    res.status(500).json({ ok: false, error: String(err) });
  }
});

// Trades list
router.get('/trades', (req, res) => {
  try {
    const rows = db.prepare('SELECT * FROM trades ORDER BY opened_at DESC LIMIT 200').all();
    res.json({ ok: true, count: rows.length, rows });
  } catch (err) {
    logger.error({ err }, 'trades error');
    res.status(500).json({ ok: false, error: String(err) });
  }
});

// WS subscriptions
router.get('/ws/subscriptions', (req, res) => {
  try {
    const subs = {};
    for (let i = 0; i < wsManager.connections.length; i++) {
      const c = wsManager.connections[i];
      subs[i] = { id: c.id, symbols: Array.from(c.symbols || []), topics: Array.from(c._topics || []) };
    }
    res.json({ ok: true, connections: subs });
  } catch (err) {
    logger.error({ err }, 'ws subscriptions error');
    res.status(500).json({ ok: false, error: String(err) });
  }
});

// Returns ms until next aligned 5m scan
router.get('/scan/next', (req, res) => {
  try {
    const ms = msToNext5m();
    res.json({ ok: true, msToNext5m: ms, nextAt: new Date(Date.now() + ms).toISOString() });
  } catch (err) {
    logger.error({ err }, 'scan/next error');
    res.status(500).json({ ok: false, error: String(err) });
  }
});

// Trigger a manual full scan once (paginated A-Z)
router.post('/scan', async (req, res) => {
  try {
    await poller.scanOnce();
    // Return a quick summary: total symbols and top few results
    const symbolCount = db.prepare('SELECT COUNT(*) as c FROM symbols').get().c || 0;
    res.json({ ok: true, message: 'scanOnce executed', symbolCount });
  } catch (err) {
    logger.error({ err }, 'manual scan error');
    res.status(500).json({ ok: false, error: String(err) });
  }
});

// Trigger a scan for a single symbol's root TFs
router.post('/scan/symbol', async (req, res) => {
  try {
    const { symbol } = req.body || {};
    if (!symbol) return res.status(400).json({ ok: false, error: 'symbol required in JSON body' });
    await poller.scanSymbolRoots(symbol);
    res.json({ ok: true, message: `scanSymbolRoots executed for ${symbol}` });
  } catch (err) {
    logger.error({ err }, 'scan symbol error');
    res.status(500).json({ ok: false, error: String(err) });
  }
});

// Start a temporary interval scanner that runs scanOnce every N seconds (for testing)
// body: { intervalSeconds: 30 } - returns id and status
router.post('/scan/startInterval', (req, res) => {
  try {
    const { intervalSeconds } = req.body || {};
    const secs = Math.max(5, Number(intervalSeconds) || 30);
    if (manualIntervalId) return res.status(400).json({ ok: false, error: 'manual interval already running' });
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

// Stop the temporary interval scanner
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

// Force seed (re-run initial seeding steps for a subset) - careful
// body: { symbols: ["BTCUSDT","ETHUSDT"] } - optional, if omitted seeds all current symbols
router.post('/seed', async (req, res) => {
  try {
    const body = req.body || {};
    const symbols = Array.isArray(body.symbols) ? body.symbols : null;
    if (symbols && symbols.length) {
      for (const s of symbols) {
        // seed root TF klines for given symbol
        for (const tf of (require('../config').ROOT_TFS || [])) {
          await poller.seedKlinesForSymbol(s, tf); // note: this method exists in poller
        }
      }
      return res.json({ ok: true, message: `Seeded ${symbols.length} symbols` });
    } else {
      // run initial full seed via poller.initialScan if you want, but it's heavy
      // don't call poller.initialScan() automatically to avoid heavy work in production
      return res.status(400).json({ ok: false, error: 'symbols array required to seed specific symbols' });
    }
  } catch (err) {
    logger.error({ err }, 'seed error');
    res.status(500).json({ ok: false, error: String(err) });
  }
});

module.exports = router;
