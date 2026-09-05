const express = require('express');
const path = require('path');
const config = require('./config');
const logger = require('pino')();
const dbModule = require('./db');
const telegram = require('./services/telegram');
const poller = require('./services/poller');
const wsManager = require('./services/bybitWs');
const tradeManager = require('./services/tradeManager');
const signalManager = require('./services/signalManager');
const tradingview = require('./services/tradingview');

const app = express();

// ============================================================
// INITIALIZATION
// ============================================================

// Initialize database
dbModule.init();
logger.info('Database initialized');

// Initialize Telegram
telegram.init();
logger.info('Telegram initialized');

// Initialize WS Manager
wsManager.start();
logger.info('WebSocket manager started');

// Initialize Signal Manager
signalManager.start();
logger.info('Signal manager started');

// Register WS callbacks for trade management
try {
  tradeManager.registerWs(wsManager);
  logger.info('Trade manager registered to WS events');
} catch (err) {
  logger.warn({ err }, 'Failed to register trade manager with WS');
}

// ============================================================
// TV RATING CACHE CLEANUP (scheduled daily at 2am UTC)
// ============================================================

const scheduleTvCacheCleanup = () => {
  const now = new Date();
  const target = new Date(now);
  target.setUTCHours(2, 0, 0, 0);
  
  // If 2am already passed today, schedule for tomorrow
  if (target <= now) {
    target.setUTCDate(target.getUTCDate() + 1);
  }
  
  const waitMs = target - now;
  logger.info({ nextCleanup: target.toISOString() }, 'TV cache cleanup scheduled');
  
  setTimeout(() => {
    try {
      logger.info('Running TV cache cleanup (removing ratings older than 24 hours)');
      tradingview.clearOldTvRatingCache(24);
      logger.info('TV cache cleanup completed');
    } catch (err) {
      logger.error({ err }, 'TV cache cleanup failed');
    }
    
    // Reschedule for next day
    scheduleTvCacheCleanup();
  }, waitMs);
};

scheduleTvCacheCleanup();

// ============================================================
// START POLLER (main scanning loop)
// ============================================================

try {
  logger.info('Starting poller (initial scan + continuous boundary monitoring)...');
  poller.start();
  logger.info('Poller started successfully');
} catch (err) {
  logger.error({ err }, 'Failed to start poller');
  process.exit(1);
}

// ============================================================
// EXPRESS SERVER (health check + management endpoints)
// ============================================================

app.use(express.json());

// Health check
app.get('/health', (req, res) => {
  res.json({
    status: 'ok',
    timestamp: new Date().toISOString(),
    config: {
      root_tfs: config.ROOT_TFS,
      mtf_tfs: config.MTF_TFS,
      max_open_trades: config.MAX_OPEN_TRADES,
      symbol_filter: config.SYMBOL_FILTER
    }
  });
});

// Status endpoint (shows latest signals snapshot)
app.get('/status', (req, res) => {
  try {
    const db = dbModule.get();
    const signals = db.prepare(`
      SELECT s1.symbol, s1.root_tf, s1.detected_at, s1.state, s1.meta
      FROM signals s1
      INNER JOIN (
        SELECT symbol, root_tf, MAX(detected_at) as max_dt
        FROM signals
        GROUP BY symbol, root_tf
      ) s2 ON s1.symbol = s2.symbol AND s1.root_tf = s2.root_tf AND s1.detected_at = s2.max_dt
      ORDER BY s1.detected_at DESC
      LIMIT 50
    `).all();

    const trades = db.prepare('SELECT * FROM trades WHERE status = ? ORDER BY opened_at DESC LIMIT 20').all('open');

    res.json({
      timestamp: new Date().toISOString(),
      latest_signals: signals.map(s => ({
        symbol: s.symbol,
        root_tf: s.root_tf,
        detected_at: new Date(s.detected_at).toISOString(),
        state: s.state,
        meta: (() => { try { return JSON.parse(s.meta); } catch (e) { return {}; } })()
      })),
      open_trades: trades,
      open_trades_count: trades.length,
      max_trades: config.MAX_OPEN_TRADES
    });
  } catch (err) {
    logger.error({ err }, 'Status endpoint error');
    res.status(500).json({ error: err.message });
  }
});

// Graceful shutdown
process.on('SIGTERM', async () => {
  logger.info('SIGTERM received, graceful shutdown initiated');
  try {
    await wsManager.closeAll();
    dbModule.close();
    logger.info('Graceful shutdown completed');
    process.exit(0);
  } catch (err) {
    logger.error({ err }, 'Error during graceful shutdown');
    process.exit(1);
  }
});

process.on('SIGINT', async () => {
  logger.info('SIGINT received, graceful shutdown initiated');
  try {
    await wsManager.closeAll();
    dbModule.close();
    logger.info('Graceful shutdown completed');
    process.exit(0);
  } catch (err) {
    logger.error({ err }, 'Error during graceful shutdown');
    process.exit(1);
  }
});

// Start server
const PORT = config.PORT || 3000;
app.listen(PORT, () => {
  logger.info({ port: PORT }, 'Express server started');
});

module.exports = app;
