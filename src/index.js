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

logger.info('Starting trading bot application...');

// Initialize database
try {
  dbModule.init();
  logger.info('✅ Database initialized');
} catch (err) {
  logger.error({ err }, '❌ Database initialization failed');
  process.exit(1);
}

// Initialize Telegram
try {
  telegram.init();
  logger.info('✅ Telegram initialized');
} catch (err) {
  logger.error({ err }, '❌ Telegram initialization failed');
}

// Initialize WS Manager
try {
  wsManager.start();
  logger.info('✅ WebSocket manager started');
} catch (err) {
  logger.error({ err }, '❌ WebSocket manager startup failed');
}

// Initialize Signal Manager
try {
  signalManager.start();
  logger.info('✅ Signal manager started');
} catch (err) {
  logger.error({ err }, '❌ Signal manager startup failed');
}

// Register WS callbacks for trade management
try {
  tradeManager.registerWs(wsManager);
  logger.info('✅ Trade manager registered to WS events');
} catch (err) {
  logger.warn({ err }, '⚠️ Failed to register trade manager with WS (continuing)');
}

// ============================================================
// TV RATING CACHE CLEANUP (scheduled daily at 2am UTC)
// ============================================================

const scheduleTvCacheCleanup = () => {
  try {
    const now = new Date();
    const target = new Date(now);
    target.setUTCHours(2, 0, 0, 0);
    
    if (target <= now) {
      target.setUTCDate(target.getUTCDate() + 1);
    }
    
    const waitMs = target - now;
    logger.info({ nextCleanup: target.toISOString(), waitMinutes: Math.round(waitMs / 60000) }, 'TV cache cleanup scheduled');
    
    setTimeout(() => {
      try {
        logger.info('Running TV cache cleanup (removing ratings older than 24 hours)');
        tradingview.clearOldTvRatingCache(24);
        logger.info('✅ TV cache cleanup completed');
      } catch (err) {
        logger.error({ err }, '❌ TV cache cleanup failed');
      }
      
      scheduleTvCacheCleanup();
    }, waitMs);
  } catch (err) {
    logger.error({ err }, '❌ Failed to schedule TV cache cleanup');
  }
};

scheduleTvCacheCleanup();

// ============================================================
// START POLLER (main scanning loop)
// ============================================================

try {
  logger.info('Starting poller (initial scan + continuous boundary monitoring)...');
  poller.start();
  logger.info('✅ Poller started successfully');
} catch (err) {
  logger.error({ err }, '❌ Failed to start poller');
  process.exit(1);
}

// ============================================================
// EXPRESS SERVER SETUP
// ============================================================

app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// ============================================================
// SIMPLE REQUEST LOGGING (lightweight)
// ============================================================

app.use((req, res, next) => {
  if (!req.path.startsWith('/health')) {
    logger.debug({ method: req.method, path: req.path }, 'HTTP request');
  }
  next();
});

// ============================================================
// HEALTH CHECK ENDPOINT (lightweight, no DB queries)
// ============================================================

app.get('/health', (req, res) => {
  try {
    const uptime = process.uptime();
    const memUsage = process.memoryUsage();
    
    res.json({
      status: 'ok',
      timestamp: new Date().toISOString(),
      uptime_seconds: Math.floor(uptime),
      memory_usage_mb: {
        heap_used: Math.round(memUsage.heapUsed / 1024 / 1024),
        heap_total: Math.round(memUsage.heapTotal / 1024 / 1024),
        rss: Math.round(memUsage.rss / 1024 / 1024)
      },
      config: {
        root_tfs: config.ROOT_TFS,
        mtf_tfs: config.MTF_TFS,
        max_open_trades: config.MAX_OPEN_TRADES,
        symbol_filter: config.SYMBOL_FILTER,
        opentrade_enabled: config.OPENTRADE,
        telegram_enabled: !!config.TELEGRAM_BOT_TOKEN
      }
    });
  } catch (err) {
    logger.error({ err }, 'Health check error');
    res.status(500).json({ error: err.message });
  }
});

// ============================================================
// STATUS ENDPOINT (OPTIMIZED - caches results for 5 seconds)
// ============================================================

let cachedStatus = null;
let lastStatusTime = 0;
const STATUS_CACHE_MS = 5000; // Cache for 5 seconds

app.get('/status', (req, res) => {
  try {
    const now = Date.now();
    
    // Return cached status if fresh
    if (cachedStatus && (now - lastStatusTime) < STATUS_CACHE_MS) {
      return res.json(cachedStatus);
    }

    const db = dbModule.get();
    if (!db) {
      return res.status(503).json({ error: 'Database not available' });
    }

    // Get open trades count (fast query)
    const openTradesRow = db.prepare('SELECT COUNT(*) as cnt FROM trades WHERE status = ?').get('open');
    const openCount = openTradesRow?.cnt || 0;

    // Get closed trades count (fast query)
    const closedTradesRow = db.prepare('SELECT COUNT(*) as cnt FROM trades WHERE status = ?').get('closed');
    const closedCount = closedTradesRow?.cnt || 0;

    // Get latest signals (limit to 20 instead of 50)
    const signals = db.prepare(`
      SELECT s1.symbol, s1.root_tf, s1.detected_at, s1.state, s1.meta
      FROM signals s1
      INNER JOIN (
        SELECT symbol, root_tf, MAX(detected_at) as max_dt
        FROM signals
        GROUP BY symbol, root_tf
      ) s2 ON s1.symbol = s2.symbol AND s1.root_tf = s2.root_tf AND s1.detected_at = s2.max_dt
      ORDER BY s1.detected_at DESC
      LIMIT 20
    `).all();

    // Get only open trades (no joins)
    const openTrades = db.prepare('SELECT id, symbol, opened_at, side, size, entry_price, tp, sl FROM trades WHERE status = ? ORDER BY opened_at DESC LIMIT 10').all('open');

    // Get symbol count (fast with index)
    const symbolCountRow = db.prepare('SELECT COUNT(*) as count FROM symbols').get();
    const symbolCount = symbolCountRow?.count || 0;

    const responseData = {
      timestamp: new Date().toISOString(),
      signals: {
        latest_count: signals.length,
        signals: signals.map(s => {
          let meta = {};
          try { meta = s.meta ? JSON.parse(s.meta) : {}; } catch (e) { meta = {}; }
          return {
            symbol: s.symbol,
            root_tf: s.root_tf,
            detected_at: new Date(s.detected_at).toISOString(),
            state: s.state,
            tv_score: meta.tvScore || 0,
            mtf_score: meta.mtfScore || 0,
            decision: meta.decision || 'unknown'
          };
        })
      },
      trades: {
        open_count: openCount,
        closed_count: closedCount,
        max_slots: config.MAX_OPEN_TRADES,
        open_trades: openTrades.map(t => ({
          id: t.id,
          symbol: t.symbol,
          opened_at: new Date(t.opened_at).toISOString(),
          side: t.side,
          size: t.size,
          entry_price: t.entry_price
        }))
      },
      symbols: {
        total_count: symbolCount
      }
    };

    // Cache the response
    cachedStatus = responseData;
    lastStatusTime = now;

    res.json(responseData);
  } catch (err) {
    logger.error({ err }, 'Status endpoint error');
    res.status(500).json({ error: err.message });
  }
});

// ============================================================
// SIGNALS ENDPOINT (single symbol)
// ============================================================

app.get('/signals/:symbol', (req, res) => {
  try {
    const { symbol } = req.params;
    if (!symbol || symbol.length === 0) {
      return res.status(400).json({ error: 'Symbol parameter required' });
    }

    const db = dbModule.get();
    const signals = db.prepare(`
      SELECT symbol, root_tf, detected_at, state, meta
      FROM signals
      WHERE symbol = ?
      ORDER BY detected_at DESC
      LIMIT 100
    `).all(symbol);

    res.json({
      symbol,
      count: signals.length,
      signals: signals.map(s => {
        let meta = {};
        try { meta = s.meta ? JSON.parse(s.meta) : {}; } catch (e) { meta = {}; }
        return {
          root_tf: s.root_tf,
          detected_at: new Date(s.detected_at).toISOString(),
          state: s.state,
          tv_score: meta.tvScore || 0,
          mtf_score: meta.mtfScore || 0,
          decision: meta.decision || 'unknown'
        };
      })
    });
  } catch (err) {
    logger.error({ err }, 'Signals endpoint error');
    res.status(500).json({ error: err.message });
  }
});

// ============================================================
// MARKET DATA ENDPOINT
// ============================================================

app.get('/market/:symbol', (req, res) => {
  try {
    const { symbol } = req.params;
    if (!symbol || symbol.length === 0) {
      return res.status(400).json({ error: 'Symbol parameter required' });
    }

    const db = dbModule.get();
    const marketData = db.prepare(`
      SELECT symbol, price, volume_24h_usdt, volume_change_pct, market_cap, updated_at
      FROM market_data
      WHERE symbol = ?
    `).get(symbol);

    if (!marketData) {
      return res.json({
        symbol,
        data: {
          price: 0,
          volume_24h_usdt: 0,
          volume_change_pct: null,
          market_cap: null,
          updated_at: null
        }
      });
    }

    res.json({
      symbol,
      data: {
        price: marketData.price,
        volume_24h_usdt: marketData.volume_24h_usdt,
        volume_change_pct: marketData.volume_change_pct,
        market_cap: marketData.market_cap,
        updated_at: new Date(marketData.updated_at).toISOString()
      }
    });
  } catch (err) {
    logger.error({ err }, 'Market data endpoint error');
    res.status(500).json({ error: err.message });
  }
});

// ============================================================
// TV RATING ENDPOINT
// ============================================================

app.get('/tv-rating/:symbol', (req, res) => {
  try {
    const { symbol } = req.params;
    if (!symbol || symbol.length === 0) {
      return res.status(400).json({ error: 'Symbol parameter required' });
    }

    const db = dbModule.get();
    const cached = db.prepare(`
      SELECT symbol, score, source, exchange, updated_at
      FROM tv_ratings
      WHERE symbol = ?
    `).get(symbol);

    if (!cached) {
      return res.json({
        symbol,
        rating: {
          score: 0,
          source: 'not_cached',
          updated_at: null
        }
      });
    }

    res.json({
      symbol,
      rating: {
        score: cached.score,
        source: cached.source,
        exchange: cached.exchange,
        updated_at: new Date(cached.updated_at).toISOString(),
        age_minutes: Math.round((Date.now() - cached.updated_at) / 60000)
      }
    });
  } catch (err) {
    logger.error({ err }, 'TV rating endpoint error');
    res.status(500).json({ error: err.message });
  }
});

// ============================================================
// MANUAL SCAN ENDPOINT (non-blocking)
// ============================================================

app.post('/scan', async (req, res) => {
  try {
    logger.info('Manual scan triggered via API');
    
    // Don't wait for scan, return immediately
    res.json({
      status: 'scan_triggered',
      message: 'Scan initiated (results will be sent via Telegram)',
      timestamp: new Date().toISOString()
    });

    // Run scan in background (non-blocking)
    setImmediate(async () => {
      try {
        await poller.scanOnce({ notifyNewSignals: true });
        logger.info('Manual scan completed successfully');
      } catch (err) {
        logger.error({ err }, 'Manual scan error');
      }
    });
  } catch (err) {
    logger.error({ err }, 'Manual scan endpoint error');
    res.status(500).json({ error: err.message });
  }
});

// ============================================================
// ROOT PATH (API documentation)
// ============================================================

app.get('/', (req, res) => {
  res.json({
    name: 'Trading Bot API',
    version: '1.0.0',
    uptime: Math.floor(process.uptime()),
    endpoints: {
      health: {
        method: 'GET',
        path: '/health',
        description: 'Health check and system status (lightweight)'
      },
      status: {
        method: 'GET',
        path: '/status',
        description: 'Latest signals and open trades (cached 5 sec)',
        cache_duration: '5 seconds'
      },
      signals: {
        method: 'GET',
        path: '/signals/:symbol',
        description: 'Get all signals for a specific symbol',
        example: '/signals/BTCUSDT'
      },
      market_data: {
        method: 'GET',
        path: '/market/:symbol',
        description: 'Get cached market data for symbol',
        example: '/market/BTCUSDT'
      },
      tv_rating: {
        method: 'GET',
        path: '/tv-rating/:symbol',
        description: 'Get cached TradingView rating for symbol',
        example: '/tv-rating/BTCUSDT'
      },
      scan: {
        method: 'POST',
        path: '/scan',
        description: 'Trigger immediate scan (non-blocking, returns immediately)'
      }
    }
  });
});

// ============================================================
// 404 HANDLER
// ============================================================

app.use((req, res) => {
  res.status(404).json({
    error: 'Not found',
    path: req.path,
    method: req.method,
    documentation: 'GET /'
  });
});

// ============================================================
// ERROR HANDLER (lightweight)
// ============================================================

app.use((err, req, res, next) => {
  logger.error({ err, path: req.path, method: req.method }, 'Unhandled error in Express');
  res.status(500).json({
    error: 'Internal server error',
    message: err.message
  });
});

// ============================================================
// GRACEFUL SHUTDOWN
// ============================================================

const gracefulShutdown = async (signal) => {
  logger.info({ signal }, `${signal} received, graceful shutdown initiated`);
  try {
    logger.info('Closing WebSocket connections...');
    await wsManager.closeAll();
    logger.info('✅ WebSocket connections closed');

    logger.info('Closing database...');
    dbModule.close();
    logger.info('✅ Database closed');

    logger.info('✅ Graceful shutdown completed');
    process.exit(0);
  } catch (err) {
    logger.error({ err }, '❌ Error during graceful shutdown');
    process.exit(1);
  }
};

process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

process.on('uncaughtException', (err) => {
  logger.error({ err }, '❌ Uncaught exception');
  gracefulShutdown('uncaughtException');
});

process.on('unhandledRejection', (reason, promise) => {
  logger.error({ reason }, '❌ Unhandled rejection');
});

// ============================================================
// START SERVER
// ============================================================

const PORT = config.PORT || 3000;
const server = app.listen(PORT, '0.0.0.0', () => {
  logger.info({ port: PORT }, '✅ Express server started and listening');
  logger.info('Trading bot is ready. Check /health endpoint for status');
});

server.on('error', (err) => {
  logger.error({ err }, '❌ Server error');
  if (err.code === 'EADDRINUSE') {
    logger.error({ port: PORT }, `Port ${PORT} is already in use`);
  }
  process.exit(1);
});

module.exports = app;
