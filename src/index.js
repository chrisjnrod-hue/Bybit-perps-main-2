/**
 * Entrypoint - starts Express server and the poller/scanner
 *
 * Startup flow:
 *  - db.init()
 *  - telegram.init()
 *  - TV cache cleanup scheduler
 *  - poller.initialScan()
 *  - targeted seeding (optional)
 *  - poller.scanAllForStartup()  // ensures every symbol is evaluated for flips (silent)
 *  - signalManager.sendStartupSummary()
 *  - poller.start(), wsManager.start(), signalManager.start()
 *  - Express server with optimized endpoints
 */

require('dotenv').config();
const express = require('express');
const pino = require('pino');
const config = require('./config');
const db = require('./db');
const poller = require('./services/poller');
const wsManager = require('./services/bybitWs');
const signalManager = require('./services/signalManager');
const telegram = require('./services/telegram');
const tradeManager = require('./services/tradeManager');
const tradingview = require('./services/tradingview');
const debugRoutes = require('./routes/debug');

const logger = pino({ level: config.LOG_LEVEL || 'info' });

process.on('uncaughtException', (err) => {
  logger.error({ err }, 'UNCAUGHT EXCEPTION - the process may terminate');
});
process.on('unhandledRejection', (reason) => {
  logger.error({ reason }, 'UNHANDLED REJECTION - promise rejected without handler');
});

const app = express();
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use('/debug', debugRoutes);

const PORT = process.env.PORT || config.PORT || 3000;
let server;
let heartbeatInterval;

// ============================================================
// STATUS CACHING (optimize /status endpoint)
// ============================================================

let cachedStatus = null;
let lastStatusTime = 0;
const STATUS_CACHE_MS = 5000; // Cache for 5 seconds

function invalidateStatusCache() {
  cachedStatus = null;
  lastStatusTime = 0;
}

// ============================================================
// TV CACHE CLEANUP SCHEDULER (daily at 2am UTC)
// ============================================================

function scheduleTvCacheCleanup() {
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
}

// ============================================================
// STARTUP SEQUENCE
// ============================================================

async function start() {
  try {
    logger.info('🚀 Starting trading bot application...');
    
    // 1) Initialize database
    db.init();
    logger.info('✅ Database initialized');

    // 2) Initialize telegram early so notifications are possible
    telegram.init();
    logger.info('✅ Telegram initialized');

    // 3) Schedule TV cache cleanup
    scheduleTvCacheCleanup();
    logger.info('✅ TV cache cleanup scheduled');

    // 4) Background bybit probe (non-blocking)
    try {
      const bybitRest = require('./services/bybitRest');
      bybitRest.probeHosts(3000)
        .then((base) => {
          if (base) logger.info({ base }, '✅ Bybit host probe completed');
          else logger.warn('⚠️ Bybit host probe found no suitable base');
        })
        .catch((e) => logger.debug({ e }, 'Bybit host probe failed (non-fatal)'));
    } catch (e) {
      logger.debug({ e }, 'Bybit host probe startup call failed');
    }

    // 5) Discover symbols
    try {
      logger.info('📊 Running initialScan() to populate symbols');
      await poller.initialScan();
      logger.info('✅ initialScan() completed');
    } catch (e) {
      logger.warn({ e }, '⚠️ initialScan failed during startup (continuing)');
    }

    // 6) Targeted synchronous seeding for limited symbols
    try {
      const startupSeedCount = Number(process.env.STARTUP_SEED_SYMBOLS || config.STARTUP_SEED_SYMBOLS || 50);
      let seedList = [];
      try {
        const dbInst = db.get();
        const rows = dbInst.prepare('SELECT symbol FROM symbols ORDER BY symbol COLLATE NOCASE ASC LIMIT ?').all(startupSeedCount);
        seedList = rows.map(r => ({ symbol: r.symbol }));
      } catch (e) {
        logger.debug({ e }, 'Failed to read symbols from DB for targeted seeding');
      }

      if (seedList.length && typeof poller.backgroundSeedKlines === 'function') {
        logger.info({ count: seedList.length }, '🌱 Seeding klines for top symbols before full flip pass');
        await poller.backgroundSeedKlines(seedList);
        logger.info('✅ Targeted seeding completed');
      } else {
        logger.info('ℹ️ No targeted seed list available; skipping targeted seeding');
      }
    } catch (e) {
      logger.warn({ e }, '⚠️ Targeted seeding failed (continuing)');
    }

    // 7) Full iteration across all symbols to detect flips (silent)
    try {
      logger.info('🔍 Running full symbol flip pass (silent) to populate signals for summary');
      if (typeof poller.scanAllForStartup === 'function') {
        await poller.scanAllForStartup();
      } else {
        await poller.scanOnce({ notifyNewSignals: false });
      }
      logger.info('✅ Full flip pass completed');
    } catch (e) {
      logger.warn({ e }, '⚠️ Full flip pass failed during startup (continuing)');
    }

    // 8) Send startup summary
    try {
      logger.info('📢 Sending startup summary');
      await signalManager.sendStartupSummary();
      logger.info('✅ Startup summary sent');
    } catch (e) {
      logger.debug({ e }, '⚠️ Failed to send startup summary (non-fatal)');
    }

    // 9) Start schedulers and managers
    poller.start();
    logger.info('✅ Poller started');
    
    wsManager.start();
    logger.info('✅ WebSocket manager started');
    
    signalManager.start();
    logger.info('✅ Signal manager started');
    
    tradeManager.registerWs(wsManager);
    logger.info('✅ Trade manager registered to WS events');

    // ============================================================
    // EXPRESS ROUTES (optimized for speed)
    // ============================================================

    // Root endpoint
    app.get('/', (req, res) => {
      res.json({ 
        ok: true, 
        version: '0.3.0',
        timestamp: new Date().toISOString()
      });
    });

    // Health check (lightweight, no DB queries)
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
            max_open_trades: config.MAX_OPEN_TRADES,
            opentrade_enabled: config.OPENTRADE,
            telegram_enabled: !!config.TELEGRAM_BOT_TOKEN
          }
        });
      } catch (err) {
        logger.error({ err }, 'Health check error');
        res.status(500).json({ error: err.message });
      }
    });

    // Status endpoint (OPTIMIZED with caching)
    app.get('/status', (req, res) => {
      try {
        const now = Date.now();
        
        // Return cached status if fresh
        if (cachedStatus && (now - lastStatusTime) < STATUS_CACHE_MS) {
          return res.json(cachedStatus);
        }

        const dbInst = db.get();
        if (!dbInst) {
          return res.status(503).json({ error: 'Database not available' });
        }

        // Get trade counts (fast queries)
        const openCount = dbInst.prepare('SELECT COUNT(*) as cnt FROM trades WHERE status = ?').get('open')?.cnt || 0;
        const closedCount = dbInst.prepare('SELECT COUNT(*) as cnt FROM trades WHERE status = ?').get('closed')?.cnt || 0;

        // Get latest signals (limit to 20)
        const signals = dbInst.prepare(`
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

        // Get open trades (simplified query)
        const openTrades = dbInst.prepare('SELECT id, symbol, opened_at, side, size, entry_price, tp, sl FROM trades WHERE status = ? ORDER BY opened_at DESC LIMIT 10').all('open');

        // Get symbol count
        const symbolCount = dbInst.prepare('SELECT COUNT(*) as count FROM symbols').get()?.count || 0;

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

    // Signals endpoint (for specific symbol)
    app.get('/signals/:symbol', (req, res) => {
      try {
        const { symbol } = req.params;
        if (!symbol || symbol.length === 0) {
          return res.status(400).json({ error: 'Symbol parameter required' });
        }

        const dbInst = db.get();
        const signals = dbInst.prepare(`
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

    // Market data endpoint
    app.get('/market/:symbol', (req, res) => {
      try {
        const { symbol } = req.params;
        if (!symbol || symbol.length === 0) {
          return res.status(400).json({ error: 'Symbol parameter required' });
        }

        const dbInst = db.get();
        const marketData = dbInst.prepare(`
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

    // TV rating endpoint
    app.get('/tv-rating/:symbol', (req, res) => {
      try {
        const { symbol } = req.params;
        if (!symbol || symbol.length === 0) {
          return res.status(400).json({ error: 'Symbol parameter required' });
        }

        const dbInst = db.get();
        const cached = dbInst.prepare(`
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

    // Manual scan endpoint (non-blocking)
    app.post('/scan', async (req, res) => {
      try {
        logger.info('🔄 Manual scan triggered via API');
        
        invalidateStatusCache(); // Clear cache when scan triggered
        
        res.json({
          status: 'scan_triggered',
          message: 'Scan initiated (results will be sent via Telegram)',
          timestamp: new Date().toISOString()
        });

        // Run scan in background (non-blocking)
        setImmediate(async () => {
          try {
            await poller.scanOnce({ notifyNewSignals: true });
            logger.info('✅ Manual scan completed successfully');
            invalidateStatusCache(); // Clear cache after scan completes
          } catch (err) {
            logger.error({ err }, '❌ Manual scan error');
          }
        });
      } catch (err) {
        logger.error({ err }, 'Manual scan endpoint error');
        res.status(500).json({ error: err.message });
      }
    });

    // 404 handler
    app.use((req, res) => {
      res.status(404).json({
        error: 'Not found',
        path: req.path,
        method: req.method
      });
    });

    // Error handler
    app.use((err, req, res, next) => {
      logger.error({ err, path: req.path, method: req.method }, 'Unhandled error in Express');
      res.status(500).json({
        error: 'Internal server error',
        message: err.message
      });
    });

    // ============================================================
    // START EXPRESS SERVER
    // ============================================================

    server = app.listen(PORT, '0.0.0.0', () => {
      logger.info({ PORT }, '✅ Express server listening');
    });

    // ============================================================
    // HEARTBEAT (log every 60 seconds)
    // ============================================================

    heartbeatInterval = setInterval(() => {
      logger.info({ ts: new Date().toISOString(), uptime: Math.floor(process.uptime()) }, 'heartbeat');
    }, 60_000);

    logger.info('🎉 Startup complete - trading bot is ready');
  } catch (err) {
    logger.error({ err }, '❌ Failed to start application');
    process.exit(1);
  }
}

// ============================================================
// GRACEFUL SHUTDOWN
// ============================================================

async function gracefulShutdown(signal) {
  logger.info({ signal }, '🛑 Starting graceful shutdown');
  try {
    if (heartbeatInterval) clearInterval(heartbeatInterval);

    if (server && server.close) {
      logger.info('Closing HTTP server...');
      await new Promise((resolve) => server.close(resolve));
      logger.info('✅ HTTP server closed');
    }

    try {
      if (wsManager && typeof wsManager.closeAll === 'function') {
        logger.info('Closing WebSocket connections...');
        await wsManager.closeAll();
        logger.info('✅ WebSocket connections closed');
      }
    } catch (e) {
      logger.warn({ e }, '⚠️ Failed to close WS manager cleanly');
    }

    try {
      const dbInst = db.get();
      if (db && typeof db.close === 'function') {
        logger.info('Closing database...');
        db.close();
        logger.info('✅ Database closed');
      } else if (dbInst && typeof dbInst.close === 'function') {
        logger.info('Closing database (via db.get())...');
        dbInst.close();
        logger.info('✅ Database closed');
      }
    } catch (e) {
      logger.warn({ e }, '⚠️ Error closing DB');
    }

    await new Promise((resolve) => setTimeout(resolve, 500));
  } catch (err) {
    logger.error({ err }, '❌ Error during graceful shutdown');
  } finally {
    logger.info('✅ Shutdown complete, exiting process');
    process.exit(0);
  }
}

process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

// ============================================================
// START APPLICATION
// ============================================================

start();

module.exports = app;
