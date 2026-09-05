/**
 * Entrypoint - starts Express server and the poller/scanner
 *
 * Startup flow:
 *  - db.init()
 *  - telegram.init()
 *  - poller.initialScan()
 *  - targeted seeding for STARTUP_SEED_SYMBOLS
 *  - poller.scanOnce({ notifyNewSignals: false })  // silent persist-only quick scan
 *  - signalManager.sendStartupSummary()
 *  - poller.start(), wsManager.start(), signalManager.start()
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
app.use('/debug', debugRoutes);

const PORT = process.env.PORT || config.PORT || 3000;
let server;
let heartbeatInterval;

async function start() {
  try {
    logger.info('Starting app...');
    db.init();

    // init telegram early so any immediate notifications are possible
    telegram.init();

    // background bybit probe (non-blocking)
    try {
      const bybitRest = require('./services/bybitRest');
      bybitRest.probeHosts(3000)
        .then((base) => {
          if (base) logger.info({ base }, 'probeHosts completed in background');
          else logger.warn('probeHosts completed in background with no selected base');
        })
        .catch((e) => logger.debug({ e }, 'probeHosts background failure'));
    } catch (e) {
      logger.debug({ e }, 'probeHosts startup call failed');
    }

    // 1) Discover symbols
    try {
      logger.info('Startup: running initialScan() to populate symbols');
      await poller.initialScan();
    } catch (e) {
      logger.warn({ e }, 'initialScan failed during startup (continuing)');
    }

    // 2) targeted synchronous seeding for a limited number of symbols so klines + MACD are available.
    try {
      const startupSeedCount = Number(process.env.STARTUP_SEED_SYMBOLS || config.STARTUP_SEED_SYMBOLS || 50);
      let seedList = [];
      try {
        const dbInst = db.get();
        const rows = dbInst.prepare('SELECT symbol FROM symbols ORDER BY symbol COLLATE NOCASE ASC LIMIT ?').all(startupSeedCount);
        seedList = rows.map(r => ({ symbol: r.symbol }));
      } catch (e) {
        logger.debug({ e }, 'Startup: failed to read symbols from DB for targeted seeding (will still attempt quick scan)');
      }

      if (seedList.length && typeof poller.backgroundSeedKlines === 'function') {
        logger.info({ count: seedList.length }, 'Startup: seeding klines for top symbols before quick scan');
        await poller.backgroundSeedKlines(seedList);
      } else {
        logger.info('Startup: no targeted seed list available or backgroundSeedKlines not present; skipping targeted seeding');
      }
    } catch (e) {
      logger.warn({ e }, 'Startup: targeted seeding failed (continuing)');
    }

    // 3) silent quick scanOnce to persist any detected signals but DO NOT notify per-signal messages
    try {
      logger.info('Startup: running quick silent scanOnce() to detect flips and populate signals (no notifications)');
      if (typeof poller.scanOnce === 'function') {
        await poller.scanOnce({ notifyNewSignals: false });
      }
    } catch (e) {
      logger.warn({ e }, 'scanOnce failed during startup quick pass (continuing)');
    }

    // 4) send startup summary now that snapshot should be populated
    try {
      logger.info('Startup: sending startup summary (after targeted seeding and silent scan)');
      await signalManager.sendStartupSummary();
    } catch (e) {
      logger.debug({ e }, 'Failed to send startup summary (non-fatal)');
    }

    // 5) start schedulers and managers
    poller.start();
    wsManager.start();
    signalManager.start();
    tradeManager.registerWs(wsManager);

    app.get('/', (req, res) => res.json({ ok: true, version: '0.3.0' }));

    server = app.listen(PORT, () => {
      logger.info({ PORT }, 'Server listening');
    });

    heartbeatInterval = setInterval(() => {
      logger.info('heartbeat', { ts: new Date().toISOString() });
    }, 60_000);

    logger.info('Startup complete');
  } catch (err) {
    logger.error({ err }, 'Failed to start application');
    process.exit(1);
  }
}

async function gracefulShutdown(signal) {
  logger.info({ signal }, 'Starting graceful shutdown');
  try {
    if (heartbeatInterval) clearInterval(heartbeatInterval);

    if (server && server.close) {
      logger.info('Closing HTTP server');
      await new Promise((resolve) => server.close(resolve));
    }

    try {
      if (wsManager && typeof wsManager.closeAll === 'function') {
        await wsManager.closeAll();
        logger.info('WS Manager closed all connections');
      } else if (wsManager && Array.isArray(wsManager.connections)) {
        wsManager.connections.forEach((c) => { try { c.ws && c.ws.close(); } catch (e) {} });
      }
    } catch (e) {
      logger.warn({ e }, 'Failed to close WS manager cleanly');
    }

    try {
      const dbInstance = db.get();
      if (db && typeof db.close === 'function') {
        db.close();
        logger.info('Database closed');
      } else if (dbInstance && typeof dbInstance.close === 'function') {
        dbInstance.close();
        logger.info('Database closed (db.get())');
      }
    } catch (e) {
      logger.warn({ e }, 'Error closing DB');
    }

    await new Promise((resolve) => setTimeout(resolve, 500));
  } catch (err) {
    logger.error({ err }, 'Error during graceful shutdown');
  } finally {
    logger.info('Shutdown complete, exiting process');
    process.exit(0);
  }
}

process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

start();
