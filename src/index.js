/**
 * Entrypoint - starts Express server and the poller/scanner
 *
 * UPDATED:
 * - Increased delay for startup summary (give poller time to seed and detect signals)
 * - Better logging for startup phases
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

// Global error handlers so unexpected errors show in logs
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
    // init DB
    db.init();

    // Start Bybit probe in background
    try {
      const bybitRest = require('./services/bybitRest');
      bybitRest.probeHosts(3000)
        .then((base) => {
          if (base) logger.info({ base }, 'probeHosts completed in background');
          else logger.warn('probeHosts completed in background with no selected base');
        })
        .catch((e) => {
          logger.debug({ e }, 'probeHosts background failure');
        });
    } catch (e) {
      logger.debug({ e }, 'probeHosts startup call failed');
    }

    // start REST poller (symbol discovery + root seeding)
    poller.start();
    // start websockets manager (batched)
    wsManager.start();
    // start signal manager (consumes cache & WS events)
    signalManager.start();
    // start telegram
    telegram.init();
    // register trade manager to listen to WS kline events for breakeven logic
    tradeManager.registerWs(wsManager);

    app.get('/', (req, res) => res.json({ ok: true, version: '0.3.0' }));

    server = app.listen(PORT, () => {
      logger.info({ PORT }, 'Server listening');
    });

    // heartbeat so Render logs show the process is alive
    heartbeatInterval = setInterval(() => {
      logger.info('heartbeat', { ts: new Date().toISOString() });
    }, 60_000);

    logger.info('Startup complete');

    // UPDATED: DELAY startup telegram summary to allow seeding/compilation
    // Give poller 15-20 seconds to discover and seed symbols, then another 8s for MACD computation
    const delayMs = parseInt(process.env.STARTUP_SUMMARY_DELAY_MS || '23000');
    logger.info({ delayMs }, 'Scheduling startup telegram summary');
    
    setTimeout(async () => {
      try {
        logger.info('Sending startup telegram summary after seeding delay...');
        await signalManager.sendStartupSummary();
      } catch (e) {
        logger.debug({ e }, 'Failed to send startup summary (non-fatal)');
      }
    }, delayMs);

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
      } else {
        if (wsManager && Array.isArray(wsManager.connections)) {
          wsManager.connections.forEach((c) => {
            try { c.ws && c.ws.close(); } catch (e) { /* noop */ }
          });
        }
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
