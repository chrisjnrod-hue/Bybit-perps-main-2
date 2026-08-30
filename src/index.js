/**
 * Entrypoint - starts Express server and the poller/scanner
 *
 * Added:
 * - global uncaughtException / unhandledRejection handlers
 * - heartbeat logging
 * - graceful shutdown that attempts to close DB and WS connections
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

    // Probe Bybit hosts to pick a working base (helps in environments with multiple mirrors)
    try {
      const bybitRest = require('./services/bybitRest');
      await bybitRest.probeHosts(3000);
    } catch (e) {
      logger.debug({ e }, 'probeHosts call failed (continuing)');
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
  } catch (err) {
    logger.error({ err }, 'Failed to start application');
    // exit non-zero so Render restarts
    process.exit(1);
  }
}

async function gracefulShutdown(signal) {
  logger.info({ signal }, 'Starting graceful shutdown');
  try {
    // stop heartbeat
    if (heartbeatInterval) clearInterval(heartbeatInterval);

    // stop accepting new connections
    if (server && server.close) {
      logger.info('Closing HTTP server');
      await new Promise((resolve) => server.close(resolve));
    }

    // try to close WS connections
    try {
      if (wsManager && typeof wsManager.closeAll === 'function') {
        await wsManager.closeAll();
        logger.info('WS Manager closed all connections');
      } else {
        // best-effort: if connections array exists close sockets
        if (wsManager && Array.isArray(wsManager.connections)) {
          wsManager.connections.forEach((c) => {
            try { c.ws && c.ws.close(); } catch (e) { /* noop */ }
          });
        }
      }
    } catch (e) {
      logger.warn({ e }, 'Failed to close WS manager cleanly');
    }

    // close DB
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

    // small delay to let things flush
    await new Promise((resolve) => setTimeout(resolve, 500));
  } catch (err) {
    logger.error({ err }, 'Error during graceful shutdown');
  } finally {
    logger.info('Shutdown complete, exiting process');
    process.exit(0);
  }
}

// Capture termination signals from Render
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

// Start the app
start();
