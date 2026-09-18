/**
 * Entrypoint - starts Express server and the poller/scanner.
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

const logger = pino({
  level: config.LOG_LEVEL || 'info'
});

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
    telegram.init();

    /*
     * Host probing is allowed to run in the background.
     * poller.start() owns all scan and seeding operations.
     */
    try {
      const bybitRest = require('./services/bybitRest');

      bybitRest.probeHosts(3000)
        .then((base) => {
          if (base) {
            logger.info({ base }, 'probeHosts completed in background');
          } else {
            logger.warn('probeHosts completed in background with no selected base');
          }
        })
        .catch((err) => {
          logger.debug({ err }, 'probeHosts background failure');
        });
    } catch (err) {
      logger.debug({ err }, 'probeHosts startup call failed');
    }

    /*
     * IMPORTANT:
     *
     * Do not call any of these directly here:
     *
     *   poller.initialScan()
     *   poller.backgroundSeedKlines()
     *   poller.scanAllForStartup()
     *
     * poller.start() owns those operations. Calling them here and then
     * calling poller.start() causes duplicate startup scans and duplicate
     * Telegram signal blocks.
     */
    poller.start();

    wsManager.start();
    signalManager.start();
    tradeManager.registerWs(wsManager);

    app.get('/', (req, res) => {
      res.json({
        ok: true,
        version: '0.3.0'
      });
    });

    server = app.listen(PORT, () => {
      logger.info({ PORT }, 'Server listening');
    });

    heartbeatInterval = setInterval(() => {
      logger.info(
        { ts: new Date().toISOString() },
        'heartbeat'
      );
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
    if (heartbeatInterval) {
      clearInterval(heartbeatInterval);
      heartbeatInterval = null;
    }

    if (server && typeof server.close === 'function') {
      logger.info('Closing HTTP server');

      await new Promise((resolve) => {
        server.close(() => resolve());
      });
    }

    try {
      if (wsManager && typeof wsManager.closeAll === 'function') {
        await wsManager.closeAll();
        logger.info('WS Manager closed all connections');
      }
    } catch (err) {
      logger.warn({ err }, 'Failed to close WS manager cleanly');
    }

    try {
      db.close();
      logger.info('Database closed');
    } catch (err) {
      logger.warn({ err }, 'Error closing database');
    }

    await new Promise(resolve => setTimeout(resolve, 500));
  } catch (err) {
    logger.error({ err }, 'Error during graceful shutdown');
  } finally {
    logger.info('Shutdown complete, exiting process');
    process.exit(0);
  }
}

process.on('SIGTERM', () => {
  gracefulShutdown('SIGTERM').catch(err => {
    logger.error({ err }, 'SIGTERM shutdown failed');
    process.exit(1);
  });
});

process.on('SIGINT', () => {
  gracefulShutdown('SIGINT').catch(err => {
    logger.error({ err }, 'SIGINT shutdown failed');
    process.exit(1);
  });
});

start().catch((err) => {
  logger.error({ err }, 'Application startup rejected');
  process.exit(1);
});
