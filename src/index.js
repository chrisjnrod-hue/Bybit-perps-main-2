const path = require('path');
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
  logger.error(
    { err },
    'UNCAUGHT EXCEPTION - the process may terminate'
  );
});

process.on('unhandledRejection', (reason) => {
  logger.error(
    { reason },
    'UNHANDLED REJECTION - promise rejected without handler'
  );
});

const app = express();

app.use(express.json());
app.use('/debug', debugRoutes);

const PORT = process.env.PORT || config.PORT || 3000;

let server;
let heartbeatInterval;

async function runStartup() {
  logger.info('Startup: initializing database');
  db.init();

  logger.info('Startup: initializing Telegram');
  telegram.init();

  try {
    const bybitRest = require('./services/bybitRest');

    bybitRest
      .probeHosts(3000)
      .then((base) => {
        if (base) {
          logger.info(
            { base },
            'probeHosts completed in background'
          );
        } else {
          logger.warn(
            'probeHosts completed in background with no selected base'
          );
        }
      })
      .catch((err) => {
        logger.debug(
          { err },
          'probeHosts background failure'
        );
      });
  } catch (err) {
    logger.debug(
      { err },
      'probeHosts startup call failed'
    );
  }

  try {
    logger.info('Startup: discovering symbols');
    await poller.initialScan({ seed: false });
  } catch (err) {
    logger.warn(
      { err },
      'initialScan failed during startup; continuing'
    );
  }

  try {
    const startupSeedCount = Number(
      process.env.STARTUP_SEED_SYMBOLS ||
      config.STARTUP_SEED_SYMBOLS ||
      50
    );

    const dbInstance = db.get();

    const rows = dbInstance
      .prepare(
        `
          SELECT symbol
          FROM symbols
          ORDER BY symbol COLLATE NOCASE ASC
          LIMIT ?
        `
      )
      .all(startupSeedCount);

    const seedList = rows.map((row) => ({
      symbol: row.symbol
    }));

    if (
      seedList.length > 0 &&
      typeof poller.backgroundSeedKlines === 'function'
    ) {
      logger.info(
        {
          count: seedList.length
        },
        'Startup: synchronously seeding startup symbols'
      );

      await poller.backgroundSeedKlines(seedList);
    } else {
      logger.info('Startup: no symbols available for targeted seeding');
    }
  } catch (err) {
    logger.warn(
      { err },
      'Startup: targeted seeding failed; continuing'
    );
  }

  try {
    logger.info('Startup: running full startup flip pass');
    await poller.scanAllForStartup();
    logger.info('Startup: full startup flip pass completed');
  } catch (err) {
    logger.warn(
      { err },
      'Startup: full startup flip pass failed; continuing'
    );
  }

  poller.start();
  poller.startBoundaryScanLoop();
  poller.startMtfAlignmentLoop();

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
    logger.info(
      { PORT },
      'Server listening'
    );
  });

  heartbeatInterval = setInterval(() => {
    logger.info(
      {
        ts: new Date().toISOString()
      },
      'heartbeat'
    );
  }, 60_000);

  logger.info('Startup complete');
}

async function start() {
  try {
    logger.info('Starting app');
    await runStartup();
  } catch (err) {
    logger.error(
      { err },
      'Failed to start application'
    );

    process.exit(1);
  }
}

async function gracefulShutdown(signal) {
  logger.info(
    { signal },
    'Starting graceful shutdown'
  );

  try {
    if (heartbeatInterval) {
      clearInterval(heartbeatInterval);
      heartbeatInterval = null;
    }

    if (server && typeof server.close === 'function') {
      logger.info('Closing HTTP server');

      await new Promise((resolve) => {
        server.close(resolve);
      });
    }

    try {
      if (
        wsManager &&
        typeof wsManager.closeAll === 'function'
      ) {
        await wsManager.closeAll();

        logger.info(
          'WS manager closed all connections'
        );
      } else if (
        wsManager &&
        Array.isArray(wsManager.connections)
      ) {
        wsManager.connections.forEach((connection) => {
          try {
            if (connection.ws) {
              connection.ws.close();
            }
          } catch (_) {
            // Ignore individual connection close failures.
          }
        });
      }
    } catch (err) {
      logger.warn(
        { err },
        'Failed to close WS manager cleanly'
      );
    }

    try {
      if (db && typeof db.close === 'function') {
        db.close();
        logger.info('Database closed');
      } else {
        const dbInstance = db.get();

        if (
          dbInstance &&
          typeof dbInstance.close === 'function'
        ) {
          dbInstance.close();
          logger.info('Database closed through db.get()');
        }
      }
    } catch (err) {
      logger.warn(
        { err },
        'Error closing database'
      );
    }

    await new Promise((resolve) => {
      setTimeout(resolve, 500);
    });
  } catch (err) {
    logger.error(
      { err },
      'Error during graceful shutdown'
    );
  } finally {
    logger.info(
      'Shutdown complete, exiting process'
    );

    process.exit(0);
  }
}

process.on('SIGTERM', () => {
  gracefulShutdown('SIGTERM');
});

process.on('SIGINT', () => {
  gracefulShutdown('SIGINT');
});

start();
