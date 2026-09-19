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

const app = express();

app.use(express.json());
app.use('/debug', debugRoutes);

app.get('/', (req, res) => {
  res.status(200).json({
    ok: true,
    version: '0.3.0'
  });
});

app.get('/health', (req, res) => {
  res.status(200).json({
    ok: true,
    service: 'bybit-perps'
  });
});

const PORT = Number(
  process.env.PORT || config.PORT || 3000
);

let server;
let heartbeatInterval;
let shuttingDown = false;

process.on('uncaughtException', err => {
  logger.fatal(
    { err },
    'UNCAUGHT EXCEPTION'
  );

  /*
   * Do not silently continue after an unknown process-level error.
   * Render will restart the service if the process exits.
   */
});

process.on('unhandledRejection', reason => {
  logger.error(
    { reason },
    'UNHANDLED REJECTION'
  );
});

function listenForRender() {
  return new Promise((resolve, reject) => {
    server = app.listen(
      PORT,
      '0.0.0.0',
      () => {
        logger.info(
          { port: PORT },
          'HTTP server listening'
        );

        resolve(server);
      }
    );

    server.once('error', reject);
  });
}

async function subscribeDatabaseSymbols() {
  if (!config.USE_WS) {
    logger.info(
      'USE_WS=false; skipping database WebSocket subscriptions'
    );

    return;
  }

  const database = db.get();

  const rows = database.prepare(
    'SELECT symbol FROM symbols ORDER BY symbol COLLATE NOCASE ASC'
  ).all();

  let subscribed = 0;

  for (const row of rows) {
    if (!row || !row.symbol) {
      continue;
    }

    const connection =
      wsManager.subscribeSymbolMTF(row.symbol);

    if (connection) {
      subscribed++;
    }
  }

  logger.info(
    {
      discovered: rows.length,
      subscribed
    },
    'Database symbols submitted to WebSocket manager'
  );
}

async function runApplicationStartup() {
  try {
    logger.info('Starting application');

    db.init();
    telegram.init();

    /*
     * Start the WS manager independently. It creates a socket but does
     * not subscribe until symbols have been discovered.
     */
    if (config.USE_WS) {
      wsManager.start();
    } else {
      logger.info(
        'WebSocket manager disabled because USE_WS=false'
      );
    }

    /*
     * The HTTP server is already listening before these potentially
     * slow startup operations run.
     */
    logger.info(
      'Startup: discovering symbols'
    );

    await poller.initialScan();

    if (config.USE_WS) {
      await subscribeDatabaseSymbols();
    }

    try {
      const startupSeedCount = Number(
        process.env.STARTUP_SEED_SYMBOLS ||
        config.STARTUP_SEED_SYMBOLS ||
        50
      );

      const database = db.get();

      const rows = database.prepare(
        'SELECT symbol FROM symbols ' +
        'ORDER BY symbol COLLATE NOCASE ASC LIMIT ?'
      ).all(startupSeedCount);

      const seedList = rows.map(row => ({
        symbol: row.symbol
      }));

      if (
        seedList.length &&
        typeof poller.backgroundSeedKlines === 'function'
      ) {
        logger.info(
          { count: seedList.length },
          'Startup: seeding initial symbols'
        );

        await poller.backgroundSeedKlines(seedList);
      }
    } catch (err) {
      logger.warn(
        { err },
        'Startup targeted seeding failed; continuing'
      );
    }

    try {
      logger.info(
        'Startup: running full silent flip pass'
      );

      if (
        typeof poller.scanAllForStartup === 'function'
      ) {
        await poller.scanAllForStartup();
      }
    } catch (err) {
      logger.warn(
        { err },
        'Startup full flip pass failed; continuing'
      );
    }

    try {
      await signalManager.sendStartupSummary();
    } catch (err) {
      logger.debug(
        { err },
        'Startup summary failed; continuing'
      );
    }

    poller.start();
    signalManager.start();
    tradeManager.registerWs(wsManager);

    logger.info('Application startup complete');
  } catch (err) {
    logger.error(
      { err },
      'Background application startup failed'
    );
  }
}

async function gracefulShutdown(signal) {
  if (shuttingDown) {
    return;
  }

  shuttingDown = true;

  logger.info(
    { signal },
    'Starting graceful shutdown'
  );

  try {
    if (heartbeatInterval) {
      clearInterval(heartbeatInterval);
    }

    if (wsManager && typeof wsManager.closeAll === 'function') {
      await wsManager.closeAll();
    }

    if (server) {
      await new Promise(resolve => {
        server.close(() => resolve());
      });
    }

    try {
      const database = db.get();

      if (
        database &&
        typeof database.close === 'function'
      ) {
        database.close();
      } else if (
        db &&
        typeof db.close === 'function'
      ) {
        db.close();
      }
    } catch (err) {
      logger.warn(
        { err },
        'Failed to close database'
      );
    }
  } catch (err) {
    logger.error(
      { err },
      'Graceful shutdown error'
    );
  } finally {
    logger.info('Shutdown complete');
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

async function main() {
  try {
    await listenForRender();

    heartbeatInterval = setInterval(() => {
      logger.info(
        {
          ts: new Date().toISOString(),
          wsConnections: wsManager.connections.length
        },
        'heartbeat'
      );
    }, 60000);

    /*
     * Run the slow startup sequence only after Render can reach
     * the health endpoint.
     */
    setImmediate(() => {
      runApplicationStartup().catch(err => {
        logger.error(
          { err },
          'Application startup task failed'
        );
      });
    });
  } catch (err) {
    logger.fatal(
      { err },
      'Failed to bind HTTP server'
    );

    process.exit(1);
  }
}

main();
