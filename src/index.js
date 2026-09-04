/**
 * Entrypoint - starts Express server and the poller/scanner
 *
 * CRITICAL FIX:
 * - Wait for seeding AND first scan to complete before sending ANY telegram blocks
 * - Only send startup summary after signals are actually detected
 * - Better logging and error handling
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
    logger.info('═══════════════════════════════════════════════');
    logger.info('Starting app...');
    logger.info('═══════════════════════════════════════════════');
    
    // init DB
    db.init();
    logger.info('✅ Database initialized');

    // Start Bybit probe in background
    try {
      const bybitRest = require('./services/bybitRest');
      bybitRest.probeHosts(3000)
        .then((base) => {
          if (base) logger.info({ base }, '✅ probeHosts completed in background');
          else logger.warn('⚠️ probeHosts completed in background with no selected base');
        })
        .catch((e) => {
          logger.debug({ e }, 'probeHosts background failure');
        });
    } catch (e) {
      logger.debug({ e }, 'probeHosts startup call failed');
    }

    // start REST poller (symbol discovery + root seeding)
    logger.info('🌱 Starting poller...');
    poller.start();
    logger.info('✅ Poller started');

    // start websockets manager (batched)
    logger.info('🌐 Starting WS manager...');
    try {
      const maybe = wsManager.start();
      if (maybe && typeof maybe.then === 'function') await maybe;
      logger.info('✅ WS Manager started');
    } catch (e) {
      logger.error({ e }, 'WS Manager failed to start (caught and continuing)');
    }

    // start signal manager (consumes cache & WS events)
    logger.info('📊 Starting signal manager...');
    signalManager.start();
    logger.info('✅ Signal Manager started');

    // start telegram
    logger.info('💬 Initializing telegram...');
    try {
      telegram.init && telegram.init();
      logger.info('✅ Telegram initialized');
    } catch (e) {
      logger.warn({ e }, 'Telegram init failed (continuing)');
    }

    // register trade manager to listen to WS kline events for breakeven logic
    logger.info('📈 Registering trade manager...');
    tradeManager.registerWs(wsManager);
    logger.info('✅ Trade Manager registered');

    app.get('/', (req, res) => res.json({ ok: true, version: '0.3.0' }));

    server = app.listen(PORT, () => {
      logger.info({ PORT }, '✅ Server listening on port');
    });

    // heartbeat so Render logs show the process is alive
    heartbeatInterval = setInterval(() => {
      logger.info('💓 heartbeat', { ts: new Date().toISOString() });
    }, 60_000);

    logger.info('═══════════════════════════════════════════════');
    logger.info('✅ Startup complete - waiting for seeding and signal detection');
    logger.info('═══════════════════════════════════════════════');

    // CRITICAL FIX: Wait for seeding to complete, then wait for first scan to detect signals
    // Only THEN send the startup summary
    const checkAndSendSummary = async () => {
      let maxWaitTime = 120000; // 2 minutes max wait
      let checkInterval = 1000; // Check every 1 second
      let elapsedTime = 0;
      let lastLogTime = 0;

      logger.info('═══════════════════════════════════════════════');
      logger.info('🔍 Starting readiness check loop...');
      logger.info('═══════════════════════════════════════════════');

      while (elapsedTime < maxWaitTime) {
        const isSeedingDone = poller.isSeedingComplete();
        const allSignals = signalManager.getAllSignals();
        const signalCount = allSignals.length;

        // Also require that a scan has run at least once
        let lastScanAt = null;
        try {
          lastScanAt = db.getState ? db.getState('lastScanAt') : null;
        } catch (e) {
          lastScanAt = null;
        }
        const hasFirstScan = !!lastScanAt;

        // Log every 5 seconds to avoid spam
        if (elapsedTime - lastLogTime >= 5000 || elapsedTime < 1000) {
          logger.info({ 
            seedingDone: isSeedingDone, 
            signalCount,
            hasFirstScan,
            elapsedSeconds: Math.round(elapsedTime / 1000),
            maxSeconds: Math.round(maxWaitTime / 1000)
          }, '⏳ Readiness check...');
          lastLogTime = elapsedTime;
        }

        // Check conditions: seeding done AND at least 1 signal detected AND at least one scan has run
        if (isSeedingDone && hasFirstScan && signalCount > 0) {
          logger.info('═══════════════════════════════════════════════');
          logger.info({ 
            seedingDone: isSeedingDone,
            signalCount,
            elapsedSeconds: Math.round(elapsedTime / 1000)
          }, '✅ READY! Seeding complete and signals detected');
          logger.info('═══════════════════════════════════════════════');
          
          try {
            logger.info('⏳ Waiting 2 seconds for any pending signals...');
            await new Promise(resolve => setTimeout(resolve, 2000));
            
            logger.info('🚀 Sending startup telegram summary...');
            await signalManager.sendStartupSummary();
            logger.info('═══════════════════════════════════════════════');
            logger.info('✅ Startup summary sent successfully');
            logger.info('═══════════════════════════════════════════════');
            return; // Exit after successful send
          } catch (e) {
            logger.error({ e }, '❌ Failed to send startup summary');
            logger.info('═══════════════════════════════════════════════');
            return;
          }
        }

        // Wait before checking again
        await new Promise(resolve => setTimeout(resolve, checkInterval));
        elapsedTime += checkInterval;
      }

      // Max time reached - send summary anyway
      logger.warn('═══════════════════════════════════════════════');
      logger.warn({ 
        maxWaitTime,
        elapsedSeconds: Math.round(elapsedTime / 1000)
      }, '⏱️ Max wait time reached (2 minutes). Sending startup summary anyway.');
      logger.warn('═══════════════════════════════════════════════');
      
      try {
        logger.info('🚀 Sending startup telegram summary after timeout...');
        await signalManager.sendStartupSummary();
        logger.info('═══════════════════════════════════════════════');
        logger.info('✅ Startup summary sent after timeout');
        logger.info('═══════════════════════════════════════════════');
      } catch (e) {
        logger.error({ e }, '❌ Failed to send startup summary after timeout');
        logger.info('═══════════════════════════════════════════════');
      }
    };

    // Run check in background (non-blocking)
    checkAndSendSummary().catch(err => logger.error({ err }, 'checkAndSendSummary exception'));

  } catch (err) {
    logger.error({ err }, 'Failed to start application');
    // exit non-zero so Render restarts
    process.exit(1);
  }
}

async function gracefulShutdown(signal) {
  logger.info('═══════════════════════════════════════════════');
  logger.info({ signal }, 'Starting graceful shutdown');
  logger.info('═══════════════════════════════════════════════');
  try {
    // stop heartbeat
    if (heartbeatInterval) clearInterval(heartbeatInterval);

    // stop accepting new connections
    if (server && server.close) {
      logger.info('Closing HTTP server');
      await new Promise((resolve) => server.close(resolve));
      logger.info('✅ HTTP server closed');
    }

    // try to close WS connections
    try {
      if (wsManager && typeof wsManager.closeAll === 'function') {
        await wsManager.closeAll();
        logger.info('✅ WS Manager closed all connections');
      } else {
        // best-effort: if connections array exists close sockets
        if (wsManager && Array.isArray(wsManager.connections)) {
          wsManager.connections.forEach((c) => {
            try { c.ws && c.ws.close(); } catch (e) { /* noop */ }
          });
          logger.info('✅ WS connections closed');
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
        logger.info('✅ Database closed');
      } else if (dbInstance && typeof dbInstance.close === 'function') {
        dbInstance.close();
        logger.info('✅ Database closed (db.get())');
      }
    } catch (e) {
      logger.warn({ e }, 'Error closing DB');
    }

    // small delay to let things flush
    await new Promise((resolve) => setTimeout(resolve, 500));
  } catch (err) {
    logger.error({ err }, 'Error during graceful shutdown');
  } finally {
    logger.info('═══════════════════════════════════════════════');
    logger.info('Shutdown complete, exiting process');
    logger.info('═══════════════════════════════════════════════');
    process.exit(0);
  }
}

// Capture termination signals from Render
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

// Start the app
start();
