// src/services/poller.js
/**
 * v0.4.0 Updates:
 * - Precise UTC 5-min boundary alignment (scans at exact :00, :05, :10, :15, etc.)
 * - Only sends NEW root signals (not seen in previous scan)
 * - Sends individual Telegram blocks per new signal
 * - Alignment confirmation alerts when ALL MTF align
 */

const dbModule = require('../db');
const bybit = require('./bybitRest');
const config = require('../config');
const logger = require('pino')();
const Bottleneck = require('bottleneck');
const macdUtil = require('./macd');
const signalManager = require('./signalManager');
const tradeManager = require('./tradeManager');

const limiter = new Bottleneck({ minTime: 50 });
const SEED_CONCURRENCY = Number(config.SEED_CONCURRENCY || 6);

let isRunning = false;
let lastRootCandleState = {}; // Track last root candle to prevent duplicate flips
let lastScanSignalKeys = new Set(); // Track signals from previous scan to detect NEW signals only

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms || 0));
}

/**
 * Calculate milliseconds until next precise UTC 5-min boundary
 * Returns exact time until :00, :05, :10, :15, :20, etc. UTC
 */
function msUntilNext5MinBoundary() {
  const now = new Date();
  const nowUTC = now.getTime();
  
  // Get current minute and second
  const minutes = now.getUTCMinutes();
  const seconds = now.getUTCSeconds();
  const milliseconds = now.getUTCMilliseconds();
  
  // Calculate next 5-min boundary
  const nextBoundaryMinutes = Math.ceil(minutes / 5) * 5;
  const nextBoundary = new Date(now);
  
  if (nextBoundaryMinutes >= 60) {
    // Next boundary is in next hour
    nextBoundary.setUTCHours(nextBoundary.getUTCHours() + 1);
    nextBoundary.setUTCMinutes(0);
  } else {
    nextBoundary.setUTCMinutes(nextBoundaryMinutes);
  }
  
  nextBoundary.setUTCSeconds(0);
  nextBoundary.setUTCMilliseconds(0);
  
  const msToWait = nextBoundary.getTime() - nowUTC;
  
  return {
    msToWait: Math.max(0, msToWait),
    boundaryTime: nextBoundary,
    currentMinute: minutes,
    nextMinute: nextBoundaryMinutes >= 60 ? 0 : nextBoundaryMinutes
  };
}

module.exports = {
  start() {
    if (isRunning) return;
    isRunning = true;

    try { signalManager.setOpenTradesAllowed(false); } catch (e) { /* ignore */ }

    try {
      bybit.probeHosts(3000)
        .then((base) => {
          if (base) logger.info({ base }, 'probeHosts completed in background');
          else logger.warn('probeHosts completed in background with no selected base');
        })
        .catch((e) => logger.debug({ e }, 'probeHosts background failure'));
    } catch (e) {
      logger.debug({ e }, 'probeHosts startup call failed');
    }

    (async () => {
      try {
        logger.info('poller: starting initialScan');
        await this.initialScan();
        logger.info('poller: initialScan completed');
        
        // Full silent startup scan
        try {
          await this.scanAllForStartup();
          logger.info('poller: startup full scan completed');
        } catch (err) {
          logger.error({ err }, 'poller: scanAllForStartup error');
        }
      } catch (err) {
        logger.error({ err }, 'poller: initialScan failed');
      }
    })();

    // Always use 5-min boundary alignment (ignore ROOT_MIDSCAN_INTERVAL)
    this.scheduleAlignedTo5m();
  },

  async initialScan() {
    logger.info('poller.initialScan: starting');

    let allSymbols = [];
    const useWs = !!config.USE_WS;

    if (useWs) {
      try {
        const wsTimeoutMs = config.WS_INITIAL_SCAN_TIMEOUT || 10000;
        logger.info({ timeoutMs: wsTimeoutMs }, 'poller: attempting WS initial scan');
        allSymbols = await Promise.race([
          this.performWsInitialScan(),
          new Promise((_, reject) => setTimeout(() => reject(new Error('WS scan timeout')), wsTimeoutMs))
        ]);
        if (!Array.isArray(allSymbols) || allSymbols.length === 0) {
          logger.warn('poller: WS initial scan returned no symbols; will fallback to REST');
          allSymbols = [];
        } else {
          logger.info({ count: allSymbols.length }, 'poller: WS initial scan provided symbols');
        }
      } catch (e) {
        logger.debug({ e }, 'poller: WS initial scan failed or timed out; fallback to REST');
        allSymbols = [];
      }
    }

    if (!allSymbols || allSymbols.length === 0) {
      logger.info('poller: fetching symbols via REST (cursor pagination)');
      allSymbols = await bybit.fetchAllSymbols();
    }

    if (!Array.isArray(allSymbols) || allSymbols.length === 0) {
      logger.warn('poller.initialScan: no symbols discovered');
      return;
    }

    const db = dbModule.get();
    const insert = db.prepare('INSERT OR REPLACE INTO symbols (symbol, base, quote, fetched_at) VALUES (?, ?, ?, ?)');
    const now = Date.now();
    const insertMany = db.transaction((rows) => {
      for (const s of rows) {
        insert.run(s.symbol, s.base || s.symbol.replace(/USDT[Pp]?$/i, ''), s.quote || 'USDT', now);
      }
    });
    insertMany(allSymbols.filter(s => s && s.symbol));
    logger.info({ total: allSymbols.length }, 'poller.initialScan: symbols persisted');

    const seedSymbols = bybit.getSeedSymbols(allSymbols);
    if (seedSymbols && seedSymbols.length) {
      setImmediate(() => this.backgroundSeedKlines(seedSymbols));
    } else {
      logger.info('poller.initialScan: no seed symbols to process (SYMBOL_SEED_ALL disabled or none)');
    }
  },

  async performWsInitialScan() {
    try {
      const wsManager = require('./bybitWs');
      if (wsManager && typeof wsManager.performInitialScan === 'function') {
        const res = await wsManager.performInitialScan();
        return Array.isArray(res) ? res : [];
      }
    } catch (e) {
      logger.debug({ e }, 'performWsInitialScan failed');
    }
    return [];
  },

  async backgroundSeedKlines(symbols = []) {
    if (!Array.isArray(symbols) || symbols.length === 0) {
      logger.info('backgroundSeedKlines: nothing to seed');
      return;
    }
    logger.info({ count: symbols.length, concurrency: SEED_CONCURRENCY }, 'backgroundSeedKlines: starting');

    for (let i = 0; i < symbols.length; i += SEED_CONCURRENCY) {
      const batch = symbols.slice(i, i + SEED_CONCURRENCY);
      const jobs = batch.map(s => limiter.schedule(() => this.seedKlinesForSymbol(s.symbol)));
      try {
        await Promise.all(jobs);
      } catch (e) {
        logger.debug({ e }, 'backgroundSeedKlines: batch failed (continuing)');
      }
    }
    logger.info('backgroundSeedKlines: completed');
  },

  async seedKlinesForSymbol(symbol, timeframe = null) {
    try {
      const rootTfs = timeframe ? [String(timeframe)] : (config.ROOT_TFS || []);
      const mtfTfs = Array.isArray(config.MTF_TFS) ? config.MTF_TFS.map(String) : [];
      const tfsSet = new Set([...(rootTfs || []), ...(mtfTfs || [])]);
      const tfs = Array.from(tfsSet);

      for (const tf of tfs) {
        const interval = String(tf) === 'D' ? 'D' : String(tf);
        try {
          const klines = await limiter.schedule(() => bybit.fetchKlines(symbol, interval, config.SEED_KLINES_LIMIT));
          if (!klines || klines.length === 0) {
            logger.debug({ symbol, tf }, 'seedKlinesForSymbol: no klines returned from API');
            continue;
          }

          const db = dbModule.get();
          const insert = db.prepare('INSERT OR IGNORE INTO klines (symbol, timeframe, open_time, open, high, low, close, volume) VALUES (?, ?, ?, ?, ?, ?, ?, ?)');
          const insertMany = db.transaction((rows) => {
            for (const k of rows) {
              insert.run(symbol, tf, k.open_time, k.open, k.high, k.low, k.close, k.volume);
            }
          });
          insertMany(klines);
          logger.debug({ symbol, tf, count: klines.length }, 'seedKlinesForSymbol: klines persisted');

          try {
            if (typeof macdUtil.computeAndStoreMacd === 'function') {
              await macdUtil.computeAndStoreMacd(symbol, tf);
            } else if (typeof macdUtil.computeMacdHistogram === 'function') {
              await macdUtil.computeMacdHistogram(symbol, tf);
            }
          } catch (err) {
            logger.debug({ err, symbol, tf }, 'seedKlinesForSymbol: macd warm-up failed (continuing)');
          }
        } catch (err) {
          logger.debug({ err, symbol, tf }, 'seedKlinesForSymbol: fetch failed (skipping tf)');
        }
      }
    } catch (err) {
      logger.debug({ err, symbol, timeframe }, 'seedKlinesForSymbol: unexpected error');
    }
  },

  /**
   * Feature 3: scanOnce at 5-min boundary (PRECISE UTC ALIGNMENT)
   * - Only sends NEW signals (not seen in previous scan)
   * - Sends individual Telegram blocks per new flip detected
   * - Checks alignment confirmation for symbols with partial alignment
   */
  async scanOnce({ notifyNewSignals = true, isStartup = false } = {}) {
    try {
      const db = dbModule;
      const scanStart = Date.now();
      
      // Get snapshot BEFORE scanning
      let snapshotBefore = [];
      try {
        if (typeof dbModule.getLatestSignalsSnapshot === 'function') {
          snapshotBefore = dbModule.getLatestSignalsSnapshot() || [];
        }
      } catch (e) {
        logger.debug({ e }, 'scanOnce: failed to get pre-scan snapshot');
      }

      const previousSignalKeys = new Set(snapshotBefore.map(r => r.key));

      // Scan all symbols for root TF flips
      const rows = db.get().prepare('SELECT symbol FROM symbols ORDER BY symbol COLLATE NOCASE ASC').all();
      for (let i = 0; i < rows.length; i += config.PAGE_SIZE) {
        const page = rows.slice(i, i + config.PAGE_SIZE);
        const tasks = page.map(r => this.scanSymbolRoots(r.symbol, { notifyImmediately: false, detected_ts: scanStart }));
        try {
          await Promise.all(tasks);
        } catch (e) {
          logger.debug({ e }, 'scanOnce: page tasks error (continuing)');
        }
      }

      // Get snapshot AFTER scanning
      let snapshotAfter = [];
      try {
        if (typeof dbModule.getLatestSignalsSnapshot === 'function') {
          snapshotAfter = dbModule.getLatestSignalsSnapshot() || [];
        }
      } catch (e) {
        logger.debug({ e }, 'scanOnce: failed to get post-scan snapshot');
      }

      // Find ONLY NEW signals (not in previous snapshot AND detected in this scan)
      const newSignals = snapshotAfter.filter(r => {
        const isNew = !previousSignalKeys.has(r.key);
        const isThisScan = r.detected_at >= scanStart;
        return isNew && isThisScan;
      });

      if (newSignals.length > 0) {
        logger.info({ 
          newSignalsCount: newSignals.length, 
          notifyNewSignals,
          isStartup
        }, 'scanOnce: NEW signals found this boundary');

        if (notifyNewSignals && !isStartup) {
          // Send individual blocks for EACH new signal
          const telegram = require('./telegram');
          for (let i = 0; i < newSignals.length; i++) {
            const s = newSignals[i];
            try {
              await telegram.sendNewSignalSingleBlock(s);
              logger.info({ symbol: s.symbol, root_tf: s.root_tf }, 'Sent individual signal block for new root signal');
            } catch (e) {
              logger.debug({ e, s }, 'scanOnce: failed to send new-signal message');
            }
            await sleep(config.TELEGRAM_SEND_DELAY_MS || 100);
          }

          // After NEW signals sent, check alignment confirmation for those symbols
          const newSymbols = new Set(newSignals.map(s => s.symbol));
          for (const symbol of newSymbols) {
            try {
              await signalManager.checkAlignmentConfirmation(symbol);
              logger.debug({ symbol }, 'Checked alignment confirmation after new signal');
            } catch (e) {
              logger.debug({ e, symbol }, 'scanOnce: alignment check error');
            }
            await sleep(50);
          }
        } else if (isStartup) {
          logger.info('scanOnce: startup scan complete; new signals will be shown in startup summary');
        }
      } else {
        logger.info({ isStartup }, 'scanOnce: no NEW signals found this boundary');
      }

      // Persist scan state
      try {
        db.setState('lastScanAt', scanStart);
        db.setState('lastScanSignalKeys', Array.from(new Set(snapshotAfter.map(r => r.key))));
      } catch (e) {
        logger.debug({ e }, 'scanOnce: failed to persist scan state');
      }

    } catch (err) {
      logger.error({ err }, 'scanOnce: unexpected error');
    }
  },

  async scanAllForStartup() {
    try {
      logger.info('scanAllForStartup: starting full startup pass (silent)');
      const db = dbModule.get();
      const rows = db.prepare('SELECT symbol FROM symbols ORDER BY symbol COLLATE NOCASE ASC').all();
      for (let i = 0; i < rows.length; i += config.PAGE_SIZE) {
        const page = rows.slice(i, i + config.PAGE_SIZE);
        const tasks = page.map(r => this.scanSymbolRoots(r.symbol, { notifyImmediately: false, detected_ts: Date.now() }));
        try {
          await Promise.all(tasks);
        } catch (e) {
          logger.debug({ e }, 'scanAllForStartup: page tasks error (continuing)');
        }
      }
      logger.info('scanAllForStartup: completed full startup pass');
    } catch (err) {
      logger.error({ err }, 'scanAllForStartup: unexpected error');
    }
  },

  async scanSymbolRoots(symbol, { notifyImmediately = true, detected_ts = null } = {}) {
    const tfList = config.ROOT_TFS || [];
    const results = [];
    for (const tf of tfList) {
      try {
        const db = dbModule.get();
        const selectStmt = db.prepare('SELECT open_time, close, open FROM klines WHERE symbol=? AND timeframe=? ORDER BY open_time DESC LIMIT 2');
        let rows = selectStmt.all(symbol, tf);
        if (!rows || rows.length < 2) {
          logger.debug({ symbol, tf }, 'scanSymbolRoots: insufficient klines, seeding now');
          await this.seedKlinesForSymbol(symbol, tf);

          rows = selectStmt.all(symbol, tf);
          if (!rows || rows.length < 2) {
            logger.debug({ symbol, tf }, 'scanSymbolRoots: still insufficient klines after seeding, skipping tf');
            continue;
          }
        }

        const stateKey = `${symbol}:${tf}`;
        const latestCandleTime = rows[rows.length - 1].open_time;
        
        if (lastRootCandleState[stateKey] === latestCandleTime) {
          logger.debug({ symbol, tf, candleTime: latestCandleTime }, 'scanSymbolRoots: already processed this candle, skipping');
          continue;
        }
        
        lastRootCandleState[stateKey] = latestCandleTime;

        const flip = await require('./macd').isMacdFlip(symbol, tf);
        if (flip) {
          const sig = await signalManager.handleRootSignal({
            symbol,
            root_tf: tf,
            detected_at: detected_ts || Date.now(),
            notifyImmediately
          });
          if (sig) results.push(sig);
        }
      } catch (err) {
        logger.debug({ err, symbol, tf }, 'scanSymbolRoots: error checking flip');
      }
    }
    return results;
  },

  /**
   * Schedule scans to run at PRECISE UTC 5-minute boundaries
   * Runs at: :00, :05, :10, :15, :20, :25, :30, :35, :40, :45, :50, :55
   */
  scheduleAlignedTo5m() {
    let firstBoundaryPassed = false;

    const schedule = async () => {
      const timing = msUntilNext5MinBoundary();
      const boundaryStr = `${String(timing.nextMinute).padStart(2, '0')}:00 UTC`;
      
      logger.info({ 
        msToWait: timing.msToWait, 
        boundaryTime: timing.boundaryTime.toISOString(),
        nextMinute: boundaryStr
      }, 'scheduleAlignedTo5m: waiting until next 5m boundary');

      setTimeout(async () => {
        try {
          const boundaryNow = new Date();
          logger.info({ boundaryTime: boundaryNow.toISOString() }, 'scheduleAlignedTo5m: executing at 5m boundary');

          // Run scan with notifications enabled at 5-min boundary
          await this.scanOnce({ notifyNewSignals: true, isStartup: false });

          // Check if any ROOT_TFS boundaries are crossed at this time
          const minute = boundaryNow.getUTCMinutes();
          const hour = boundaryNow.getUTCHours();
          const newRootTfs = [];

          for (const tf of config.ROOT_TFS) {
            if (String(tf).toUpperCase() === 'D') {
              if (hour === 0 && minute === 0) newRootTfs.push('D');
            } else {
              const tfNum = Number(tf);
              if (!isNaN(tfNum)) {
                const minutesSinceEpoch = Math.floor(boundaryNow.getTime() / 60000);
                if (minutesSinceEpoch % tfNum === 0) newRootTfs.push(String(tf));
              }
            }
          }

          // Close least profitable trade if enabled
          if (config.CLOSE_LEAST_PROFITABLE_ENABLED && newRootTfs.length > 0) {
            const openCount = dbModule.get().prepare('SELECT COUNT(*) as c FROM trades WHERE status = ?').get('open').c || 0;
            if (openCount >= config.MAX_OPEN_TRADES) {
              const minutesSinceHour = boundaryNow.getUTCMinutes();
              const closeMins = config.CLOSE_LEAST_PROFITABLE_MINS_BEFORE_BOUNDARY || 5;
              if (minutesSinceHour >= (60 - closeMins)) {
                logger.info({ openCount, closeMins }, 'Closing least profitable trade before boundary');
                try {
                  await tradeManager.closeLeastProfitableTrade();
                } catch (err) {
                  logger.error({ err }, 'Error closing least profitable trade');
                }
              }
            }
          }

          // Send root candle update if any ROOT_TFS crossed boundaries
          if (newRootTfs.length && config.NEW_ROOT_CANDLE_NOTIFY) {
            try {
              logger.info({ newRootTfs }, 'Sending root candle update for new root TF boundaries');
              await signalManager.handleNewRootCandle(newRootTfs);
            } catch (e) {
              logger.debug({ e, newRootTfs }, 'scheduleAlignedTo5m: handleNewRootCandle failed');
            }
          }

          // Enable open trades after first boundary
          if (!firstBoundaryPassed) {
            firstBoundaryPassed = true;
            try {
              signalManager.setOpenTradesAllowed(true);
              logger.info('scheduleAlignedTo5m: open trades enabled after first boundary');
            } catch (e) {
              logger.debug({ e }, 'scheduleAlignedTo5m: failed to set open trades allowed');
            }
          }
        } catch (err) {
          logger.error({ err }, 'scheduleAlignedTo5m: boundary task failed');
        } finally {
          schedule(); // Reschedule for next 5-min boundary
        }
      }, timing.msToWait);
    };

    schedule();
  }
};
