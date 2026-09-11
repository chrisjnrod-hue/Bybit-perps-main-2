// src/services/poller.js
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

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms || 0));
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

    if (config.ROOT_MIDSCAN_INTERVAL && Number(config.ROOT_MIDSCAN_INTERVAL) > 0) {
      setInterval(() => this.scanOnce(), Number(config.ROOT_MIDSCAN_INTERVAL) * 1000);

      const msToNext5 = () => {
        const d = new Date();
        const m = d.getUTCMinutes();
        const next = new Date(d);
        const deltaM = 5 - (m % 5);
        next.setUTCMinutes(m + deltaM);
        next.setUTCSeconds(0);
        next.setUTCMilliseconds(500);
        return next - d;
      };
      setTimeout(() => {
        try {
          signalManager.setOpenTradesAllowed(true);
          logger.info('Open trades enabled at next 5m boundary (interval mode)');
        } catch (e) { logger.debug({ e }, 'Failed to set open trades allowed'); }
      }, msToNext5());
    } else {
      this.scheduleAlignedTo5m();
    }
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
   * FIXED: scanOnce at 5-min boundary
   * - Scans all ROOT_TFS for new flips
   * - Checks alignment confirmation for each symbol
   * - Sends individual signal blocks per new flip detected
   */
  async scanOnce({ notifyNewSignals = true } = {}) {
    try {
      logger.info({ notifyNewSignals }, 'scanOnce: starting full scan');

      const db = dbModule;
      const prev = db.getLatestSignalsSnapshot ? db.getLatestSignalsSnapshot() : [];
      const prevKeys = new Set(prev.map(r => r.key));
      
      logger.info({ previousSignalCount: prevKeys.size }, 'scanOnce: loaded previous signal snapshot');

      const scanStart = Date.now();

      const rows = db.get().prepare('SELECT symbol FROM symbols ORDER BY symbol COLLATE NOCASE ASC').all();
      logger.info({ totalSymbols: rows.length }, 'scanOnce: starting symbol iteration');

      for (let i = 0; i < rows.length; i += config.PAGE_SIZE) {
        const page = rows.slice(i, i + config.PAGE_SIZE);
        const tasks = page.map(r => this.scanSymbolRoots(r.symbol, { 
          notifyImmediately: false, 
          detected_ts: scanStart,
          isBoundaryScan: true
        }));
        try {
          await Promise.all(tasks);
        } catch (e) {
          logger.debug({ e, pageNum: Math.floor(i / config.PAGE_SIZE) }, 'scanOnce: page tasks error (continuing)');
        }
      }

      const after = db.getLatestSignalsSnapshot ? db.getLatestSignalsSnapshot() : [];
      const newSignals = after.filter(r => {
        const isNew = !prevKeys.has(r.key);
        const isRecent = r.detected_at >= scanStart;
        return isNew && isRecent;
      });

      logger.info({ 
        previousCount: prev.length, 
        currentCount: after.length, 
        newSignalsCount: newSignals.length, 
        notifyNewSignals 
      }, 'scanOnce: scan iteration complete, processing signals');

      if (newSignals.length > 0) {
        logger.info({ newSignals: newSignals.length, notifyNewSignals }, 'scanOnce: NEW SIGNALS DETECTED');

        if (notifyNewSignals) {
          const telegram = require('./telegram');
          for (let i = 0; i < newSignals.length; i++) {
            const s = newSignals[i];
            try {
              logger.info({ symbol: s.symbol, root_tf: s.root_tf, signalNum: i + 1, totalSignals: newSignals.length }, 'scanOnce: sending telegram signal block');
              await telegram.sendNewSignalSingleBlock(s);
              logger.info({ symbol: s.symbol, root_tf: s.root_tf }, 'scanOnce: signal block sent successfully');
            } catch (e) {
              logger.warn({ e, symbol: s.symbol, root_tf: s.root_tf }, 'scanOnce: failed to send new-signal message');
            }
            await sleep(config.TELEGRAM_SEND_DELAY_MS || 100);
          }

          // After all signals sent, check alignment confirmation for monitored symbols
          logger.info('scanOnce: checking alignment confirmations for all symbols');
          for (let i = 0; i < rows.length; i++) {
            const symbol = rows[i].symbol;
            try {
              await signalManager.checkAlignmentConfirmation(symbol);
            } catch (e) {
              logger.debug({ e, symbol }, 'scanOnce: alignment check error');
            }
            await sleep(50);
          }
        } else {
          logger.info('scanOnce: notifications suppressed for this run (silent startup/root-open scan)');
        }
      } else {
        logger.info('scanOnce: no new signals found this boundary');
        
        // Still check alignment confirmations even if no new signals
        if (notifyNewSignals) {
          logger.info('scanOnce: checking alignment confirmations for all symbols (no new signals but still checking)');
          for (let i = 0; i < rows.length; i++) {
            const symbol = rows[i].symbol;
            try {
              await signalManager.checkAlignmentConfirmation(symbol);
            } catch (e) {
              logger.debug({ e, symbol }, 'scanOnce: alignment check error');
            }
            await sleep(50);
          }
        }
      }

      try {
        db.setState('lastScanAt', scanStart);
        db.setState('lastScanSignals', after.map(r => r.key));
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
        const tasks = page.map(r => this.scanSymbolRoots(r.symbol, { 
          notifyImmediately: false, 
          detected_ts: Date.now(),
          isBoundaryScan: false
        }));
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

  async scanSymbolRoots(symbol, { notifyImmediately = true, detected_ts = null, isBoundaryScan = false } = {}) {
    const tfList = config.ROOT_TFS || [];
    const results = [];
    for (const tf of tfList) {
      try {
        const db = dbModule.get();
        const selectStmt = db.prepare('SELECT open_time, close, open FROM klines WHERE symbol=? AND timeframe=? ORDER BY open_time DESC LIMIT 2');
        let rows = selectStmt.all(symbol, tf);
        if (!rows || rows.length < 2) {
          logger.debug({ symbol, tf }, 'scanSymbolRoots: insufficient klines, seeding now (will also seed MTF TFs)');
          await this.seedKlinesForSymbol(symbol, tf);

          rows = selectStmt.all(symbol, tf);
          if (!rows || rows.length < 2) {
            logger.debug({ symbol, tf }, 'scanSymbolRoots: still insufficient klines after seeding, skipping tf for now');
            continue;
          } else {
            logger.info({ symbol, tf }, 'scanSymbolRoots: klines seeded and available, re-checking flip');
          }
        }

        // Track last root candle open_time to prevent duplicate flip detections
        const stateKey = `${symbol}:${tf}`;
        const latestCandleTime = rows[0].open_time; // Most recent candle (DESC order)
        
        if (lastRootCandleState[stateKey] === latestCandleTime) {
          logger.debug({ symbol, tf, candleTime: latestCandleTime }, 'scanSymbolRoots: already processed this candle, skipping flip detection');
          continue;
        }
        
        lastRootCandleState[stateKey] = latestCandleTime;
        logger.debug({ symbol, tf, candleTime: latestCandleTime }, 'scanSymbolRoots: new candle detected, checking for flip');

        const flip = await require('./macd').isMacdFlip(symbol, tf);
        if (flip) {
          logger.info({ symbol, tf, isBoundaryScan }, 'scanSymbolRoots: FLIP DETECTED - handling signal');
          const sig = await signalManager.handleRootSignal({
            symbol,
            root_tf: tf,
            detected_at: detected_ts || Date.now(),
            notifyImmediately,
            isBoundaryScan
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
   * FIXED: scheduleAlignedTo5m
   * - Properly triggers scanOnce with notifyNewSignals=true at exactly 5-min boundaries
   * - Enhanced logging for debugging boundary execution
   * - Correctly handles new root candle notifications
   */
  scheduleAlignedTo5m() {
    const msToNext5 = () => {
      const d = new Date();
      const m = d.getUTCMinutes();
      const next = new Date(d);
      const deltaM = 5 - (m % 5);
      next.setUTCMinutes(m + deltaM);
      next.setUTCSeconds(0);
      next.setUTCMilliseconds(500);
      return next - d;
    };

    let firstBoundaryPassed = false;

    const schedule = async () => {
      const wait = msToNext5();
      const nextBoundaryTime = new Date(Date.now() + wait).toISOString();
      logger.info({ waitMs: wait, nextBoundaryTime }, 'scheduleAlignedTo5m: scheduled, waiting until next 5m boundary');
      
      setTimeout(async () => {
        try {
          const triggeredTime = new Date().toISOString();
          logger.info({ triggeredAt: triggeredTime }, '═══════════════════════════════════════════════════════════');
          logger.info({ triggeredAt: triggeredTime }, '🔔 5-MINUTE BOUNDARY TRIGGERED - RUNNING SCAN WITH NOTIFICATIONS');
          logger.info({ triggeredAt: triggeredTime }, '═══════════════════════════════════════════════════════════');
          
          // FIX #1: Run scan with notifyNewSignals=TRUE at 5-min boundary
          await this.scanOnce({ notifyNewSignals: true });
          
          logger.info('scheduleAlignedTo5m: scanOnce completed with notifications');

          const now = new Date();
          const minute = now.getUTCMinutes();
          const hour = now.getUTCHours();
          const newRootTfs = [];
          
          for (const tf of config.ROOT_TFS) {
            if (String(tf).toUpperCase() === 'D') {
              if (hour === 0 && minute === 0) newRootTfs.push('D');
            } else {
              const tfNum = Number(tf);
              if (!isNaN(tfNum)) {
                const minutesSinceEpoch = Math.floor(now.getTime() / 60000);
                if (minutesSinceEpoch % tfNum === 0) newRootTfs.push(String(tf));
              }
            }
          }

          if (newRootTfs.length > 0) {
            logger.info({ newRootTfs }, '📊 NEW ROOT CANDLES DETECTED - handling notifications');
            // FIX #2: Handle new root candle opening
            try {
              await signalManager.handleNewRootCandle(newRootTfs);
              logger.info({ newRootTfs }, '✅ Root candle notifications sent');
            } catch (e) {
              logger.warn({ e, newRootTfs }, '❌ handleNewRootCandle failed');
            }
          }

          // Close least profitable trade if enabled and max trades filled
          if (config.CLOSE_LEAST_PROFITABLE_ENABLED && newRootTfs.length > 0) {
            const openCount = dbModule.get().prepare('SELECT COUNT(*) as c FROM trades WHERE status = ?').get('open').c || 0;
            if (openCount >= config.MAX_OPEN_TRADES) {
              const minutesSinceHour = new Date().getUTCMinutes();
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

          if (!firstBoundaryPassed) {
            firstBoundaryPassed = true;
            try {
              signalManager.setOpenTradesAllowed(true);
              logger.info('✅ OPEN TRADES ENABLED after first boundary');
            } catch (e) {
              logger.warn({ e }, 'Failed to set open trades allowed');
            }
          }
        } catch (err) {
          logger.error({ err }, 'scheduleAlignedTo5m: boundary task failed');
        } finally {
          schedule();
        }
      }, wait);
    };

    schedule();
  }
};
