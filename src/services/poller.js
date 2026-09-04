// src/services/poller.js
/**
 * Poller / scheduler
 *
 * Responsibilities:
 * - initialScan (WS optional) -> discover symbols, persist
 * - backgroundSeedKlines -> seed klines for symbols (concurrent)
 * - scanOnce -> check roots across symbols and detect new signals at boundary
 * - scheduleAlignedTo5m -> runs scanOnce at 5m-aligned boundaries; detects new root-candle opens
 */

const dbModule = require('../db');
const bybit = require('./bybitRest');
const config = require('../config');
const logger = require('pino')();
const Bottleneck = require('bottleneck');
const macdUtil = require('./macd');
const signalManager = require('./signalManager');

const limiter = new Bottleneck({ minTime: 50 });

// Configurable concurrency for kline seeding (from config)
const SEED_CONCURRENCY = Number(config.SEED_CONCURRENCY || 6);

let isRunning = false;
let seedingComplete = false;

module.exports = {
  start() {
    if (isRunning) return;
    isRunning = true;

    logger.info('═══════════════════════════════════════════════');
    logger.info('🌱 Poller starting...');
    logger.info('═══════════════════════════════════════════════');

    // Prevent opening trades until we've passed the next aligned 5m boundary.
    try { signalManager.setOpenTradesAllowed(false); } catch (e) { /* ignore */ }

    // Run host probe in background (do not block startup)
    try {
      bybit.probeHosts(3000)
        .then((base) => {
          if (base) logger.info({ base }, '✅ probeHosts completed in background');
          else logger.warn('⚠️ probeHosts completed in background with no selected base');
        })
        .catch((e) => logger.debug({ e }, 'probeHosts background failure'));
    } catch (e) {
      logger.debug({ e }, 'probeHosts startup call failed');
    }

    // Run initialScan immediately (fast)
    (async () => {
      try {
        logger.info('═══════════════════════════════════════════════');
        logger.info('🌱 poller: starting initialScan');
        logger.info('═══════════════════════════════════════════════');
        await this.initialScan();
        logger.info('═══════════════════════════════════════════════');
        logger.info('✅ poller: initialScan completed - SEEDING NOW COMPLETE');
        logger.info('═══════════════════════════════════════════════');
        seedingComplete = true;
        logger.info({ seedingComplete }, '✅ poller: seedingComplete flag set to TRUE');
      } catch (err) {
        logger.error({ err }, '❌ poller: initialScan failed');
      }
    })();

    // Scheduling
    if (config.ROOT_MIDSCAN_INTERVAL && Number(config.ROOT_MIDSCAN_INTERVAL) > 0) {
      // Interval mode (seconds)
      setInterval(() => this.scanOnce(), Number(config.ROOT_MIDSCAN_INTERVAL) * 1000);
      // Also enable open trades at next aligned 5m boundary (non-blocking)
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
          logger.info('✅ Open trades enabled at next 5m boundary (interval mode)');
        } catch (e) { logger.debug({ e }, 'Failed to set open trades allowed'); }
      }, msToNext5());
    } else {
      // Align to 5m boundaries
      this.scheduleAlignedTo5m();
    }
  },

  /**
   * Check if seeding is complete
   */
  isSeedingComplete() {
    return seedingComplete;
  },

  /**
   * initialScan:
   * - If config.USE_WS true, attempts wsManager.performInitialScan() (if function exists) with timeout
   * - Falls back to bybit.fetchAllSymbols()
   * - persists discovered symbols to DB
   * - Kicks off background seeding for seedSymbols if SYMBOL_SEED_ALL true
   */
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
          logger.info({ count: allSymbols.length }, '✅ poller: WS initial scan provided symbols');
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

    // Persist all symbols to DB
    const db = dbModule.get();
    const insert = db.prepare('INSERT OR REPLACE INTO symbols (symbol, base, quote, fetched_at) VALUES (?, ?, ?, ?)');
    const now = Date.now();
    const insertMany = db.transaction((rows) => {
      for (const s of rows) {
        insert.run(s.symbol, s.base || s.symbol.replace(/USDT(\.P)?$/i, ''), s.quote || 'USDT', now);
      }
    });
    insertMany(allSymbols.filter(s => s && s.symbol));
    logger.info({ total: allSymbols.length }, '✅ poller.initialScan: symbols persisted to DB');

    // Background seed if configured
    const seedSymbols = bybit.getSeedSymbols(allSymbols);
    if (seedSymbols && seedSymbols.length) {
      logger.info({ seedCount: seedSymbols.length }, '🌱 poller.initialScan: starting background seeding');
      setImmediate(() => this.backgroundSeedKlines(seedSymbols));
    } else {
      logger.info('poller.initialScan: no seed symbols to process (SYMBOL_SEED_ALL disabled or none)');
    }
  },

  /**
   * performWsInitialScan - delegates to bybitWs manager if it exposes performInitialScan()
   */
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

  /**
   * backgroundSeedKlines(symbols)
   * - Seeds klines for given list with concurrency controlled by SEED_CONCURRENCY
   */
  async backgroundSeedKlines(symbols = []) {
    if (!Array.isArray(symbols) || symbols.length === 0) {
      logger.info('backgroundSeedKlines: nothing to seed');
      return;
    }
    logger.info({ count: symbols.length, concurrency: SEED_CONCURRENCY }, '🌱 backgroundSeedKlines: starting');

    let completedCount = 0;

    for (let i = 0; i < symbols.length; i += SEED_CONCURRENCY) {
      const batch = symbols.slice(i, i + SEED_CONCURRENCY);
      const jobs = batch.map(s => 
        limiter.schedule(() => this.seedKlinesForSymbol(s.symbol))
          .then(() => {
            completedCount++;
            if (completedCount % 10 === 0) {
              logger.debug({ completedCount, total: symbols.length }, '🌱 backgroundSeedKlines: batch progress');
            }
          })
      );
      try {
        await Promise.all(jobs);
      } catch (e) {
        logger.debug({ e }, 'backgroundSeedKlines: batch failed (continuing)');
      }
    }
    logger.info({ completed: completedCount, total: symbols.length }, '✅ backgroundSeedKlines: completed');
  },

  /**
   * seedKlinesForSymbol(symbol, timeframe)
   * - Fetches klines for ROOT_TFS (or provided timeframe), stores to DB, and triggers MACD compute
   */
  async seedKlinesForSymbol(symbol, timeframe = null) {
    const tfs = timeframe ? [timeframe] : (config.ROOT_TFS || []);
    for (const tf of tfs) {
      const interval = tf === 'D' ? 'D' : String(tf);
      try {
        logger.debug({ symbol, tf }, '🌱 Seeding klines for symbol/timeframe');
        
        const klines = await limiter.schedule(() => bybit.fetchKlines(symbol, interval, config.SEED_KLINES_LIMIT));
        if (!klines || klines.length === 0) {
          logger.debug({ symbol, tf }, '⚠️ No klines returned from fetch');
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
        logger.debug({ symbol, tf, klinesCount: klines.length }, '✅ Klines seeded and persisted');

        try {
          await macdUtil.computeAndStoreMacd(symbol, tf);
          logger.debug({ symbol, tf }, '✅ MACD computed successfully');
        } catch (err) {
          logger.debug({ err, symbol, tf }, 'seedKlinesForSymbol: macd compute failed (continuing)');
        }
      } catch (err) {
        logger.debug({ err, symbol, tf }, 'seedKlinesForSymbol: fetch failed (skipping tf)');
      }
    }
  },

  /**
   * scanOnce:
   * - Runs over all symbols (paged by PAGE_SIZE)
   * - For each symbol, calls scanSymbolRoots(symbol, { notifyImmediately: false })
   * - After scanning, compares latest snapshot to previous snapshot (db.getLatestSignalsSnapshot)
   * - For any newly-detected signals at or after scan start, sends a single telegram block per signal
   */
  async scanOnce() {
    try {
      const db = dbModule;
      const prev = db.getLatestSignalsSnapshot();
      const prevKeys = new Set(prev.map(r => r.key));

      const scanStart = Date.now();

      const rows = db.get().prepare('SELECT symbol FROM symbols ORDER BY symbol COLLATE NOCASE ASC').all();
      logger.info({ symbolCount: rows.length }, '🔍 scanOnce: scanning symbols');
      
      for (let i = 0; i < rows.length; i += config.PAGE_SIZE) {
        const page = rows.slice(i, i + config.PAGE_SIZE);
        const tasks = page.map(r => this.scanSymbolRoots(r.symbol, { notifyImmediately: false, detected_ts: scanStart }));
        try {
          await Promise.all(tasks);
        } catch (e) {
          logger.debug({ e }, 'scanOnce: page tasks error (continuing)');
        }
      }

      const after = db.getLatestSignalsSnapshot();
      const newSignals = after.filter(r => !prevKeys.has(r.key) && r.detected_at >= scanStart);

      if (newSignals.length > 0) {
        logger.info({ newSignals: newSignals.length }, '📢 scanOnce: new signals detected, notifying');
        const telegram = require('./telegram');
        for (const s of newSignals) {
          try {
            await telegram.sendNewSignalSingleBlock(s);
          } catch (e) {
            logger.debug({ e, s }, 'scanOnce: failed to send new-signal message');
          }
        }
      } else {
        logger.debug('scanOnce: no new signals found this boundary');
      }

      // Persist last scan metadata
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

  /**
   * scanSymbolRoots(symbol, { notifyImmediately = true })
   * - For each ROOT_TF checks last 2 klines and calls macd.isMacdFlip()
   * - If flip detected, calls signalManager.handleRootSignal({ notifyImmediately })
   */
  async scanSymbolRoots(symbol, { notifyImmediately = true, detected_ts = null } = {}) {
    const tfList = config.ROOT_TFS || [];
    const results = [];
    for (const tf of tfList) {
      try {
        const db = dbModule.get();
        const rows = db.prepare('SELECT open_time, close, open FROM klines WHERE symbol=? AND timeframe=? ORDER BY open_time DESC LIMIT 2').all(symbol, tf);
        if (!rows || rows.length < 2) {
          // seed quickly if missing
          await this.seedKlinesForSymbol(symbol, tf);
          continue;
        }
        const flip = await require('./macd').isMacdFlip(symbol, tf);
        if (flip) {
          logger.info({ symbol, tf }, '🚨 MACD flip detected!');
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
   * scheduleAlignedTo5m:
   * - Schedules scanOnce at the next 5m boundary and then every 5m
   * - After running scanOnce, determines which root TFs have a new root candle and calls signalManager.handleNewRootCandle
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
      logger.info({ wait }, '⏰ scheduleAlignedTo5m: waiting ms until next 5m boundary');
      setTimeout(async () => {
        try {
          // Run scan
          await this.scanOnce();

          // Determine new root TFs that open at this exact boundary
          const now = new Date();
          const minute = now.getUTCMinutes();
          const hour = now.getUTCHours();
          const newRootTfs = [];
          for (const tf of config.ROOT_TFS) {
            if (String(tf).toUpperCase() === 'D') {
              // daily opens at 00:00 UTC
              if (hour === 0 && minute === 0) newRootTfs.push('D');
            } else {
              const tfNum = Number(tf);
              if (!isNaN(tfNum)) {
                // if current time in minutes since epoch is divisible by tfNum
                const minutesSinceEpoch = Math.floor(now.getTime() / 60000);
                if (minutesSinceEpoch % tfNum === 0) newRootTfs.push(String(tf));
              }
            }
          }

          if (newRootTfs.length && config.NEW_ROOT_CANDLE_NOTIFY) {
            try {
              await signalManager.handleNewRootCandle(newRootTfs);
            } catch (e) {
              logger.debug({ e, newRootTfs }, 'scheduleAlignedTo5m: handleNewRootCandle failed');
            }
          }

          if (!firstBoundaryPassed) {
            firstBoundaryPassed = true;
            try {
              signalManager.setOpenTradesAllowed(true);
              logger.info('✅ scheduleAlignedTo5m: open trades enabled after first boundary');
            } catch (e) {
              logger.debug({ e }, 'scheduleAlignedTo5m: failed to set open trades allowed');
            }
          }
        } catch (err) {
          logger.error({ err }, 'scheduleAlignedTo5m: boundary task failed');
        } finally {
          // schedule next
          schedule();
        }
      }, wait);
    };

    schedule();
  }
};
