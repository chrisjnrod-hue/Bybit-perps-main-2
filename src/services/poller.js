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

    // ========== CONFIG VALIDATION (Bug #10) ==========
    try {
      if (!Array.isArray(config.ROOT_TFS) || config.ROOT_TFS.length === 0) {
        logger.error('ROOT_TFS not configured; poller cannot start');
        return;
      }
      if (!Array.isArray(config.MTF_TFS) || config.MTF_TFS.length === 0) {
        logger.warn('MTF_TFS not configured; MTF alignment alerts disabled');
      }
      if (!config.PAGE_SIZE || config.PAGE_SIZE <= 0) {
        config.PAGE_SIZE = 50;
        logger.warn('PAGE_SIZE not configured, defaulting to 50');
      }
    } catch (e) {
      logger.error({ e }, 'Config validation failed during startup');
      return;
    }

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

    // LOOP 1: Initial Deploy (startup summary with all signals)
    (async () => {
      try {
        logger.info('poller: Loop 1 starting (Initial Deploy)');
        await this.initialScan();
        logger.info('poller: Loop 1 initialScan completed');

        // Full silent scan to populate DB
        try {
          await this.scanAllForStartup();
          logger.info('poller: Loop 1 full startup scan completed');
        } catch (err) {
          logger.error({ err }, 'poller: Loop 1 scanAllForStartup error');
        }

        // Send startup summary with all detected signals
        try {
          await signalManager.sendStartupSummary();
          logger.info('poller: Loop 1 startup summary sent');
        } catch (err) {
          logger.error({ err }, 'poller: Loop 1 failed to send startup summary');
        }
      } catch (err) {
        logger.error({ err }, 'poller: Loop 1 (Initial Deploy) failed');
      }
    })();

    // LOOP 2: 5m Boundary Scan (continuous monitoring, one signal per new detection)
    this.scheduleAlignedTo5m();

    // Schedule first open trades enable at next 5m boundary
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
        logger.info('Open trades enabled at next 5m boundary');
      } catch (e) { logger.debug({ e }, 'Failed to set open trades allowed'); }
    }, msToNext5());
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
      logger.info('poller.initialScan: no seed symbols to process');
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
   * LOOP 2: 5m Boundary Scan
   * Runs every 5 minutes aligned to UTC boundaries.
   * - Scans all symbols for new root signal detections (sends one block per new signal)
   * - Also checks monitored signals for MTF alignment changes (sends alerts)
   * - No summary or recommended blocks
   */
  async scan5mBoundary() {
    try {
      const db = dbModule;
      const scanStart = Date.now();

      // Get snapshot BEFORE scan (Bug #2 fix: null check)
      let prevSnapshot = db.getLatestSignalsSnapshot();
      if (!Array.isArray(prevSnapshot)) {
        logger.warn('scan5mBoundary: prevSnapshot is not an array, initializing empty');
        prevSnapshot = [];
      }
      const prevKeys = new Set(prevSnapshot.map(r => r.key));

      logger.debug({ prevSnapshotCount: prevSnapshot.length }, 'scan5mBoundary: starting symbol scan');

      // Scan all symbols for new root signals
      let rows = [];
      try {
        rows = db.get().prepare('SELECT symbol FROM symbols ORDER BY symbol COLLATE NOCASE ASC').all() || [];
      } catch (err) {
        logger.error({ err }, 'scan5mBoundary: failed to fetch symbols from DB');
        return;
      }

      for (let i = 0; i < rows.length; i += config.PAGE_SIZE) {
        const page = rows.slice(i, i + config.PAGE_SIZE);
        const tasks = page.map(r => this.scanSymbolRoots(r.symbol, { notifyImmediately: false, detected_ts: scanStart }));
        try {
          await Promise.all(tasks);
        } catch (e) {
          logger.debug({ e, pageIndex: i }, 'scan5mBoundary: page tasks error (continuing)');
        }
      }

      // Get snapshot AFTER scan (Bug #3 fix: proper variable assignment)
      let afterSnapshot = db.getLatestSignalsSnapshot();
      if (!Array.isArray(afterSnapshot)) {
        logger.warn('scan5mBoundary: afterSnapshot is not an array, initializing empty');
        afterSnapshot = [];
      }

      // Detect new signals (Bug #6 fix: timestamp check prevents duplicates)
      const newSignals = afterSnapshot.filter(r => !prevKeys.has(r.key) && r.detected_at >= scanStart);

      // Send one telegram block per new signal (no summary or recommended)
      if (newSignals.length > 0) {
        logger.info({ newSignals: newSignals.length }, 'scan5mBoundary: new signals detected');
        const telegram = require('./telegram');
        for (let i = 0; i < newSignals.length; i++) {
          const s = newSignals[i];
          try {
            await telegram.sendNewSignalSingleBlock(s);
          } catch (e) {
            logger.debug({ e, symbol: s?.symbol }, 'scan5mBoundary: failed to send new-signal message');
          }
          await sleep(config.TELEGRAM_SEND_DELAY_MS || 100);
        }
      } else {
        logger.debug('scan5mBoundary: no new signals found');
      }

      // Check monitored signals for MTF alignment changes (Bug #3 fix: use afterSnapshot)
      const monitoredSignals = afterSnapshot.filter(s => s && s.meta && s.meta.decision === 'monitor');
      if (monitoredSignals.length > 0) {
        logger.debug({ count: monitoredSignals.length }, 'scan5mBoundary: checking MTF alignment for monitored signals');
        const telegram = require('./telegram');
        
        for (let i = 0; i < monitoredSignals.length; i++) {
          const s = monitoredSignals[i];
          try {
            // Re-evaluate MTF alignment (Bug #4 fix: full error handling)
            let currentAlignment = null;
            try {
              currentAlignment = await signalManager.evaluateMtfAlignment(s.symbol);
            } catch (alignErr) {
              logger.debug({ err: alignErr, symbol: s.symbol }, 'scan5mBoundary: MTF evaluation failed for symbol');
              continue; // Skip this signal and move to next
            }

            const prevAlignment = s.meta?.alignment || {};

            // Detect if alignment has changed (Bug #1 fix: corrected logic)
            const changed = this.hasAlignmentChanged(prevAlignment, currentAlignment);
            if (changed) {
              logger.info({ symbol: s.symbol, root_tf: s.root_tf }, 'MTF alignment changed for monitored signal');
              await telegram.sendMtfAlignmentAlert({
                symbol: s.symbol,
                root_tf: s.root_tf,
                prevAlignment,
                currentAlignment
              });
            }
          } catch (e) {
            logger.debug({ e, symbol: s?.symbol }, 'scan5mBoundary: MTF check unexpected error');
          }
          await sleep(config.TELEGRAM_SEND_DELAY_MS || 100);
        }
      }

      // Persist scan state
      try {
        db.setState('lastScanAt', scanStart);
        db.setState('lastScanKeys', Array.from(afterSnapshot.map(r => r.key)));
      } catch (e) {
        logger.debug({ e }, 'scan5mBoundary: failed to persist scan state');
      }

      logger.debug({ duration: Date.now() - scanStart }, 'scan5mBoundary: completed');
    } catch (err) {
      logger.error({ err }, 'scan5mBoundary: unexpected error');
    }
  },

  /**
   * Bug #1 Fix: Proper MTF alignment change detection
   * Detects when any TF's positive/negative or rising/falling status changes
   * NOT just when TF count changes
   */
  hasAlignmentChanged(prevAlignment, currentAlignment) {
    if (!prevAlignment || !currentAlignment) return false;

    const allTfs = new Set([
      ...Object.keys(prevAlignment || {}),
      ...Object.keys(currentAlignment || {})
    ]);

    if (allTfs.size === 0) return false;

    for (const tf of allTfs) {
      const p = prevAlignment[tf];
      const c = currentAlignment[tf];

      // Detect actual changes
      if (!p && c) {
        // New TF data appeared
        if (c.positive !== undefined) return true;
      }
      if (p && !c) {
        // TF data disappeared (unlikely but handle it)
        if (p.positive !== undefined) return true;
      }
      if (p && c) {
        // Both have data: check for flips
        if (p.positive !== c.positive) {
          logger.debug({ tf, prev: p.positive, current: c.positive }, 'Alignment change detected: positive/negative flip');
          return true;
        }
        if (p.rising !== c.rising) {
          logger.debug({ tf, prev: p.rising, current: c.rising }, 'Alignment change detected: rising/falling flip');
          return true;
        }
      }
    }

    return false;
  },

  async scanAllForStartup() {
    try {
      logger.info('scanAllForStartup: starting full startup pass (silent)');
      const db = dbModule.get();
      const rows = db.prepare('SELECT symbol FROM symbols ORDER BY symbol COLLATE NOCASE ASC').all() || [];
      
      for (let i = 0; i < rows.length; i += config.PAGE_SIZE) {
        const page = rows.slice(i, i + config.PAGE_SIZE);
        const tasks = page.map(r => this.scanSymbolRoots(r.symbol, { notifyImmediately: false, detected_ts: Date.now() }));
        try {
          await Promise.all(tasks);
        } catch (e) {
          logger.debug({ e, pageIndex: i }, 'scanAllForStartup: page tasks error (continuing)');
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
        let rows = selectStmt.all(symbol, tf) || [];
        
        if (!Array.isArray(rows) || rows.length < 2) {
          logger.debug({ symbol, tf, rowCount: rows.length }, 'scanSymbolRoots: insufficient klines, seeding now');
          await this.seedKlinesForSymbol(symbol, tf);

          rows = selectStmt.all(symbol, tf) || [];
          if (!Array.isArray(rows) || rows.length < 2) {
            logger.debug({ symbol, tf }, 'scanSymbolRoots: still insufficient klines after seeding, skipping tf');
            continue;
          } else {
            logger.info({ symbol, tf }, 'scanSymbolRoots: klines seeded and available');
          }
        }

        // Track last root candle to prevent duplicate flip detections
        const stateKey = `${symbol}:${tf}`;
        const latestCandleTime = rows[rows.length - 1].open_time;

        if (lastRootCandleState[stateKey] === latestCandleTime) {
          logger.debug({ symbol, tf, candleTime: latestCandleTime }, 'scanSymbolRoots: already processed this candle');
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
   * LOOP 2 Scheduler: Aligned to 5m UTC boundaries
   * Runs scan5mBoundary() every 5 minutes
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

    const schedule = async () => {
      const wait = msToNext5();
      logger.info({ waitMs: wait }, 'scheduleAlignedTo5m: waiting until next 5m boundary');
      setTimeout(async () => {
        try {
          // Run 5m boundary scan
          await this.scan5mBoundary();

          const now = new Date();
          const minute = now.getUTCMinutes();
          const hour = now.getUTCHours();
          const newRootTfs = [];

          // Detect which root TFs have new candles
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

          // LOOP 3: New Root Candle Open (send updated summary for affected TFs)
          if (newRootTfs.length > 0 && config.NEW_ROOT_CANDLE_NOTIFY) {
            logger.info({ newRootTfs }, 'New root candle(s) opened, triggering Loop 3');
            try {
              await signalManager.handleNewRootCandle(newRootTfs);
            } catch (e) {
              logger.debug({ e, newRootTfs }, 'scheduleAlignedTo5m: handleNewRootCandle failed');
            }
          }

          // Close least profitable trade if enabled and max trades filled
          if (config.CLOSE_LEAST_PROFITABLE_ENABLED && newRootTfs.length > 0) {
            try {
              const openCount = dbModule.get().prepare('SELECT COUNT(*) as c FROM trades WHERE status = ?').get('open')?.c || 0;
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
            } catch (err) {
              logger.debug({ err }, 'Failed to check open trades count');
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
