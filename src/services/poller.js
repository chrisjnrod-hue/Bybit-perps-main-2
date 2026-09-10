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
        
        // LOOP 1: Full silent startup scan
        try {
          await this.scanAllForStartup();
          logger.info('poller: LOOP 1 - startup full scan completed');
          
          // After initial scan, send startup summary with all signals
          try {
            await signalManager.sendStartupSummary();
            logger.info('poller: LOOP 1 - startup summary sent via telegram');
          } catch (err) {
            logger.error({ err }, 'poller: LOOP 1 - startup summary failed');
          }
        } catch (err) {
          logger.error({ err }, 'poller: LOOP 1 - scanAllForStartup error');
        }
      } catch (err) {
        logger.error({ err }, 'poller: initialScan failed');
      }
    })();

    if (config.ROOT_MIDSCAN_INTERVAL && Number(config.ROOT_MIDSCAN_INTERVAL) > 0) {
      setInterval(() => this.scanOnce(), Number(config.ROOT_MIDSCAN_INTERVAL) * 1000);

      const msToNext5 = () => {
        const now = new Date();
        const currentMs = now.getTime();
        const m = now.getUTCMinutes();
        const nextMinuteBoundary = Math.floor(m / 5) * 5 + 5;
        
        const nextBoundary = new Date(now);
        if (nextMinuteBoundary >= 60) {
          nextBoundary.setUTCHours(nextBoundary.getUTCHours() + 1);
          nextBoundary.setUTCMinutes(0);
        } else {
          nextBoundary.setUTCMinutes(nextMinuteBoundary);
        }
        nextBoundary.setUTCSeconds(0);
        nextBoundary.setUTCMilliseconds(0);
        return nextBoundary.getTime() - currentMs;
      };
      setTimeout(() => {
        try {
          signalManager.setOpenTradesAllowed(true);
          logger.info('Open trades enabled at next 5m boundary (interval mode)');
        } catch (e) { logger.debug({ e }, 'Failed to set open trades allowed'); }
      }, msToNext5());
    } else {
      // LOOP 2 & 3: Scheduled at exact 5-min boundaries
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
   * LOOP 2: Scan at 5-min boundary
   * - Detects only NEW signals (not in previous scan)
   * - Sends individual signal blocks per new signal (NOT full summary)
   * - Checks MTF alignment confirmations
   * - Sends telegram EXACTLY at 5-min boundary time
   */
  async scanOnce({ notifyNewSignals = true } = {}) {
    try {
      logger.info('LOOP 2 - scanOnce: 5-min boundary scan starting');
      const db = dbModule;
      const prev = db.getLatestSignalsSnapshot();
      const prevKeys = new Set(prev.map(r => r.key));

      const scanStart = Date.now();

      const rows = db.get().prepare('SELECT symbol FROM symbols ORDER BY symbol COLLATE NOCASE ASC').all();
      for (let i = 0; i < rows.length; i += config.PAGE_SIZE) {
        const page = rows.slice(i, i + config.PAGE_SIZE);
        const tasks = page.map(r => this.scanSymbolRoots(r.symbol, { notifyImmediately: false, detected_ts: scanStart }));
        try {
          await Promise.all(tasks);
        } catch (e) {
          logger.debug({ e }, 'LOOP 2 - scanOnce: page tasks error (continuing)');
        }
      }

      const after = db.getLatestSignalsSnapshot();
      
      // Get ONLY NEW signals detected in THIS scan boundary
      const newSignals = after.filter(r => !prevKeys.has(r.key) && r.detected_at >= scanStart);

      if (newSignals.length > 0) {
        logger.info({ newSignals: newSignals.length, notifyNewSignals }, 'LOOP 2 - scanOnce: found new signals this boundary');

        if (notifyNewSignals) {
          const telegram = require('./telegram');
          
          // Send individual signal blocks for each NEW signal ONLY (no summary)
          for (let i = 0; i < newSignals.length; i++) {
            const s = newSignals[i];
            try {
              await telegram.sendNewSignalSingleBlock(s);
              logger.info({ symbol: s.symbol, root_tf: s.root_tf }, 'LOOP 2 - Sent individual signal block for new signal');
            } catch (e) {
              logger.debug({ e, s }, 'LOOP 2 - failed to send new-signal message');
            }
            await sleep(config.TELEGRAM_SEND_DELAY_MS || 100);
          }

          // Check MTF alignment confirmations for each symbol
          for (let i = 0; i < rows.length; i++) {
            const symbol = rows[i].symbol;
            try {
              await signalManager.checkAlignmentConfirmation(symbol);
            } catch (e) {
              logger.debug({ e, symbol }, 'LOOP 2 - alignment check error');
            }
            await sleep(50);
          }
        } else {
          logger.info('LOOP 2 - notifications suppressed (silent mode)');
        }
      } else {
        logger.info('LOOP 2 - scanOnce: no new signals found this boundary');
        
        // Still check MTF confirmations even if no new signals
        if (notifyNewSignals) {
          for (let i = 0; i < rows.length; i++) {
            const symbol = rows[i].symbol;
            try {
              await signalManager.checkAlignmentConfirmation(symbol);
            } catch (e) {
              logger.debug({ e, symbol }, 'LOOP 2 - alignment check error');
            }
            await sleep(50);
          }
        }
      }

      try {
        db.setState('lastScanAt', scanStart);
        db.setState('lastScanSignals', after.map(r => r.key));
      } catch (e) {
        logger.debug({ e }, 'LOOP 2 - failed to persist scan state');
      }

      logger.info('LOOP 2 - scanOnce: completed at 5-min boundary');
    } catch (err) {
      logger.error({ err }, 'LOOP 2 - scanOnce: unexpected error');
    }
  },

  /**
   * LOOP 1: Full startup scan (silent, no notifications)
   */
  async scanAllForStartup() {
    try {
      logger.info('LOOP 1 - scanAllForStartup: silent startup full scan starting');
      const db = dbModule.get();
      const rows = db.prepare('SELECT symbol FROM symbols ORDER BY symbol COLLATE NOCASE ASC').all();
      for (let i = 0; i < rows.length; i += config.PAGE_SIZE) {
        const page = rows.slice(i, i + config.PAGE_SIZE);
        const tasks = page.map(r => this.scanSymbolRoots(r.symbol, { notifyImmediately: false, detected_ts: Date.now() }));
        try {
          await Promise.all(tasks);
        } catch (e) {
          logger.debug({ e }, 'LOOP 1 - page tasks error (continuing)');
        }
      }
      logger.info('LOOP 1 - scanAllForStartup: completed full startup pass (ready for telegram summary)');
    } catch (err) {
      logger.error({ err }, 'LOOP 1 - scanAllForStartup: unexpected error');
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
            logger.debug({ symbol, tf }, 'scanSymbolRoots: still insufficient klines after seeding, skipping');
            continue;
          } else {
            logger.info({ symbol, tf }, 'scanSymbolRoots: klines seeded, re-checking flip');
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

  scheduleAlignedTo5m() {
    /**
     * Calculate milliseconds until EXACTLY the next 5-minute boundary at .000 ms
     * Boundaries: 00:00:00.000, 00:05:00.000, 00:10:00.000, etc.
     */
    const msToNext5 = () => {
      const now = new Date();
      const currentMs = now.getTime();
      
      const m = now.getUTCMinutes();
      const nextMinuteBoundary = Math.floor(m / 5) * 5 + 5;
      
      const nextBoundary = new Date(now);
      if (nextMinuteBoundary >= 60) {
        nextBoundary.setUTCHours(nextBoundary.getUTCHours() + 1);
        nextBoundary.setUTCMinutes(0);
      } else {
        nextBoundary.setUTCMinutes(nextMinuteBoundary);
      }
      nextBoundary.setUTCSeconds(0);
      nextBoundary.setUTCMilliseconds(0); // EXACT boundary: HH:MM:00.000
      
      return nextBoundary.getTime() - currentMs;
    };

    let firstBoundaryPassed = false;

    const schedule = async () => {
      const wait = msToNext5();
      const nextBoundaryTime = new Date(Date.now() + wait).toISOString();
      logger.info({ wait, nextBoundaryTime }, 'scheduleAlignedTo5m: waiting until next 5m boundary');
      
      setTimeout(async () => {
        const boundaryTimeNow = new Date().toISOString();
        logger.info({ boundaryTimeNow }, '=== 5-MIN BOUNDARY REACHED ===');

        try {
          // LOOP 2: Run scan at 5-min boundary with notifications enabled (only new signals, no summary)
          await this.scanOnce({ notifyNewSignals: true });

          // Determine new root TFs that just opened
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

          // LOOP 3: Notify about new root candles with full summary format if any opened
          if (newRootTfs.length && config.NEW_ROOT_CANDLE_NOTIFY) {
            try {
              logger.info({ newRootTfs }, 'LOOP 3 - Sending new root candle summary notification');
              await signalManager.handleNewRootCandle(newRootTfs);
            } catch (e) {
              logger.debug({ e, newRootTfs }, 'LOOP 3 - handleNewRootCandle failed');
            }
          }

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
          schedule();
        }
      }, wait);
    };

    schedule();
  }
};
