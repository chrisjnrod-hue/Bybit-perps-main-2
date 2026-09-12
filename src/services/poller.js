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
        logger.info('poller: LOOP 1 - Initial/Deploy scan starting');
        await this.initialScan();
        logger.info('poller: initialScan completed');
        
        // Full scan on startup (silent, no notifications)
        try {
          await this.scanAllForStartup();
          logger.info('poller: LOOP 1 startup full scan completed');
        } catch (err) {
          logger.error({ err }, 'poller: LOOP 1 scanAllForStartup error');
        }

        // Send startup summary via telegram (includes header + per-signal blocks + recommended)
        try {
          const telegram = require('./telegram');
          const snapshot = dbModule.getLatestSignalsSnapshot();
          await telegram.sendStartupSummary({ snapshot });
          logger.info('poller: LOOP 1 Startup summary telegram sent');
        } catch (err) {
          logger.error({ err }, 'poller: LOOP 1 Failed to send startup summary');
        }

      } catch (err) {
        logger.error({ err }, 'poller: LOOP 1 initialScan failed');
      }
    })();

    // LOOP 2: 5m Boundary Scan
    this.schedule5mBoundaryScan();

    // LOOP 3: New Root Candle Open Scan
    this.scheduleNewRootCandleScan();
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

  async scanAllForStartup() {
    try {
      logger.info('LOOP 1: scanAllForStartup - starting full startup scan');
      const db = dbModule.get();
      const rows = db.prepare('SELECT symbol FROM symbols ORDER BY symbol COLLATE NOCASE ASC').all();
      let totalScanned = 0;
      
      for (let i = 0; i < rows.length; i += config.PAGE_SIZE) {
        const page = rows.slice(i, i + config.PAGE_SIZE);
        const tasks = page.map(r => this.fullComprehensiveScan(r.symbol, { detected_ts: Date.now() }));
        try {
          const results = await Promise.all(tasks);
          const signalsFound = results.reduce((sum, arr) => sum + (Array.isArray(arr) ? arr.length : 0), 0);
          totalScanned += page.length;
          logger.debug({ pageSize: page.length, signalsFound }, 'LOOP 1: Page scanned');
        } catch (e) {
          logger.debug({ e }, 'LOOP 1: page tasks error (continuing)');
        }
      }
      logger.info({ totalScanned }, 'LOOP 1: scanAllForStartup completed');
    } catch (err) {
      logger.error({ err }, 'LOOP 1: scanAllForStartup unexpected error');
    }
  },

  /**
   * fullComprehensiveScan:
   * Complete scan process: seed klines → compute MACD → detect flips
   * Used by LOOP 1, LOOP 2, and LOOP 3
   * Returns array of signal objects
   */
  async fullComprehensiveScan(symbol, { detected_ts = null } = {}) {
    const tfList = config.ROOT_TFS || [];
    const results = [];
    
    for (const tf of tfList) {
      try {
        logger.debug({ symbol, tf }, 'fullComprehensiveScan: STEP 1 - Seeding klines');
        
        // STEP 1: Seed klines for this symbol+tf
        await this.seedKlinesForSymbol(symbol, tf);
        
        // STEP 2: Verify klines exist in DB
        const db = dbModule.get();
        const klineCheck = db.prepare('SELECT COUNT(*) as cnt FROM klines WHERE symbol=? AND timeframe=?').get(symbol, tf);
        const klineCount = klineCheck ? klineCheck.cnt : 0;
        
        if (klineCount === 0) {
          logger.debug({ symbol, tf }, 'fullComprehensiveScan: STEP 2 - No klines in DB after seeding, skipping');
          continue;
        }
        logger.debug({ symbol, tf, klineCount }, 'fullComprehensiveScan: STEP 2 - Klines verified');
        
        // STEP 3: Compute MACD for this symbol+tf
        logger.debug({ symbol, tf }, 'fullComprehensiveScan: STEP 3 - Computing MACD');
        try {
          if (typeof macdUtil.computeAndStoreMacd === 'function') {
            await macdUtil.computeAndStoreMacd(symbol, tf);
            logger.debug({ symbol, tf }, 'fullComprehensiveScan: STEP 3 - MACD computed via computeAndStoreMacd');
          } else if (typeof macdUtil.computeMacdHistogram === 'function') {
            await macdUtil.computeMacdHistogram(symbol, tf);
            logger.debug({ symbol, tf }, 'fullComprehensiveScan: STEP 3 - MACD computed via computeMacdHistogram');
          }
        } catch (err) {
          logger.debug({ err, symbol, tf }, 'fullComprehensiveScan: STEP 3 - MACD computation failed, skipping flip check');
          continue;
        }
        
        // STEP 4: Check for MACD flip
        logger.debug({ symbol, tf }, 'fullComprehensiveScan: STEP 4 - Checking for MACD flip');
        try {
          const flip = await require('./macd').isMacdFlip(symbol, tf);
          if (flip) {
            logger.info({ symbol, tf }, 'fullComprehensiveScan: STEP 4 - MACD FLIP DETECTED');
            const sig = await signalManager.handleRootSignal({
              symbol,
              root_tf: tf,
              detected_at: detected_ts || Date.now(),
              notifyImmediately: false
            });
            if (sig) {
              logger.info({ symbol, tf }, 'fullComprehensiveScan: Signal persisted to DB');
              results.push(sig);
            }
          } else {
            logger.debug({ symbol, tf }, 'fullComprehensiveScan: STEP 4 - No flip detected');
          }
        } catch (err) {
          logger.debug({ err, symbol, tf }, 'fullComprehensiveScan: STEP 4 - Flip check error');
        }
      } catch (err) {
        logger.debug({ err, symbol, tf }, 'fullComprehensiveScan: Outer error processing symbol+tf');
      }
    }
    return results;
  },

  /**
   * LOOP 2: 5m Boundary Scan
   */
  schedule5mBoundaryScan() {
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
    let previousSnapshot = new Set();

    const schedule = async () => {
      const wait = msToNext5();
      logger.info({ 
        wait, 
        nextBoundary: new Date(Date.now() + wait).toISOString()
      }, 'LOOP 2: Waiting ms until next 5m boundary');
      
      setTimeout(async () => {
        try {
          logger.info('LOOP 2: Starting 5m boundary scan');
          await this.scan5mBoundary(previousSnapshot);
          
          // Update snapshot
          const current = dbModule.getLatestSignalsSnapshot();
          previousSnapshot = new Set(current.map(s => s.key));
          logger.info({ snapshotSize: previousSnapshot.size }, 'LOOP 2: Snapshot updated');

          if (config.CLOSE_LEAST_PROFITABLE_ENABLED) {
            try {
              const openCount = dbModule.get().prepare('SELECT COUNT(*) as c FROM trades WHERE status = ?').get('open').c || 0;
              if (openCount >= config.MAX_OPEN_TRADES) {
                const minutesSinceHour = new Date().getUTCMinutes();
                const closeMins = config.CLOSE_LEAST_PROFITABLE_MINS_BEFORE_BOUNDARY || 5;
                if (minutesSinceHour >= (60 - closeMins)) {
                  logger.info({ openCount }, 'LOOP 2: Closing least profitable trade');
                  await tradeManager.closeLeastProfitableTrade();
                }
              }
            } catch (err) {
              logger.error({ err }, 'LOOP 2: Error managing trades');
            }
          }

          if (!firstBoundaryPassed) {
            firstBoundaryPassed = true;
            try {
              signalManager.setOpenTradesAllowed(true);
              logger.info('LOOP 2: Open trades enabled');
            } catch (e) {
              logger.debug({ e }, 'LOOP 2: Failed to enable open trades');
            }
          }
        } catch (err) {
          logger.error({ err }, 'LOOP 2: Boundary task failed');
        } finally {
          schedule();
        }
      }, wait);
    };

    schedule();
  },

  /**
   * scan5mBoundary: Full comprehensive scan with cross-check
   */
  async scan5mBoundary(previousSnapshot = new Set()) {
    try {
      const scanStart = Date.now();
      logger.info({ 
        scanTime: new Date(scanStart).toISOString(),
        previousSignals: previousSnapshot.size
      }, 'LOOP 2: scan5mBoundary starting comprehensive scan');

      // FULL COMPREHENSIVE SCAN
      const db = dbModule.get();
      const rows = db.prepare('SELECT symbol FROM symbols ORDER BY symbol COLLATE NOCASE ASC').all();
      let totalSymbolsProcessed = 0;
      
      for (let i = 0; i < rows.length; i += config.PAGE_SIZE) {
        const page = rows.slice(i, i + config.PAGE_SIZE);
        const tasks = page.map(r => this.fullComprehensiveScan(r.symbol, { detected_ts: scanStart }));
        try {
          const results = await Promise.all(tasks);
          totalSymbolsProcessed += page.length;
        } catch (e) {
          logger.debug({ e }, 'LOOP 2: Page error (continuing)');
        }
      }

      logger.info({ totalSymbolsProcessed }, 'LOOP 2: Comprehensive scan completed, waiting for DB persistence');
      
      // Wait for DB to persist all signals
      await sleep(200);

      // Get current snapshot and cross-check
      const currentSnapshot = dbModule.getLatestSignalsSnapshot();
      const newSignals = currentSnapshot.filter(s => 
        !previousSnapshot.has(s.key) && s.detected_at >= scanStart
      );

      logger.info({ 
        total: currentSnapshot.length,
        previous: previousSnapshot.size,
        new: newSignals.length
      }, 'LOOP 2: Cross-check complete');

      // Send telegram for new signals only
      if (newSignals.length > 0) {
        logger.info({ count: newSignals.length }, 'LOOP 2: SENDING TELEGRAM BLOCKS FOR NEW SIGNALS');
        const telegram = require('./telegram');

        for (let i = 0; i < newSignals.length; i++) {
          const s = newSignals[i];
          try {
            logger.info({ symbol: s.symbol, tf: s.root_tf }, 'LOOP 2: Sending signal block');
            await telegram.sendNewSignalSingleBlock(s);
          } catch (e) {
            logger.error({ err: e, symbol: s.symbol }, 'LOOP 2: Failed to send block');
          }
          await sleep(config.TELEGRAM_SEND_DELAY_MS || 100);
        }
      } else {
        logger.info('LOOP 2: No new signals detected');
      }

    } catch (err) {
      logger.error({ err }, 'LOOP 2: scan5mBoundary error');
    }
  },

  /**
   * LOOP 3: New Root Candle Open Scan
   */
  scheduleNewRootCandleScan() {
    const schedule = async () => {
      try {
        const nextEvent = this.calculateNextCandleEvent();
        const waitMs = Math.max(nextEvent.waitMs, 100);
        
        logger.info({ 
          nextTf: nextEvent.tf, 
          nextOpenTime: new Date(nextEvent.nextOpenTime).toISOString(),
          waitMs
        }, 'LOOP 3: Scheduled next candle');
        
        await sleep(waitMs);
        
        const detectedTfs = this.detectCurrentCandleOpens();
        if (detectedTfs && detectedTfs.length > 0) {
          logger.info({ detectedTfs }, 'LOOP 3: Candle(s) detected as open');
          await this.scanAndNotifyNewCandles(detectedTfs);
        } else {
          logger.debug('LOOP 3: No candles detected');
        }
      } catch (err) {
        logger.error({ err }, 'LOOP 3: Unexpected error');
      } finally {
        schedule();
      }
    };

    schedule();
  },

  /**
   * scanAndNotifyNewCandles: Full comprehensive scan filtered to candle TFs
   */
  async scanAndNotifyNewCandles(detectedTfs = []) {
    try {
      if (!detectedTfs || detectedTfs.length === 0) {
        logger.info('LOOP 3: No TFs to scan');
        return;
      }

      const scanStart = Date.now();
      logger.info({ detectedTfs }, 'LOOP 3: Starting comprehensive scan for new candle signals');

      // FULL COMPREHENSIVE SCAN for all symbols
      const db = dbModule.get();
      const rows = db.prepare('SELECT symbol FROM symbols ORDER BY symbol COLLATE NOCASE ASC').all();
      let totalSymbolsProcessed = 0;
      
      for (let i = 0; i < rows.length; i += config.PAGE_SIZE) {
        const page = rows.slice(i, i + config.PAGE_SIZE);
        const tasks = page.map(r => this.fullComprehensiveScan(r.symbol, { detected_ts: scanStart }));
        try {
          const results = await Promise.all(tasks);
          totalSymbolsProcessed += page.length;
        } catch (e) {
          logger.debug({ e }, 'LOOP 3: Page error (continuing)');
        }
      }

      logger.info({ totalSymbolsProcessed }, 'LOOP 3: Comprehensive scan completed, waiting for DB persistence');
      
      // Wait for DB to persist all signals
      await sleep(200);

      // Get signals from this scan and filter to detected TFs
      const allSignals = dbModule.getLatestSignalsSnapshot();
      const candleSignals = allSignals.filter(s => 
        detectedTfs.includes(String(s.root_tf)) && s.detected_at >= scanStart
      );

      logger.info({ 
        detectedTfs,
        foundCount: candleSignals.length
      }, 'LOOP 3: Filtered to candle signals');

      if (candleSignals.length > 0) {
        logger.info({ count: candleSignals.length }, 'LOOP 3: SENDING TELEGRAM BLOCKS FOR CANDLE SIGNALS');
        const telegram = require('./telegram');

        for (let i = 0; i < candleSignals.length; i++) {
          const s = candleSignals[i];
          try {
            logger.info({ symbol: s.symbol, tf: s.root_tf }, 'LOOP 3: Sending signal block');
            await telegram.sendNewSignalSingleBlock(s);
          } catch (e) {
            logger.error({ err: e, symbol: s.symbol }, 'LOOP 3: Failed to send block');
          }
          await sleep(config.TELEGRAM_SEND_DELAY_MS || 100);
        }
      } else {
        logger.info({ detectedTfs }, 'LOOP 3: No signals detected on these candles');
      }
    } catch (err) {
      logger.error({ err }, 'LOOP 3: scanAndNotifyNewCandles error');
    }
  },

  calculateNextCandleEvent() {
    const nowMs = Date.now();
    let nextOpenMs = Infinity;
    let nextTf = null;

    for (const tf of (config.ROOT_TFS || [])) {
      const tfMs = this.timeframeToMs(tf);
      if (tfMs <= 0) continue;

      const epochFloor = Math.floor(nowMs / tfMs);
      const candidateMs = (epochFloor + 1) * tfMs;

      if (candidateMs < nextOpenMs) {
        nextOpenMs = candidateMs;
        nextTf = tf;
      }
    }

    if (nextOpenMs === Infinity) {
      return {
        tf: '1m',
        nextOpenTime: nowMs + 60000,
        waitMs: 60000
      };
    }

    const bufferMs = 100;
    const waitMs = Math.max(nextOpenMs - nowMs - bufferMs, 100);

    return {
      tf: nextTf,
      nextOpenTime: nextOpenMs,
      waitMs
    };
  },

  detectCurrentCandleOpens() {
    const nowMs = Date.now();
    const detectedTfs = [];
    const openWindowMs = 2000;

    for (const tf of (config.ROOT_TFS || [])) {
      const tfMs = this.timeframeToMs(tf);
      if (tfMs <= 0) continue;

      const epochFloor = Math.floor(nowMs / tfMs);
      const candleOpenMs = epochFloor * tfMs;
      const timeSinceCandleOpen = nowMs - candleOpenMs;

      if (timeSinceCandleOpen >= -openWindowMs && timeSinceCandleOpen <= openWindowMs) {
        detectedTfs.push(tf);
        logger.debug({ 
          tf, 
          timeSinceCandleOpen,
          candleOpenTime: new Date(candleOpenMs).toISOString()
        }, 'LOOP 3: Detected candle open');
      }
    }

    return detectedTfs;
  },

  timeframeToMs(tf) {
    if (!tf) return -1;
    const tfStr = String(tf).toUpperCase();
    if (tfStr === 'D') return 24 * 60 * 60 * 1000;
    const match = tfStr.match(/^(\d+)([MHWD])$/);
    if (!match) return -1;
    const num = parseInt(match[1], 10);
    const unit = match[2];
    switch (unit) {
      case 'M': return num * 60 * 1000;
      case 'H': return num * 60 * 60 * 1000;
      case 'W': return num * 7 * 24 * 60 * 60 * 1000;
      case 'D': return num * 24 * 60 * 60 * 1000;
      default: return -1;
    }
  }
};
