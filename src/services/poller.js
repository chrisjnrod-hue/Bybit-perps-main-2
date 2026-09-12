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
          logger.info('poller: startup full scan completed');
        } catch (err) {
          logger.error({ err }, 'poller: scanAllForStartup error');
        }

        // Send startup summary via telegram (includes header + per-signal blocks + recommended)
        try {
          const telegram = require('./telegram');
          const snapshot = dbModule.getLatestSignalsSnapshot();
          await telegram.sendStartupSummary({ snapshot });
          logger.info('poller: Startup summary telegram sent');
        } catch (err) {
          logger.error({ err }, 'poller: Failed to send startup summary');
        }

      } catch (err) {
        logger.error({ err }, 'poller: initialScan failed');
      }
    })();

    // LOOP 2: 5m Boundary Scan (precise 5-minute aligned scanning)
    this.schedule5mBoundaryScan();

    // LOOP 3: New Root Candle Open Scan (aligned to root TF candle opens)
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
      logger.info('scanAllForStartup: starting full startup pass (silent)');
      const db = dbModule.get();
      const rows = db.prepare('SELECT symbol FROM symbols ORDER BY symbol COLLATE NOCASE ASC').all();
      for (let i = 0; i < rows.length; i += config.PAGE_SIZE) {
        const page = rows.slice(i, i + config.PAGE_SIZE);
        const tasks = page.map(r => this.scanSymbolRootsWithoutTracking(r.symbol, { notifyImmediately: false, detected_ts: Date.now() }));
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

  /**
   * scanSymbolRootsWithoutTracking:
   * Used by LOOP 1 (initial scan), LOOP 2 (5m boundary), and LOOP 3 (new candle scan)
   * Does NOT use tracking state, allowing fresh detection on each call
   * Returns array of signal objects detected
   */
  async scanSymbolRootsWithoutTracking(symbol, { notifyImmediately = false, detected_ts = null } = {}) {
    const tfList = config.ROOT_TFS || [];
    const results = [];
    
    for (const tf of tfList) {
      try {
        const db = dbModule.get();
        const selectStmt = db.prepare('SELECT open_time, close, open FROM klines WHERE symbol=? AND timeframe=? ORDER BY open_time DESC LIMIT 2');
        let rows = selectStmt.all(symbol, tf);
        
        if (!rows || rows.length < 2) {
          logger.debug({ symbol, tf }, 'scanSymbolRootsWithoutTracking: insufficient klines, seeding');
          await this.seedKlinesForSymbol(symbol, tf);
          rows = selectStmt.all(symbol, tf);
          if (!rows || rows.length < 2) {
            logger.debug({ symbol, tf }, 'scanSymbolRootsWithoutTracking: still insufficient after seeding, skipping');
            continue;
          }
        }

        // Check for MACD flip WITHOUT using any tracking state
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
        logger.debug({ err, symbol, tf }, 'scanSymbolRootsWithoutTracking: error checking flip');
      }
    }
    return results;
  },

  /**
   * LOOP 2: 5m Boundary Scan
   * - Runs every 5 minutes aligned to UTC boundaries (0, 5, 10, 15, ... 55 minutes)
   * - Actively scans all symbols for root signals
   * - Sends telegram per-signal blocks for NEW signals (compares against previous boundary)
   * - Enables trading after first boundary
   * - Closes least profitable trade if needed
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
    let lastBoundarySignalKeys = new Set(); // Track signals from LAST boundary

    const schedule = async () => {
      const wait = msToNext5();
      logger.info({ 
        wait, 
        loopName: 'LOOP_2_5m_boundary_scan', 
        nextBoundary: new Date(Date.now() + wait).toISOString(),
        trackedSignals: lastBoundarySignalKeys.size
      }, 'LOOP 2: Waiting ms until next 5m boundary');
      
      setTimeout(async () => {
        try {
          // Execute scan with last boundary keys
          await this.scan5mBoundary(lastBoundarySignalKeys);
          
          // UPDATE lastBoundarySignalKeys for NEXT boundary iteration
          const currentSignals = dbModule.getLatestSignalsSnapshot();
          lastBoundarySignalKeys = new Set(currentSignals.map(s => s.key));
          logger.info({ trackedSignals: lastBoundarySignalKeys.size }, 'LOOP 2: Updated boundary tracking');

          // Close least profitable trade if enabled and max trades filled
          if (config.CLOSE_LEAST_PROFITABLE_ENABLED) {
            try {
              const openCount = dbModule.get().prepare('SELECT COUNT(*) as c FROM trades WHERE status = ?').get('open').c || 0;
              if (openCount >= config.MAX_OPEN_TRADES) {
                const minutesSinceHour = new Date().getUTCMinutes();
                const closeMins = config.CLOSE_LEAST_PROFITABLE_MINS_BEFORE_BOUNDARY || 5;
                if (minutesSinceHour >= (60 - closeMins)) {
                  logger.info({ openCount, closeMins }, 'LOOP 2: Closing least profitable trade before boundary');
                  await tradeManager.closeLeastProfitableTrade();
                }
              }
            } catch (err) {
              logger.error({ err }, 'LOOP 2: Error checking/closing trades');
            }
          }

          if (!firstBoundaryPassed) {
            firstBoundaryPassed = true;
            try {
              signalManager.setOpenTradesAllowed(true);
              logger.info('LOOP 2: Open trades enabled after first 5m boundary');
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
   * scan5mBoundary:
   * - Actively SCANS all symbols for root signal flips
   * - Collects signals detected in this scan
   * - Waits for DB persistence before fetching snapshot
   * - Compares against lastBoundarySignalKeys to identify NEW signals
   * - Sends telegram per-signal blocks ONLY for new signals
   */
  async scan5mBoundary(lastBoundarySignalKeys = new Set()) {
    try {
      const scanStart = Date.now();
      logger.info({ 
        scanStart: new Date(scanStart).toISOString(),
        previousBoundaryKeys: lastBoundarySignalKeys.size
      }, 'LOOP 2: Starting 5m boundary scan');

      // ACTIVELY SCAN all symbols for root signals
      const db = dbModule.get();
      const rows = db.prepare('SELECT symbol FROM symbols ORDER BY symbol COLLATE NOCASE ASC').all();
      const allNewSignalsFromThisScan = [];

      for (let i = 0; i < rows.length; i += config.PAGE_SIZE) {
        const page = rows.slice(i, i + config.PAGE_SIZE);
        const tasks = page.map(r => 
          this.scanSymbolRootsWithoutTracking(r.symbol, { notifyImmediately: false, detected_ts: scanStart })
        );
        try {
          const results = await Promise.all(tasks);
          // Collect all signals returned from this scan batch
          for (const result of results) {
            if (Array.isArray(result)) {
              allNewSignalsFromThisScan.push(...result);
            }
          }
        } catch (e) {
          logger.debug({ e }, 'LOOP 2: Page error (continuing)');
        }
      }

      // Small delay to ensure DB persistence before fetching snapshot
      await sleep(100);

      // Get current signals snapshot AFTER scan completion and persistence
      const currentSignals = dbModule.getLatestSignalsSnapshot();
      const currentKeys = new Set(currentSignals.map(s => s.key));

      // Identify NEW signals: those detected in this scan that weren't in previous boundary
      const newSignals = currentSignals.filter(s => 
        !lastBoundarySignalKeys.has(s.key) && s.detected_at >= scanStart
      );

      logger.info({ 
        totalSignals: currentSignals.length, 
        previousBoundary: lastBoundarySignalKeys.size, 
        newCount: newSignals.length,
        scannedThisCycle: allNewSignalsFromThisScan.length
      }, 'LOOP 2: Scan complete');

      // SEND TELEGRAM FOR NEW SIGNALS
      if (newSignals.length > 0) {
        logger.info({ newCount: newSignals.length }, 'LOOP 2: Sending telegram blocks for new signals');
        const telegram = require('./telegram');

        for (let i = 0; i < newSignals.length; i++) {
          const s = newSignals[i];
          try {
            logger.debug({ symbol: s.symbol, tf: s.root_tf, key: s.key }, 'LOOP 2: Sending signal block');
            await telegram.sendNewSignalSingleBlock(s);
            logger.info({ symbol: s.symbol, tf: s.root_tf }, 'LOOP 2: Signal block sent successfully');
          } catch (e) {
            logger.error({ err: e, symbol: s.symbol, tf: s.root_tf }, 'LOOP 2: Failed to send block');
          }
          await sleep(config.TELEGRAM_SEND_DELAY_MS || 100);
        }
      } else {
        logger.info({ 
          currentKeys: currentKeys.size, 
          previousKeys: lastBoundarySignalKeys.size 
        }, 'LOOP 2: No new signals found this boundary');
      }

    } catch (err) {
      logger.error({ err }, 'LOOP 2: Unexpected error');
    }
  },

  /**
   * LOOP 3: New Root Candle Open Scan
   * - Runs at PRECISE root timeframe candle opens
   * - Detects when candles open using epoch-aligned math
   * - Actively scans for new signals AT candle open time
   * - Sends telegram per-signal blocks for detected signals
   */
  scheduleNewRootCandleScan() {
    const schedule = async () => {
      try {
        const nextEvent = this.calculateNextCandleEvent();
        const waitMs = Math.max(nextEvent.waitMs, 100);
        
        logger.info({ 
          nextTf: nextEvent.tf, 
          nextOpenTime: new Date(nextEvent.nextOpenTime).toISOString(),
          waitMs,
          loopName: 'LOOP_3_new_root_candle_scan' 
        }, 'LOOP 3: Scheduled next candle scan');
        
        await sleep(waitMs);
        
        // Execute scan at candle open (with buffer already baked into waitMs)
        const detectedTfs = this.detectCurrentCandleOpens();
        if (detectedTfs && detectedTfs.length > 0) {
          logger.info({ detectedTfs, time: new Date().toISOString() }, 'LOOP 3: Candle(s) detected as open');
          await this.scanAndNotifyNewCandles(detectedTfs);
        } else {
          logger.debug('LOOP 3: No candles detected as open this cycle');
        }
      } catch (err) {
        logger.error({ err }, 'LOOP 3: Unexpected error');
      } finally {
        // Reschedule immediately
        schedule();
      }
    };

    schedule();
  },

  /**
   * calculateNextCandleEvent:
   * - Calculate NEXT candle open time across ALL root TFs
   * - Return which TF opens next and when in ms
   * - Factor in 100ms buffer for system latency
   */
  calculateNextCandleEvent() {
    const nowMs = Date.now();
    let nextOpenMs = Infinity;
    let nextTf = null;

    for (const tf of (config.ROOT_TFS || [])) {
      const tfMs = this.timeframeToMs(tf);
      if (tfMs <= 0) continue;

      // Calculate the epoch-aligned next candle open time
      const epochFloor = Math.floor(nowMs / tfMs);
      const candidateMs = (epochFloor + 1) * tfMs;

      if (candidateMs < nextOpenMs) {
        nextOpenMs = candidateMs;
        nextTf = tf;
      }
    }

    if (nextOpenMs === Infinity) {
      // Fallback to 1 minute
      return {
        tf: '1m',
        nextOpenTime: nowMs + 60000,
        waitMs: 60000
      };
    }

    // Calculate wait time; trigger ~100ms before actual open to account for system latency
    const bufferMs = 100;
    const waitMs = Math.max(nextOpenMs - nowMs - bufferMs, 100);

    return {
      tf: nextTf,
      nextOpenTime: nextOpenMs,
      waitMs
    };
  },

  /**
   * detectCurrentCandleOpens:
   * - Check which root TFs are currently opening (within ±2 second window)
   * - Returns array of TF strings that are detected as open
   */
  detectCurrentCandleOpens() {
    const nowMs = Date.now();
    const detectedTfs = [];
    const openWindowMs = 2000; // ±2 second window around candle open

    for (const tf of (config.ROOT_TFS || [])) {
      const tfMs = this.timeframeToMs(tf);
      if (tfMs <= 0) continue;

      // Calculate the epoch-aligned candle open time closest to now
      const epochFloor = Math.floor(nowMs / tfMs);
      const candleOpenMs = epochFloor * tfMs;

      // Check if we're within the open window
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

  /**
   * scanAndNotifyNewCandles:
   * - Called when new root candles open (from LOOP 3)
   * - Actively SCANS all symbols for root signals on newly opened TFs
   * - Waits for DB persistence
   * - Sends telegram per-signal blocks for detected signals
   */
  async scanAndNotifyNewCandles(detectedTfs = []) {
    try {
      if (!detectedTfs || detectedTfs.length === 0) {
        logger.info('LOOP 3: No TFs to scan');
        return;
      }

      logger.info({ detectedTfs, time: new Date().toISOString() }, 'LOOP 3: Starting active scan for new candle signals');
      const scanStart = Date.now();

      // ACTIVELY SCAN all symbols for signals on the newly opened TFs
      const db = dbModule.get();
      const rows = db.prepare('SELECT symbol FROM symbols ORDER BY symbol COLLATE NOCASE ASC').all();
      const allNewSignalsFromThisScan = [];

      for (let i = 0; i < rows.length; i += config.PAGE_SIZE) {
        const page = rows.slice(i, i + config.PAGE_SIZE);
        const tasks = page.map(r => 
          this.scanSymbolRootsWithoutTracking(r.symbol, { notifyImmediately: false, detected_ts: scanStart })
        );
        try {
          const results = await Promise.all(tasks);
          // Collect all signals returned from this scan batch
          for (const result of results) {
            if (Array.isArray(result)) {
              allNewSignalsFromThisScan.push(...result);
            }
          }
        } catch (e) {
          logger.debug({ e }, 'LOOP 3: Page error (continuing)');
        }
      }

      // Small delay to ensure DB persistence before fetching snapshot
      await sleep(100);

      // Get signals detected in this scan and filter to the opened TFs
      const allSignals = dbModule.getLatestSignalsSnapshot();
      const candleSignals = allSignals.filter(s => 
        detectedTfs.includes(String(s.root_tf)) && s.detected_at >= scanStart
      );

      logger.info({ 
        detectedTfs, 
        foundCount: candleSignals.length,
        scannedThisCycle: allNewSignalsFromThisScan.length
      }, 'LOOP 3: Scan complete');

      if (candleSignals.length > 0) {
        logger.info({ count: candleSignals.length }, 'LOOP 3: Signals detected, sending telegram blocks');
        const telegram = require('./telegram');

        for (let i = 0; i < candleSignals.length; i++) {
          const s = candleSignals[i];
          try {
            logger.debug({ symbol: s.symbol, tf: s.root_tf, key: s.key }, 'LOOP 3: Sending signal block');
            await telegram.sendNewSignalSingleBlock(s);
            logger.info({ symbol: s.symbol, tf: s.root_tf }, 'LOOP 3: Signal block sent successfully');
          } catch (e) {
            logger.error({ err: e, symbol: s.symbol, tf: s.root_tf }, 'LOOP 3: Failed to send block');
          }
          await sleep(config.TELEGRAM_SEND_DELAY_MS || 100);
        }
      } else {
        logger.info({ detectedTfs }, 'LOOP 3: No signals detected on these candle opens');
      }
    } catch (err) {
      logger.error({ err }, 'LOOP 3: scanAndNotifyNewCandles error');
    }
  },

  /**
   * Helper: Convert timeframe string to milliseconds
   * Supports: 1m, 5m, 15m, 1h, 4h, 1d, etc.
   */
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
