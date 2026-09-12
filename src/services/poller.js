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
let lastScannedSignalKeys = new Set(); // Track signals from last 5m boundary scan

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
        const latestCandleTime = rows[rows.length - 1].open_time; // oldest in DESC = latest candle
        
        if (lastRootCandleState[stateKey] === latestCandleTime) {
          logger.debug({ symbol, tf, candleTime: latestCandleTime }, 'scanSymbolRoots: already processed this candle, skipping flip detection');
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
   * LOOP 2: 5m Boundary Scan
   * - Runs every 5 minutes aligned to UTC boundaries (0, 5, 10, 15, ... 55 minutes)
   * - Scans all symbols for NEW root signals not in last scan
   * - Sends telegram per-signal blocks for new signals only (no summary/recommended)
   * - Also monitors MTF alignment alerts on existing signals
   * - Enables trading after first boundary
   * - Closes least profitable trade if slot needed
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

    const schedule = async () => {
      const wait = msToNext5();
      logger.info({ wait, loopName: '5m_boundary_scan' }, 'schedule5mBoundaryScan: waiting ms until next 5m boundary');
      setTimeout(async () => {
        try {
          await this.scan5mBoundary();

          // Close least profitable trade if enabled and max trades filled
          if (config.CLOSE_LEAST_PROFITABLE_ENABLED) {
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
              logger.info('schedule5mBoundaryScan: open trades enabled after first boundary');
            } catch (e) {
              logger.debug({ e }, 'schedule5mBoundaryScan: failed to set open trades allowed');
            }
          }
        } catch (err) {
          logger.error({ err }, 'schedule5mBoundaryScan: boundary task failed');
        } finally {
          schedule();
        }
      }, wait);
    };

    schedule();
  },

  /**
   * scan5mBoundary:
   * - Scans all symbols for new root signals
   * - Compares against lastScannedSignalKeys
   * - Sends telegram ONLY for new signals (per-signal blocks, no summary/recommended)
   * - Updates lastScannedSignalKeys with current signals
   * - Also monitors MTF alignment changes on already-detected signals (TODO)
   */
  async scan5mBoundary() {
    try {
      const db = dbModule;
      const scanStart = Date.now();

      logger.info('scan5mBoundary: starting 5m boundary scan');

      // Scan all symbols for new root signals
      const rows = db.get().prepare('SELECT symbol FROM symbols ORDER BY symbol COLLATE NOCASE ASC').all();
      for (let i = 0; i < rows.length; i += config.PAGE_SIZE) {
        const page = rows.slice(i, i + config.PAGE_SIZE);
        const tasks = page.map(r => this.scanSymbolRoots(r.symbol, { notifyImmediately: false, detected_ts: scanStart }));
        try {
          await Promise.all(tasks);
        } catch (e) {
          logger.debug({ e }, 'scan5mBoundary: page tasks error (continuing)');
        }
      }

      // Get current signals snapshot
      const after = db.getLatestSignalsSnapshot();
      const currentKeys = new Set(after.map(r => r.key));

      // Find NEW signals (not in lastScannedSignalKeys and detected in this scan)
      const newSignals = after.filter(r => !lastScannedSignalKeys.has(r.key) && r.detected_at >= scanStart);

      if (newSignals.length > 0) {
        logger.info({ newSignals: newSignals.length }, 'scan5mBoundary: new signals found, sending per-signal blocks');

        const telegram = require('./telegram');
        for (let i = 0; i < newSignals.length; i++) {
          const s = newSignals[i];
          try {
            // Send ONLY single signal block (no summary, no recommended)
            await telegram.sendNewSignalSingleBlock(s);
          } catch (e) {
            logger.debug({ e, s }, 'scan5mBoundary: failed to send new-signal message');
          }
          await sleep(config.TELEGRAM_SEND_DELAY_MS || 100);
        }
      } else {
        logger.info('scan5mBoundary: no new signals found this boundary');
      }

      // TODO: MTF Alignment Alerts
      // Monitor existing signals for alignment changes and send alert blocks
      // Example: if a signal's MTF alignment improved/degraded significantly

      // Update lastScannedSignalKeys for next boundary
      lastScannedSignalKeys = currentKeys;

      try {
        db.setState('lastScanAt', scanStart);
        db.setState('lastScanSignals', Array.from(currentKeys));
      } catch (e) {
        logger.debug({ e }, 'scan5mBoundary: failed to persist scan state');
      }
    } catch (err) {
      logger.error({ err }, 'scan5mBoundary: unexpected error');
    }
  },

  /**
   * LOOP 3: New Root Candle Open Scan
   * - Runs at precise root timeframe candle opens
   * - Detects when a new root candle opens (1m, 5m, 15m, 1h, D, etc.)
   * - Sends telegram update for signals on those new candles
   */
  scheduleNewRootCandleScan() {
    const calculateNextCandle = () => {
      const now = new Date();
      const nowMs = now.getTime();
      let nextCandle = null;

      for (const tf of config.ROOT_TFS || []) {
        const tfStr = String(tf).toUpperCase();
        const tfMs = this.timeframeToMs(tfStr);
        if (tfMs <= 0) continue;

        const epochMs = Math.floor(nowMs / tfMs) * tfMs + tfMs;
        if (!nextCandle || epochMs < nextCandle) {
          nextCandle = epochMs;
        }
      }

      return nextCandle || (nowMs + 60000); // fallback to 1 min
    };

    const schedule = async () => {
      const nextCandleMs = calculateNextCandle();
      const wait = nextCandleMs - Date.now();
      
      logger.info({ wait, loopName: 'new_root_candle_scan' }, 'scheduleNewRootCandleScan: waiting ms until next root candle');
      
      setTimeout(async () => {
        try {
          await this.scanNewRootCandle();
        } catch (err) {
          logger.error({ err }, 'scheduleNewRootCandleScan: scan task failed');
        } finally {
          schedule();
        }
      }, Math.max(wait, 0));
    };

    schedule();
  },

  /**
   * scanNewRootCandle:
   * - Called when a new root candle opens
   * - Sends telegram update with signals for newly opened root TFs
   * - Includes per-signal blocks (full detail)
   */
  async scanNewRootCandle() {
    try {
      logger.info('scanNewRootCandle: new root candle(s) detected, sending update');
      
      const db = dbModule;
      const snapshot = db.getLatestSignalsSnapshot();
      
      if (!snapshot || snapshot.length === 0) {
        logger.info('scanNewRootCandle: no signals to report');
        return;
      }

      // Identify which root TF just opened
      const now = new Date();
      const openedTfs = [];
      
      for (const tf of config.ROOT_TFS || []) {
        const tfStr = String(tf).toUpperCase();
        const tfMs = this.timeframeToMs(tfStr);
        if (tfMs <= 0) continue;

        const epochMs = Math.floor(now.getTime() / tfMs) * tfMs;
        if (Math.abs(Date.now() - epochMs) < 5000) { // within 5 seconds of candle open
          openedTfs.push(tf);
        }
      }

      logger.info({ openedTfs }, 'scanNewRootCandle: identified newly opened root TFs');

      const telegram = require('./telegram');
      const filtered = openedTfs.length > 0
        ? snapshot.filter(s => openedTfs.includes(String(s.root_tf)))
        : snapshot;

      if (filtered.length > 0) {
        await telegram.sendRootCandleUpdate({ snapshot: filtered, newRootTfs: openedTfs });
        logger.info({ count: filtered.length }, 'scanNewRootCandle: sent root candle update with per-signal blocks');
      } else {
        logger.info('scanNewRootCandle: no signals for newly opened candles');
      }
    } catch (err) {
      logger.error({ err }, 'scanNewRootCandle: unexpected error');
    }
  },

  /**
   * Helper: Convert timeframe string to milliseconds
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
