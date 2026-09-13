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

// Separate state tracking for LOOP 1 (initial scan) and LOOP 3 (candle open scan)
let lastRootCandleStateLoop1 = {}; // LOOP 1: Initial/Deploy Scan
let lastRootCandleStateLoop3 = {}; // LOOP 3: Root TF Candle Open Scan

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
        
        // LOOP 1 - Full silent startup scan (Deploy)
        try {
          await this.scanAllForStartup();
          logger.info('poller: startup full scan completed');
        } catch (err) {
          logger.error({ err }, 'poller: scanAllForStartup error');
        }

        // Enable trading after initial scan complete
        try {
          signalManager.setOpenTradesAllowed(true);
          logger.info('poller: open trades enabled after initial scan');
        } catch (e) { 
          logger.debug({ e }, 'Failed to set open trades allowed'); 
        }

        // Start LOOP 2 - 5m boundary scan
        this.start5mBoundaryScanLoop();

        // Start LOOP 3 - Root TF candle open scan
        this.startRootTfCandleOpenScanLoop();

      } catch (err) {
        logger.error({ err }, 'poller: initialScan failed');
      }
    })();
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

  // ============================================================
  // LOOP 1: Initial/Deploy Scan (runs once at startup)
  // Sends full signal blocks with summary & recommended
  // ============================================================
  async scanAllForStartup() {
    try {
      logger.info('scanAllForStartup: starting full startup pass (silent)');
      const db = dbModule.get();
      const rows = db.prepare('SELECT symbol FROM symbols ORDER BY symbol COLLATE NOCASE ASC').all();
      for (let i = 0; i < rows.length; i += config.PAGE_SIZE) {
        const page = rows.slice(i, i + config.PAGE_SIZE);
        const tasks = page.map(r => this.scanSymbolRootsLoop1(r.symbol, { notifyImmediately: false, detected_ts: Date.now() }));
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

  // ============================================================
  // LOOP 2: 5m Boundary Scan (runs at every 5m boundary)
  // ONLY sends 1 new signal per block (not in previous snapshot)
  // NO summary or recommended blocks (those only for LOOP 1 and 3)
  // ============================================================
  start5mBoundaryScanLoop() {
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

    const loop = async () => {
      const wait = msToNext5();
      logger.info({ wait, loop: '5m-boundary-scan' }, 'Next 5m boundary scan in ms');
      
      setTimeout(async () => {
        try {
          await this.scan5mBoundary();
        } catch (err) {
          logger.error({ err }, 'LOOP 2 (5m-boundary-scan): error');
        } finally {
          loop();
        }
      }, wait);
    };

    loop();
  },

  async scan5mBoundary() {
    try {
      const db = dbModule;
      const prev = db.getLatestSignalsSnapshot();
      const prevKeys = new Set(prev.map(r => r.key));

      const scanStart = Date.now();

      logger.info({ loop: '5m-boundary-scan' }, 'Starting 5m boundary scan');

      const rows = db.get().prepare('SELECT symbol FROM symbols ORDER BY symbol COLLATE NOCASE ASC').all();
      for (let i = 0; i < rows.length; i += config.PAGE_SIZE) {
        const page = rows.slice(i, i + config.PAGE_SIZE);
        const tasks = page.map(r => this.scanSymbolRootsLoop2(r.symbol, { notifyImmediately: false, detected_ts: scanStart }));
        try {
          await Promise.all(tasks);
        } catch (e) {
          logger.debug({ e }, 'scan5mBoundary: page tasks error (continuing)');
        }
      }

      const after = db.getLatestSignalsSnapshot();
      const newSignals = after.filter(r => !prevKeys.has(r.key) && r.detected_at >= scanStart);

      // LOOP 2: Send ONLY 1 new signal per block (no summary/recommended)
      if (newSignals.length > 0) {
        logger.info({ newSignals: newSignals.length, loop: '5m-boundary-scan' }, 'New signals detected at 5m boundary');

        const telegram = require('./telegram');
        for (let i = 0; i < newSignals.length; i++) {
          const s = newSignals[i];
          try {
            // Send ONLY signal block (notifyImmediately=false means no summary/recommended)
            await telegram.sendNewSignalSingleBlock(s);
          } catch (e) {
            logger.debug({ e, s }, 'scan5mBoundary: failed to send new-signal message');
          }
          await sleep(config.TELEGRAM_SEND_DELAY_MS || 100);
        }
      } else {
        logger.info({ loop: '5m-boundary-scan' }, 'No new signals at 5m boundary');
      }

      try {
        db.setState('lastScanAt', scanStart);
        db.setState('lastScanSignals', after.map(r => r.key));
      } catch (e) {
        logger.debug({ e }, 'scan5mBoundary: failed to persist scan state');
      }

      // Check for new root TF candles and send MTF alignment alerts (separate telegram)
      try {
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

        if (newRootTfs.length && config.NEW_ROOT_CANDLE_NOTIFY) {
          logger.info({ newRootTfs, loop: '5m-boundary-scan' }, 'New root candles detected - sending MTF alignment alerts');
          try {
            await signalManager.handleNewRootCandle(newRootTfs);
          } catch (e) {
            logger.debug({ e, newRootTfs }, 'scan5mBoundary: handleNewRootCandle failed');
          }
        }
      } catch (e) {
        logger.debug({ e }, 'scan5mBoundary: root TF detection failed');
      }

      // Close least profitable trade if enabled and max trades filled
      if (config.CLOSE_LEAST_PROFITABLE_ENABLED) {
        try {
          const openCount = dbModule.get().prepare('SELECT COUNT(*) as c FROM trades WHERE status = ?').get('open').c || 0;
          if (openCount >= config.MAX_OPEN_TRADES) {
            const minutesSinceHour = new Date().getUTCMinutes();
            const closeMins = config.CLOSE_LEAST_PROFITABLE_MINS_BEFORE_BOUNDARY || 5;
            if (minutesSinceHour >= (60 - closeMins)) {
              logger.info({ openCount, closeMins, loop: '5m-boundary-scan' }, 'Closing least profitable trade before boundary');
              try {
                await tradeManager.closeLeastProfitableTrade();
              } catch (err) {
                logger.error({ err }, 'Error closing least profitable trade');
              }
            }
          }
        } catch (err) {
          logger.debug({ err }, 'scan5mBoundary: least profitable check failed');
        }
      }

    } catch (err) {
      logger.error({ err }, 'scan5mBoundary: unexpected error');
    }
  },

  // ============================================================
  // LOOP 3: Root TF Candle Open Scan (triggered at candle open)
  // Detects flips at exact candle open and sends full Telegram
  // with summary & recommended blocks
  // ============================================================
  startRootTfCandleOpenScanLoop() {
    const getNextCandleOpenTime = () => {
      const now = new Date();
      const currentMinutes = now.getUTCMinutes();
      const currentSeconds = now.getUTCSeconds();
      
      // Find smallest root TF
      const rootTfs = config.ROOT_TFS || [];
      let minTfMinutes = Infinity;
      
      for (const tf of rootTfs) {
        if (String(tf).toUpperCase() === 'D') {
          minTfMinutes = Math.min(minTfMinutes, 1440);
        } else {
          const tfNum = Number(tf);
          if (!isNaN(tfNum)) {
            minTfMinutes = Math.min(minTfMinutes, tfNum);
          }
        }
      }
      
      if (minTfMinutes === Infinity) return null;
      
      // Next candle open time
      const minutesSinceEpoch = Math.floor(now.getTime() / 60000);
      const nextCandleEpoch = (Math.floor(minutesSinceEpoch / minTfMinutes) + 1) * minTfMinutes;
      const nextCandleTime = new Date(nextCandleEpoch * 60000);
      
      return nextCandleTime;
    };

    const loop = async () => {
      const nextCandleTime = getNextCandleOpenTime();
      if (!nextCandleTime) {
        logger.warn({ loop: 'root-tf-candle-open-scan' }, 'Could not determine next candle time');
        setTimeout(() => loop(), 60000);
        return;
      }

      const wait = nextCandleTime - new Date();
      logger.info({ wait, nextCandleTime: nextCandleTime.toISOString(), loop: 'root-tf-candle-open-scan' }, 'Next root TF candle open scan in ms');
      
      setTimeout(async () => {
        try {
          await this.scanRootTfCandleOpen();
        } catch (err) {
          logger.error({ err }, 'LOOP 3 (root-tf-candle-open-scan): error');
        } finally {
          loop();
        }
      }, Math.max(wait, 0));
    };

    loop();
  },

  async scanRootTfCandleOpen() {
    try {
      const db = dbModule;
      const scanStart = Date.now();

      logger.info({ loop: 'root-tf-candle-open-scan' }, 'Scanning root TF candle opens for flips');

      const rows = db.get().prepare('SELECT symbol FROM symbols ORDER BY symbol COLLATE NOCASE ASC').all();
      const detectedFlips = [];

      for (let i = 0; i < rows.length; i += config.PAGE_SIZE) {
        const page = rows.slice(i, i + config.PAGE_SIZE);
        const tasks = page.map(r => 
          this.scanSymbolRootsLoop3(r.symbol, scanStart)
            .then(flips => {
              if (Array.isArray(flips)) detectedFlips.push(...flips);
            })
        );
        try {
          await Promise.all(tasks);
        } catch (e) {
          logger.debug({ e }, 'scanRootTfCandleOpen: page tasks error (continuing)');
        }
      }

      // LOOP 3: Send full signal blocks with summary & recommended
      if (detectedFlips.length > 0) {
        logger.info({ detectedFlips: detectedFlips.length, loop: 'root-tf-candle-open-scan' }, 'Flips detected at candle open');

        const telegram = require('./telegram');
        for (let i = 0; i < detectedFlips.length; i++) {
          const s = detectedFlips[i];
          try {
            // Send flip signal with full layout (notifyImmediately=true includes summary/recommended)
            await telegram.sendNewSignalSingleBlock(s);
          } catch (e) {
            logger.debug({ e, s }, 'scanRootTfCandleOpen: failed to send flip message');
          }
          await sleep(config.TELEGRAM_SEND_DELAY_MS || 100);
        }
      } else {
        logger.info({ loop: 'root-tf-candle-open-scan' }, 'No flips detected at candle open');
      }

    } catch (err) {
      logger.error({ err }, 'scanRootTfCandleOpen: unexpected error');
    }
  },

  // ============================================================
  // LOOP 1: Scan for root TF signals (full blocks with summary)
  // ============================================================
  async scanSymbolRootsLoop1(symbol, { notifyImmediately = true, detected_ts = null } = {}) {
    const tfList = config.ROOT_TFS || [];
    const results = [];
    for (const tf of tfList) {
      try {
        const db = dbModule.get();
        const selectStmt = db.prepare('SELECT open_time, close, open FROM klines WHERE symbol=? AND timeframe=? ORDER BY open_time DESC LIMIT 2');
        let rows = selectStmt.all(symbol, tf);
        if (!rows || rows.length < 2) {
          logger.debug({ symbol, tf, loop: 'LOOP 1' }, 'scanSymbolRootsLoop1: insufficient klines, seeding now');
          await this.seedKlinesForSymbol(symbol, tf);

          rows = selectStmt.all(symbol, tf);
          if (!rows || rows.length < 2) {
            logger.debug({ symbol, tf, loop: 'LOOP 1' }, 'scanSymbolRootsLoop1: still insufficient klines after seeding, skipping tf');
            continue;
          } else {
            logger.info({ symbol, tf, loop: 'LOOP 1' }, 'scanSymbolRootsLoop1: klines seeded, re-checking flip');
          }
        }

        // Track last candle for LOOP 1 only
        const stateKey = `${symbol}:${tf}`;
        const latestCandleTime = rows[rows.length - 1].open_time;
        
        if (lastRootCandleStateLoop1[stateKey] === latestCandleTime) {
          logger.debug({ symbol, tf, candleTime: latestCandleTime, loop: 'LOOP 1' }, 'scanSymbolRootsLoop1: already processed this candle, skipping');
          continue;
        }
        
        lastRootCandleStateLoop1[stateKey] = latestCandleTime;

        const flip = await require('./macd').isMacdFlip(symbol, tf);
        if (flip) {
          const sig = await signalManager.handleRootSignal({
            symbol,
            root_tf: tf,
            detected_at: detected_ts || Date.now(),
            notifyImmediately
          });
          if (sig) results.push(sig);
          logger.info({ symbol, tf, loop: 'LOOP 1' }, 'MACD flip detected and signal created');
        }
      } catch (err) {
        logger.debug({ err, symbol, tf }, 'scanSymbolRootsLoop1: error checking flip');
      }
    }
    return results;
  },

  // ============================================================
  // LOOP 2: Scan for root TF signals at 5m boundary
  // (No deduplication needed, signals added to snapshot each call)
  // ============================================================
  async scanSymbolRootsLoop2(symbol, { notifyImmediately = false, detected_ts = null } = {}) {
    const tfList = config.ROOT_TFS || [];
    const results = [];
    for (const tf of tfList) {
      try {
        const db = dbModule.get();
        const selectStmt = db.prepare('SELECT open_time, close, open FROM klines WHERE symbol=? AND timeframe=? ORDER BY open_time DESC LIMIT 2');
        let rows = selectStmt.all(symbol, tf);
        if (!rows || rows.length < 2) {
          logger.debug({ symbol, tf, loop: '5m-boundary-scan' }, 'scanSymbolRootsLoop2: insufficient klines, skipping');
          continue;
        }

        const flip = await require('./macd').isMacdFlip(symbol, tf);
        if (flip) {
          const sig = await signalManager.handleRootSignal({
            symbol,
            root_tf: tf,
            detected_at: detected_ts || Date.now(),
            notifyImmediately
          });
          if (sig) results.push(sig);
          logger.info({ symbol, tf, loop: '5m-boundary-scan' }, 'MACD flip detected and signal created');
        }
      } catch (err) {
        logger.debug({ err, symbol, tf }, 'scanSymbolRootsLoop2: error checking flip');
      }
    }
    return results;
  },

  // ============================================================
  // LOOP 3: Scan for root TF signals at candle open (detects flips)
  // ============================================================
  async scanSymbolRootsLoop3(symbol, detected_ts = null) {
    const tfList = config.ROOT_TFS || [];
    const flips = [];

    for (const tf of tfList) {
      try {
        const db = dbModule.get();
        const selectStmt = db.prepare('SELECT open_time, close, open FROM klines WHERE symbol=? AND timeframe=? ORDER BY open_time DESC LIMIT 2');
        let rows = selectStmt.all(symbol, tf);

        if (!rows || rows.length < 2) {
          logger.debug({ symbol, tf, loop: 'root-tf-candle-open-scan' }, 'scanSymbolRootsLoop3: insufficient klines for flip check');
          continue;
        }

        // Track last candle for LOOP 3 only (independent from LOOP 1)
        const stateKey = `${symbol}:${tf}`;
        const latestCandleTime = rows[rows.length - 1].open_time;
        
        if (lastRootCandleStateLoop3[stateKey] === latestCandleTime) {
          logger.debug({ symbol, tf, candleTime: latestCandleTime, loop: 'root-tf-candle-open-scan' }, 'scanSymbolRootsLoop3: already processed this candle, skipping');
          continue;
        }
        
        lastRootCandleStateLoop3[stateKey] = latestCandleTime;

        // Check for MACD flip
        const flip = await require('./macd').isMacdFlip(symbol, tf);
        if (flip) {
          const sig = await signalManager.handleRootSignal({
            symbol,
            root_tf: tf,
            detected_at: detected_ts || Date.now(),
            notifyImmediately: true
          });
          if (sig) flips.push(sig);
          logger.info({ symbol, tf, loop: 'root-tf-candle-open-scan' }, 'MACD flip detected and signal created');
        }
      } catch (err) {
        logger.debug({ err, symbol, tf }, 'scanSymbolRootsLoop3: error checking flip');
      }
    }

    return flips;
  }
};
