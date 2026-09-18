// src/services/poller.js
const dbModule = require('../db');
const bybit = require('./bybitRest');
const config = require('../config');
const logger = require('pino')();
const Bottleneck = require('bottleneck');
const macdUtil = require('./macd');
const signalManager = require('./signalManager');
const notificationQueue = require('./notificationQueue');

const limiter = new Bottleneck({ minTime: 50 });
const SEED_CONCURRENCY = Number(config.SEED_CONCURRENCY || 6);

let isRunning = false;
let startupComplete = false;

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms || 0));
}

module.exports = {
  start() {
    if (isRunning) return;
    isRunning = true;
    startupComplete = false;

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

        // Full startup scan
        try {
          await this.scanAllForStartup();
          logger.info('poller: startup full scan completed');
        } catch (err) {
          logger.error({ err }, 'poller: scanAllForStartup error');
        }

        // Enable open trades after initial scan completes
        try {
          signalManager.setOpenTradesAllowed(true);
          logger.info('poller: open trades enabled after initial scan');
        } catch (e) {
          logger.debug({ e }, 'poller: failed to enable open trades');
        }
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
      logger.info('poller: fetching symbols via REST (cursor pagination for USDT perpetuals)');
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
        insert.run(s.symbol, s.base || s.symbol.replace(/USDT(\.P)?$/i, ''), s.quote || 'USDT', now);
      }
    });
    insertMany(allSymbols.filter(s => s && s.symbol));
    logger.info({ total: allSymbols.length }, 'poller.initialScan: symbols persisted (USDT perpetuals only)');

    const seedSymbols = bybit.getSeedSymbols(allSymbols);
    if (seedSymbols && seedSymbols.length) {
      // Validate that all seed symbols are USDT or USDT.P
      const invalidSymbols = seedSymbols.filter(s => {
        const sym = String(s.symbol || '').toUpperCase();
        // Reject if it's a dated variant
        if (/USDT[QHUZ0-9]/.test(sym.slice(-6))) {
          return true;
        }
        // Accept USDT and USDT.P
        return !(/USDT(\.P)?$/.test(sym));
      });

      if (invalidSymbols.length > 0) {
        logger.error(
          { count: invalidSymbols.length, samples: invalidSymbols.slice(0, 5).map(s => s.symbol) },
          'poller.initialScan: CRITICAL - invalid symbols in seed list! These should have been filtered.'
        );
      }

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
      // Validate symbol is USDT or USDT.P
      const symUpper = String(symbol || '').toUpperCase();
      if (!/USDT(\.P)?$/.test(symUpper)) {
        logger.warn({ symbol }, 'seedKlinesForSymbol: symbol is not valid USDT, skipping');
        return;
      }

      // Reject if it's a dated variant
      if (/USDT[QHUZ0-9]/.test(symUpper.slice(-6))) {
        logger.warn({ symbol }, 'seedKlinesForSymbol: symbol is a dated variant, skipping');
        return;
      }

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
    if (startupComplete) {
      logger.info('scanAllForStartup: already completed, skipping');
      return;
    }

    try {
      logger.info('scanAllForStartup: starting full startup pass');
      const db = dbModule.get();
      const rows = db.prepare('SELECT symbol FROM symbols ORDER BY symbol COLLATE NOCASE ASC').all();

      // Validate all symbols are USDT or USDT.P (non-expiry)
      const invalidSymbols = rows.filter(r => {
        const sym = String(r.symbol || '').toUpperCase();
        // Reject if it's a dated variant
        if (/USDT[QHUZ0-9]/.test(sym.slice(-6))) {
          return true;
        }
        // Accept USDT and USDT.P
        return !(/USDT(\.P)?$/.test(sym));
      });

      if (invalidSymbols.length > 0) {
        logger.warn(
          { count: invalidSymbols.length, samples: invalidSymbols.slice(0, 5).map(r => r.symbol) },
          'scanAllForStartup: WARNING - database contains invalid symbols! These will be skipped.'
        );
      }

      // Collect all signals
      const newSignals = [];

      for (let i = 0; i < rows.length; i += config.PAGE_SIZE) {
        const page = rows.slice(i, i + config.PAGE_SIZE);
        const tasks = page
          .filter(r => {
            const sym = String(r.symbol || '').toUpperCase();
            // Reject if it's a dated variant
            if (/USDT[QHUZ0-9]/.test(sym.slice(-6))) {
              return false;
            }
            // Accept USDT and USDT.P
            return /USDT(\.P)?$/.test(sym);
          })
          .map(r => this.scanSymbolRoots(r.symbol));

        try {
          const results = await Promise.all(tasks);
          newSignals.push(...results.flat());
        } catch (e) {
          logger.debug({ e }, 'scanAllForStartup: page tasks error (continuing)');
        }
      }

      // Sort signals by symbol (A-Z)
      newSignals.sort((a, b) => {
        const symA = String(a.symbol || '').toUpperCase();
        const symB = String(b.symbol || '').toUpperCase();
        return symA.localeCompare(symB);
      });

      // ENQUEUE STARTUP BATCH TO NOTIFICATION QUEUE (instead of directly calling telegram)
      if (newSignals.length > 0) {
        logger.info({ newSignals: newSignals.length }, 'scanAllForStartup: enqueuing startup batch to notification queue');
        notificationQueue.enqueueStartupBatch(newSignals);
      } else {
        logger.info('scanAllForStartup: no new signals found');
      }

      startupComplete = true;
      logger.info('scanAllForStartup: completed full startup pass (USDT perpetuals only)');
    } catch (err) {
      logger.error({ err }, 'scanAllForStartup: unexpected error');
    }
  },

  async scanSymbolRoots(symbol) {
    const tfList = config.ROOT_TFS || [];
    const results = [];

    // Validate symbol is USDT or USDT.P
    const symUpper = String(symbol || '').toUpperCase();
    if (!/USDT(\.P)?$/.test(symUpper)) {
      logger.warn({ symbol }, 'scanSymbolRoots: symbol is not valid USDT, skipping');
      return results;
    }

    // Reject if it's a dated variant
    if (/USDT[QHUZ0-9]/.test(symUpper.slice(-6))) {
      logger.warn({ symbol }, 'scanSymbolRoots: symbol is a dated variant, skipping');
      return results;
    }

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
          } else {
            logger.info({ symbol, tf }, 'scanSymbolRoots: klines seeded and available, re-checking flip');
          }
        }

        const flip = await require('./macd').isMacdFlip(symbol, tf);
        if (flip) {
          const sig = await require('./signalManager').handleRootSignal({
            symbol,
            root_tf: tf,
            detected_at: Date.now(),
            notifyImmediately: false // Signal goes to queue, not immediate telegram
          });
          if (sig) results.push(sig);
        }
      } catch (err) {
        logger.debug({ err, symbol, tf }, 'scanSymbolRoots: error checking flip');
      }
    }

    return results;
  }
};
