/**
 * src/services/poller.js
 *
 * - Fast initialScan: persist symbols quickly
 * - Background concurrent kline seeding for top N symbols
 * - Non-blocking probe on startup, open-trade gating until first aligned boundary
 */

const dbModule = require('../db');
const bybit = require('./bybitRest');
const config = require('../config');
const logger = require('pino')();
const Bottleneck = require('bottleneck');
const macdUtil = require('./macd');
const marketData = require('./marketData');
const signalManager = require('./signalManager');

const limiter = new Bottleneck({ minTime: 50 });

// Configurable environment values (defaults provided)
const SEED_TOP_SYMBOLS = Number(process.env.SEED_TOP_SYMBOLS || 100);
const SEED_CONCURRENCY = Number(process.env.SEED_CONCURRENCY || 6);
const SEED_KLINES_LIMIT = Number(process.env.SEED_KLINES_LIMIT || 200);

let isRunning = false;

module.exports = {
  start() {
    if (isRunning) return;
    isRunning = true;

    // Prevent opening trades until we've passed the next aligned 5m boundary.
    try { signalManager.setOpenTradesAllowed(false); } catch (e) { /* ignore */ }

    // Start host probe in background (do not block startup)
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

    // Run initialScan immediately (fast)
    (async () => {
      try {
        logger.info('poller: starting initialScan (deploy-time)');
        await this.initialScan();
        logger.info('poller: initialScan completed (fast symbol seed)');
      } catch (err) {
        logger.error({ err }, 'poller: initialScan failed');
      }
    })();

    // Scheduling
    if (config.ROOT_MIDSCAN_INTERVAL === 0) {
      this.scheduleAlignedTo5m();
    } else {
      setInterval(() => this.scanOnce(), config.ROOT_MIDSCAN_INTERVAL * 1000);
      // also enable open trades at next aligned boundary
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
          logger.info('Open trades enabled at boundary (interval mode)');
        } catch (e) { logger.debug({ e }, 'Failed to set open trades allowed'); }
      }, msToNext5());
    }
  },

  /**
   * initialScan:
   * - fetchAllSymbols (Bybit primary, filtered) and persist quickly
   * - schedule backgroundSeedKlines() to fetch klines concurrently for top N symbols
   */
  async initialScan() {
    logger.info('Starting initial symbol discovery (fast)');

    const all = await bybit.fetchAllSymbols();
    if (!Array.isArray(all) || all.length === 0) {
      logger.warn('poller.initialScan: fetchAllSymbols returned no symbols');
    }

    const filtered = (all || []).filter(s => s && s.symbol);
    const db = dbModule.get();
    const insert = db.prepare('INSERT OR REPLACE INTO symbols (symbol, base, quote, fetched_at) VALUES (?, ?, ?, ?)');
    const now = Date.now();
    const insertMany = db.transaction((rows) => {
      for (const s of rows) {
        insert.run(s.symbol, s.base || s.symbol.replace(/USDT$/i, ''), s.quote || 'USDT', now);
      }
    });
    insertMany(filtered);
    logger.info({ count: filtered.length }, 'Symbols saved (initialScan)');

    // start background seeding (non-blocking)
    setImmediate(() => this.backgroundSeedKlines(filtered));
  },

  /**
   * backgroundSeedKlines:
   * - seed klines concurrently for the top SEED_TOP_SYMBOLS symbols
   * - chunked batching with limiter to avoid bursts
   */
  async backgroundSeedKlines(allSymbols) {
    try {
      if (!Array.isArray(allSymbols) || allSymbols.length === 0) {
        logger.info('backgroundSeedKlines: no symbols to seed');
        return;
      }

      const toSeed = allSymbols.slice(0, SEED_TOP_SYMBOLS);
      logger.info({ count: toSeed.length, topN: SEED_TOP_SYMBOLS }, 'backgroundSeedKlines: starting seeding for top symbols');

      for (let i = 0; i < toSeed.length; i += SEED_CONCURRENCY) {
        const batch = toSeed.slice(i, i + SEED_CONCURRENCY);
        const jobs = batch.map(sym => limiter.schedule(() => this.seedKlinesForSymbol(sym.symbol)));
        await Promise.all(jobs);
      }

      logger.info('backgroundSeedKlines: completed top-N seeding');
    } catch (err) {
      logger.error({ err }, 'backgroundSeedKlines: error during seeding');
    }
  },

  /**
   * seedKlinesForSymbol(symbol, timeframe)
   * - attempts to fetch klines and store them; computes MACD but continues on failure
   */
  async seedKlinesForSymbol(symbol, timeframe) {
    const tfs = timeframe ? [timeframe] : (config.ROOT_TFS || []);
    for (const tf of tfs) {
      const interval = tf === 'D' ? 'D' : String(tf);
      try {
        const klines = await limiter.schedule(() => bybit.fetchKlines(symbol, interval, SEED_KLINES_LIMIT));
        if (!klines || klines.length === 0) continue;
        const db = dbModule.get();
        const insert = db.prepare('INSERT OR IGNORE INTO klines (symbol, timeframe, open_time, open, high, low, close, volume) VALUES (?, ?, ?, ?, ?, ?, ?, ?)');
        const insertMany = db.transaction((rows) => {
          for (const k of rows) insert.run(symbol, tf, k.open_time, k.open, k.high, k.low, k.close, k.volume);
        });
        insertMany(klines);
        try {
          await macdUtil.computeAndStoreMacd(symbol, tf);
        } catch (e) {
          logger.debug({ e, symbol, tf }, 'MACD compute failed for seed (continuing)');
        }
      } catch (err) {
        logger.debug({ err, symbol, tf }, 'seedKlinesForSymbol: failed to fetch klines (skipping)');
      }
    }
  },

  /**
   * scanOnce: iterate symbols and check for root signals
   */
  async scanOnce() {
    const db = dbModule.get();
    const rows = db.prepare('SELECT symbol FROM symbols ORDER BY symbol COLLATE NOCASE ASC').all();
    for (let i = 0; i < rows.length; i += config.PAGE_SIZE) {
      const page = rows.slice(i, i + config.PAGE_SIZE);
      await Promise.all(page.map(r => this.scanSymbolRoots(r.symbol)));
    }
  },

  async scanSymbolRoots(symbol) {
    const tfList = config.ROOT_TFS || [];
    for (const tf of tfList) {
      const db = dbModule.get();
      const rows = db.prepare('SELECT open_time, close, open FROM klines WHERE symbol=? AND timeframe=? ORDER BY open_time DESC LIMIT 2').all(symbol, tf);
      if (rows.length < 2) {
        // try to seed quickly for missing tf
        await this.seedKlinesForSymbol(symbol, tf);
        continue;
      }
      try {
        const flip = await require('./macd').isMacdFlip(symbol, tf);
        if (flip) {
          const signalManagerModule = require('./signalManager');
          signalManagerModule.handleRootSignal({ symbol, root_tf: tf, detected_at: Date.now() });
        }
      } catch (err) {
        logger.debug({ err, symbol, tf }, 'Error checking flip');
      }
    }
  },

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
      logger.info({ wait }, 'Next aligned scan in ms');
      setTimeout(async () => {
        try {
          await this.scanOnce();
          if (!firstBoundaryPassed) {
            firstBoundaryPassed = true;
            try {
              signalManager.setOpenTradesAllowed(true);
              logger.info('Open trades enabled at first aligned 5m boundary');
            } catch (e) {
              logger.debug({ e }, 'Failed to set open trades allowed after first boundary');
            }
          }
        } catch (err) {
          logger.error({ err }, 'aligned scan failed');
        } finally {
          schedule();
        }
      }, wait);
    };
    schedule();
  }
};
