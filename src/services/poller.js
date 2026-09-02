/**
 * src/services/poller.js
 *
 * UPDATED:
 * - Implements WS → REST fallback chain for initial symbol scan
 * - Cursor-based pagination fetches ALL symbols (no topN limiting)
 * - SYMBOL_SEED_ALL flag controls whether to seed all or none
 * - Fast initialScan followed by concurrent kline seeding
 * - Non-blocking probe on startup, open-trade gating until first aligned boundary
 */

const dbModule = require('../db');
const bybit = require('./bybitRest');
const config = require('../config');
const logger = require('pino')();
const Bottleneck = require('bottleneck');
const macdUtil = require('./macd');
const signalManager = require('./signalManager');

const limiter = new Bottleneck({ minTime: 50 });

// Configurable concurrency for kline seeding
const SEED_CONCURRENCY = Number(process.env.SEED_CONCURRENCY || 6);

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
        logger.info('poller: starting initialScan (deploy-time, WS → REST fallback)');
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
   * initialScan():
   * - Attempts WS initial scan with timeout (if USE_WS enabled)
   * - Falls back to REST API with cursor pagination for ALL symbols
   * - Persists symbols quickly to DB
   * - Schedules backgroundSeedKlines() for concurrent kline fetching
   */
  async initialScan() {
    logger.info('Starting initial symbol discovery (WS → REST fallback chain)');

    let allSymbols = [];

    // STEP 1: Try WS initial scan first (if enabled)
    const useWs = process.env.USE_WS === 'true';
    if (useWs) {
      try {
        const wsTimeoutMs = config.WS_INITIAL_SCAN_TIMEOUT || 10000;
        logger.info({ timeoutMs: wsTimeoutMs }, 'Attempting WS initial scan with timeout');

        allSymbols = await Promise.race([
          this.performWsInitialScan(),
          new Promise((_, reject) =>
            setTimeout(() => reject(new Error('WS scan timeout')), wsTimeoutMs)
          )
        ]);

        if (allSymbols && allSymbols.length > 0) {
          logger.info({ wsSymbols: allSymbols.length }, 'WS initial scan succeeded, using WS symbols');
        } else {
          logger.warn('WS scan returned empty; falling back to REST API');
          allSymbols = [];
        }
      } catch (e) {
        logger.warn({ err: e && e.message ? e.message : String(e) }, 'WS scan failed, falling back to REST API cursor pagination');
        allSymbols = [];
      }
    }

    // STEP 2: Fall back to REST API if WS didn't work
    if (!allSymbols || allSymbols.length === 0) {
      logger.info('Using REST API cursor pagination to fetch all symbols');
      allSymbols = await bybit.fetchAllSymbols();
    }

    if (!Array.isArray(allSymbols) || allSymbols.length === 0) {
      logger.warn('initialScan: no symbols obtained from WS or REST API');
      return;
    }

    // STEP 3: Apply seed strategy (SYMBOL_SEED_ALL controls whether to seed)
    const seedSymbols = bybit.getSeedSymbols(allSymbols);
    logger.info({ totalSymbols: allSymbols.length, seedSymbols: seedSymbols.length }, 'Symbol seeding strategy applied');

    // STEP 4: Persist ALL discovered symbols to DB (not just seed symbols)
    const filtered = (allSymbols || []).filter(s => s && s.symbol);
    const db = dbModule.get();
    const insert = db.prepare('INSERT OR REPLACE INTO symbols (symbol, base, quote, fetched_at) VALUES (?, ?, ?, ?)');
    const now = Date.now();
    const insertMany = db.transaction((rows) => {
      for (const s of rows) {
        insert.run(s.symbol, s.base || s.symbol.replace(/USDT(\.P)?$/i, ''), s.quote || 'USDT', now);
      }
    });
    insertMany(filtered);
    logger.info({ count: filtered.length }, 'All discovered symbols saved to DB');

    // STEP 5: Start background seeding for SEED symbols only (non-blocking)
    if (seedSymbols && seedSymbols.length > 0) {
      setImmediate(() => this.backgroundSeedKlines(seedSymbols));
    } else {
      logger.info('No symbols to seed (SYMBOL_SEED_ALL disabled or empty seed list)');
    }
  },

  /**
   * performWsInitialScan():
   * Placeholder for WS-based initial scan (if wsManager has this capability)
   * Falls back to empty array if method doesn't exist or throws
   */
  async performWsInitialScan() {
    try {
      const wsManager = require('./bybitWs');
      if (typeof wsManager.performInitialScan === 'function') {
        logger.info('Calling wsManager.performInitialScan()');
        const result = await wsManager.performInitialScan();
        return Array.isArray(result) ? result : [];
      } else {
        logger.debug('wsManager.performInitialScan not implemented; skipping WS scan');
        return [];
      }
    } catch (e) {
      logger.debug({ err: e && e.message ? e.message : String(e) }, 'performWsInitialScan threw exception');
      return [];
    }
  },

  /**
   * backgroundSeedKlines():
   * - Concurrently seeds klines for all provided symbols (respects SYMBOL_SEED_ALL decision)
   * - Uses limiter and batching to avoid overwhelming API
   * - Non-blocking (runs in background)
   */
  async backgroundSeedKlines(symbols) {
    try {
      if (!Array.isArray(symbols) || symbols.length === 0) {
        logger.info('backgroundSeedKlines: no symbols to seed');
        return;
      }

      logger.info({ count: symbols.length, concurrency: SEED_CONCURRENCY }, 'backgroundSeedKlines: starting seeding for symbols');

      // Process symbols in batches
      for (let i = 0; i < symbols.length; i += SEED_CONCURRENCY) {
        const batch = symbols.slice(i, i + SEED_CONCURRENCY);
        const jobs = batch.map(sym =>
          limiter.schedule(() => this.seedKlinesForSymbol(sym.symbol))
        );
        await Promise.all(jobs);
      }

      logger.info({ totalSeeded: symbols.length }, 'backgroundSeedKlines: completed seeding');
    } catch (err) {
      logger.error({ err }, 'backgroundSeedKlines: error during seeding');
    }
  },

  /**
   * seedKlinesForSymbol(symbol, timeframe):
   * - Fetches klines for symbol across ROOT_TFS
   * - Stores to DB
   * - Computes MACD but continues on failure
   */
  async seedKlinesForSymbol(symbol, timeframe) {
    const tfs = timeframe ? [timeframe] : (config.ROOT_TFS || []);
    for (const tf of tfs) {
      const interval = tf === 'D' ? 'D' : String(tf);
      try {
        const klines = await limiter.schedule(() =>
          bybit.fetchKlines(symbol, interval, config.SEED_KLINES_LIMIT)
        );
        if (!klines || klines.length === 0) continue;

        const db = dbModule.get();
        const insert = db.prepare(
          'INSERT OR IGNORE INTO klines (symbol, timeframe, open_time, open, high, low, close, volume) VALUES (?, ?, ?, ?, ?, ?, ?, ?)'
        );
        const insertMany = db.transaction((rows) => {
          for (const k of rows) {
            insert.run(symbol, tf, k.open_time, k.open, k.high, k.low, k.close, k.volume);
          }
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
   * scanOnce(): iterate symbols and check for root signals
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
      const rows = db.prepare(
        'SELECT open_time, close, open FROM klines WHERE symbol=? AND timeframe=? ORDER BY open_time DESC LIMIT 2'
      ).all(symbol, tf);
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
