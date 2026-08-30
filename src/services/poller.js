const dbModule = require('../db');
const bybit = require('./bybitRest');
const config = require('../config');
const logger = require('pino')();
const Bottleneck = require('bottleneck');
const macdUtil = require('./macd');
const marketData = require('./marketData');
const signalManager = require('./signalManager'); // used to control opening trades

const limiter = new Bottleneck({ minTime: 100 });

let isRunning = false;
module.exports = {
  start() {
    if (isRunning) return;
    isRunning = true;

    // Prevent opening trades until we've passed the next aligned 5m boundary.
    try { signalManager.setOpenTradesAllowed(false); } catch (e) { /* ignore */ }

    // Run initial seed/scan flow immediately (non-blocking).
    this.initialScan()
      .then(async () => {
        try {
          // Immediately run one scan now (will send signals but NOT open trades due to flag above)
          await this.scanOnce();
        } catch (err) {
          logger.error({ err }, 'Immediate scanOnce after initialSeed failed');
        }
      })
      .catch(err => logger.error({ err }, 'initialScan failed'));

    // Scheduling: either aligned-to-5m or interval-based.
    if (config.ROOT_MIDSCAN_INTERVAL === 0) {
      this.scheduleAlignedTo5m();
    } else {
      // Interval path: run scan every ROOT_MIDSCAN_INTERVAL seconds
      setInterval(() => this.scanOnce(), config.ROOT_MIDSCAN_INTERVAL * 1000);

      // Also ensure opening trades is enabled at the next 5-minute boundary
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

  async initialScan() {
    logger.info('Starting initial symbol discovery and seeding root TF klines');
    const all = await bybit.fetchAllSymbols();
    const usdt = all.filter(s => (s.symbol || '').toUpperCase().endsWith('USDT'));
    const filtered = usdt.filter(s => !config.EXCLUDE_STABLES.some(st => (s.symbol || '').includes(st)));
    const db = dbModule.get();
    const insert = db.prepare('INSERT OR REPLACE INTO symbols (symbol, base, quote, fetched_at) VALUES (?, ?, ?, ?)');
    const now = Date.now();
    for (const s of filtered) {
      insert.run(s.symbol, s.base || s.symbol.replace(/USDT$/, ''), s.quote || 'USDT', now);
    }
    logger.info({ count: filtered.length }, 'Symbols saved');

    for (const s of filtered) {
      try {
        await marketData.updateSymbolMarketData(s.symbol);
      } catch (err) {
        logger.debug({ err, symbol: s.symbol }, 'marketData update failed during initial seed');
      }
    }

    for (const s of filtered) {
      for (const tf of config.ROOT_TFS) {
        await this.seedKlinesForSymbol(s.symbol, tf);
      }
    }
    logger.info('Initial seeding complete');
  },

  async seedKlinesForSymbol(symbol, timeframe) {
    const interval = timeframe === 'D' ? 'D' : String(timeframe);
    const klines = await limiter.schedule(() => bybit.fetchKlines(symbol, interval, config.SEED_KLINES_LIMIT));
    if (!klines || klines.length === 0) return;
    const db = dbModule.get();
    const insert = db.prepare('INSERT OR IGNORE INTO klines (symbol, timeframe, open_time, open, high, low, close, volume) VALUES (?, ?, ?, ?, ?, ?, ?, ?)');
    const insertMany = db.transaction((rows) => {
      for (const k of rows) insert.run(symbol, timeframe, k.open_time, k.open, k.high, k.low, k.close, k.volume);
    });
    insertMany(klines);
    try {
      await macdUtil.computeAndStoreMacd(symbol, timeframe);
    } catch (err) {
      logger.debug({ err }, 'macd compute on seed failed');
    }
  },

  async scanOnce() {
    const db = dbModule.get();
    const rows = db.prepare('SELECT symbol FROM symbols ORDER BY symbol COLLATE NOCASE ASC').all();
    for (let i = 0; i < rows.length; i += config.PAGE_SIZE) {
      const page = rows.slice(i, i + config.PAGE_SIZE);
      await Promise.all(page.map(r => this.scanSymbolRoots(r.symbol)));
    }
  },

  async scanSymbolRoots(symbol) {
    const tfList = config.ROOT_TFS;
    for (const tf of tfList) {
      const db = dbModule.get();
      const rows = db.prepare('SELECT open_time, close, open FROM klines WHERE symbol=? AND timeframe=? ORDER BY open_time DESC LIMIT 2').all(symbol, tf);
      if (rows.length < 2) {
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
          // After the first aligned scan has run, enable openings
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
