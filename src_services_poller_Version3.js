const dbModule = require('../db');
const bybit = require('./bybitRest');
const config = require('../config');
const logger = require('pino')();
const Bottleneck = require('bottleneck');
const macdUtil = require('./macd');
const marketData = require('./marketData');

const limiter = new Bottleneck({ minTime: 100 });

let isRunning = false;
module.exports = {
  start() {
    if (isRunning) return;
    isRunning = true;
    this.initialScan().catch(err => logger.error({ err }, 'initialScan failed'));
    if (config.ROOT_MIDSCAN_INTERVAL === 0) {
      this.scheduleAlignedTo5m();
    } else {
      setInterval(() => this.scanOnce(), config.ROOT_MIDSCAN_INTERVAL * 1000);
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
          const signalManager = require('./signalManager');
          signalManager.handleRootSignal({ symbol, root_tf: tf, detected_at: Date.now() });
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
    const schedule = async () => {
      const wait = msToNext5();
      logger.info({ wait }, 'Next aligned scan in ms');
      setTimeout(async () => {
        try {
          await this.scanOnce();
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