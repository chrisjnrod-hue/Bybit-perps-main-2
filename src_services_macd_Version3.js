const { MACD } = require('technicalindicators');
const dbModule = require('../db');
const logger = require('pino')();

const macdOptions = { fastPeriod: 12, slowPeriod: 26, signalPeriod: 9, SimpleMAOscillator: false, SimpleMASignal: false };

module.exports = {
  async getKlineSeries(symbol, timeframe, limit = 500) {
    const db = dbModule.get();
    const rows = db.prepare('SELECT open_time, close FROM klines WHERE symbol=? AND timeframe=? ORDER BY open_time ASC LIMIT ?').all(symbol, timeframe, limit);
    return rows.map(r => ({ time: r.open_time, close: r.close }));
  },

  async computeMacdHistogram(symbol, timeframe) {
    const series = await this.getKlineSeries(symbol, timeframe, 500);
    const closes = series.map(s => s.close);
    if (closes.length < 35) return null;
    const macdInput = { values: closes, ...macdOptions };
    const out = MACD.calculate(macdInput);
    const offset = closes.length - out.length;
    const withTime = out.map((o, idx) => ({
      time: series[offset + idx].time,
      MACD: o.MACD,
      signal: o.signal,
      histogram: o.histogram
    }));
    return withTime;
  },

  async isMacdFlip(symbol, timeframe) {
    const hist = await this.computeMacdHistogram(symbol, timeframe);
    if (!hist || hist.length < 2) return false;
    const last = hist[hist.length - 1];
    const prev = hist[hist.length - 2];
    if (prev.histogram < 0 && last.histogram > 0) {
      logger.info({ symbol, timeframe, prev: prev.histogram, last: last.histogram }, 'MACD flip detected');
      return true;
    }
    return false;
  }
};