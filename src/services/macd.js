const { MACD } = require('technicalindicators');
const dbModule = require('../db');
const logger = require('pino')();
const tradingview = require('./tradingview');
const marketData = require('./marketData');

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
    try {
      const hist = await this.computeMacdHistogram(symbol, timeframe);
      if (!hist || hist.length < 2) return false;
      const last = hist[hist.length - 1];
      const prev = hist[hist.length - 2];
      if (prev.histogram < 0 && last.histogram > 0) {
        logger.info({ symbol, timeframe, prev: prev.histogram, last: last.histogram }, 'MACD flip detected');
        return true;
      }
      return false;
    } catch (err) {
      logger.debug({ err, symbol, timeframe }, 'isMacdFlip error');
      return false;
    }
  },

  /**
   * getMtfStatus(symbol, tfs)
   * Returns status for multiple timeframes: { 5: 'POSITIVE'|'NEGATIVE'|'UNKNOWN', 15: ..., 60: ..., D: ... }
   */
  async getMtfStatus(symbol, tfs = []) {
    const status = {};
    for (const tf of tfs) {
      try {
        const hist = await this.computeMacdHistogram(symbol, tf);
        if (!hist || hist.length < 1) {
          status[tf] = 'UNKNOWN';
          continue;
        }
        const last = hist[hist.length - 1];
        if (last.histogram > 0) {
          status[tf] = 'POSITIVE';
        } else if (last.histogram < 0) {
          status[tf] = 'NEGATIVE';
        } else {
          status[tf] = 'UNKNOWN';
        }
      } catch (err) {
        logger.debug({ err, symbol, tf }, 'getMtfStatus error for timeframe');
        status[tf] = 'UNKNOWN';
      }
    }
    return status;
  },

  /**
   * getSignalMetrics(symbol, rootTf, mtfTfs)
   * Computes all metrics for a signal: TV score, MTF status, MACD values, market data
   */
  async getSignalMetrics(symbol, rootTf, mtfTfs = []) {
    try {
      // TV score
      let tvScore = 0;
      let tvSource = 'fallback';
      try {
        const tvResult = await tradingview.fetchTvRatingForSymbol(symbol);
        tvScore = tvResult.score || 0;
        tvSource = tvResult.source || 'fallback';
      } catch (e) {
        logger.debug({ e, symbol }, 'getSignalMetrics: TV score fetch failed, using fallback');
        tvScore = 0;
      }

      // MTF status
      const mtfStatus = await this.getMtfStatus(symbol, mtfTfs);

      // Root MACD histogram
      const rootHist = await this.computeMacdHistogram(symbol, rootTf);
      const rootMacd = rootHist && rootHist.length > 0 ? rootHist[rootHist.length - 1] : null;

      // Market data
      let marketDataResult = null;
      try {
        marketDataResult = await marketData.updateSymbolMarketData(symbol);
      } catch (e) {
        logger.debug({ e, symbol }, 'getSignalMetrics: market data update failed');
      }

      return {
        tv_score: tvScore,
        tv_source: tvSource,
        mtf_status: mtfStatus,
        root_macd: rootMacd ? {
          macd: rootMacd.MACD,
          signal: rootMacd.signal,
          histogram: rootMacd.histogram
        } : null,
        market_data: marketDataResult ? {
          price: marketDataResult.price,
          volume_24h_usdt: marketDataResult.volume_24h_usdt,
          volume_change_pct: marketDataResult.volume_change_pct,
          market_cap: marketDataResult.market_cap
        } : null
      };
    } catch (err) {
      logger.error({ err, symbol }, 'getSignalMetrics: unexpected error');
      return {
        tv_score: 0,
        tv_source: 'error',
        mtf_status: {},
        root_macd: null,
        market_data: null
      };
    }
  }
};
