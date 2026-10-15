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
    try {
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
    } catch (err) {
      logger.debug({ err, symbol, timeframe }, 'computeMacdHistogram error');
      return null;
    }
  },

  /**
   * FIXED: isMacdFlip - Only detect FIRST candle flips (negative->positive)
   * 
   * Prevents false positives from 2nd+ candle behavior.
   * Returns true ONLY if:
   *   - Previous candle histogram was negative (< 0)
   *   - Current candle histogram is positive (> 0)
   *   - This is a 1st candle flip (clean transition)
   */
  async isMacdFlip(symbol, timeframe) {
    try {
      const hist = await this.computeMacdHistogram(symbol, timeframe);
      if (!hist || hist.length < 2) return false;

      const last = hist[hist.length - 1];      // Current candle (most recent)
      const prev = hist[hist.length - 2];      // Previous candle

      // STRICT: negative -> positive (first candle flip only)
      // Prevents 2nd candle noise from triggering signals
      if (prev.histogram < 0 && last.histogram > 0) {
        logger.info(
          { symbol, timeframe, prevHistogram: prev.histogram, lastHistogram: last.histogram },
          'MACD flip detected (1st candle: negative->positive)'
        );
        return true;
      }

      return false;
    } catch (err) {
      logger.debug({ err, symbol, timeframe }, 'isMacdFlip error');
      return false;
    }
  },

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

  async getSignalMetrics(symbol, rootTf, mtfTfs = []) {
    try {
      let tvScore = 0;
      let tvSource = 'fallback';
      try {
        const tvResult = await tradingview.fetchTvRatingForSymbol(symbol);
        tvScore = tvResult && typeof tvResult.score === 'number' ? tvResult.score : 0;
        tvSource = tvResult && tvResult.source ? tvResult.source : 'error';
      } catch (e) {
        logger.debug({ e, symbol }, 'getSignalMetrics: TV score fetch failed, using fallback');
        tvScore = 0;
        tvSource = 'error';
      }

      const mtfStatus = await this.getMtfStatus(symbol, mtfTfs);

      const rootHist = await this.computeMacdHistogram(symbol, rootTf);
      const rootMacd = rootHist && rootHist.length > 0 ? rootHist[rootHist.length - 1] : null;

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
          price: marketDataResult.price || 0,
          volume_24h_usdt: marketDataResult.volume_24h_usdt || 0,
          volume_change_pct: marketDataResult.volume_change_pct || null,
          market_cap: marketDataResult.market_cap || null
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
  },

  /**
   * NEW: Check 7/14/28 simple moving averages on daily series and return true
   * if any point in the fetched history shows sma7 > sma14 > sma28.
   *
   * "new+old is fine" — so we search across the history provided by getKlineSeries.
   */
  async is1dMaStructure(symbol) {
    try {
      // fetch daily series; keep a reasonable lookback (200 daily candles)
      const series = await this.getKlineSeries(symbol, 'D', 200);
      if (!series || series.length < 28) return false;

      const closes = series.map(s => Number(s.close)).filter(c => Number.isFinite(c));
      const len = closes.length;
      if (len < 28) return false;

      // helper to compute SMA array for a given period
      function computeSMAArray(values, period) {
        const result = new Array(values.length).fill(null);
        let sum = 0;
        for (let i = 0; i < values.length; i++) {
          sum += values[i];
          if (i >= period) {
            sum -= values[i - period];
          }
          if (i >= period - 1) {
            result[i] = sum / period;
          }
        }
        return result;
      }

      const sma7 = computeSMAArray(closes, 7);
      const sma14 = computeSMAArray(closes, 14);
      const sma28 = computeSMAArray(closes, 28);

      // scan history - if any index satisfies sma7 > sma14 > sma28, return true
      for (let i = 27; i < len; i++) {
        const s7 = sma7[i];
        const s14 = sma14[i];
        const s28 = sma28[i];
        if (typeof s7 === 'number' && typeof s14 === 'number' && typeof s28 === 'number') {
          if (s7 > s14 && s14 > s28) {
            logger.debug({ symbol, index: i, sma7: s7, sma14: s14, sma28: s28 }, 'is1dMaStructure: upward 7/14/28 observed in history');
            return true;
          }
        }
      }

      // no upward 7/14/28 crossing observed
      return false;
    } catch (err) {
      logger.debug({ err, symbol }, 'is1dMaStructure error');
      return false;
    }
  }
};
