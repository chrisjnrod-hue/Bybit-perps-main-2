const { MACD, SMA } = require('technicalindicators');
const dbModule = require('../db');
const logger = require('pino')();
const tradingview = require('./tradingview');
const marketData = require('./marketData');

const macdOptions = {
  fastPeriod: 12,
  slowPeriod: 26,
  signalPeriod: 9,
  SimpleMAOscillator: false,
  SimpleMASignal: false
};

module.exports = {
  async getKlineSeries(symbol, timeframe, limit = 500) {
    const db = dbModule.get();
    const rows = db.prepare(
      'SELECT open_time, close FROM klines WHERE symbol=? AND timeframe=? ORDER BY open_time ASC LIMIT ?'
    ).all(symbol, timeframe, limit);

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

      const last = hist[hist.length - 1];
      const prev = hist[hist.length - 2];

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
   * NEW: is1dMaStructure
   *
   * This is the list of 1D criteria to be used for signal acceptance.
   * It is applied to all root TFs, not just 1D.
   *
   * Returns true if ANY of these are true:
   *  - Positive MACD histogram
   *  - Negative but rising MACD histogram
   *  - Current candle MACD flip (negative->positive)
   *  - Structural MA order: SMA(7) > SMA(14) > SMA(28), anywhere in history or latest
   *
   * This means "old+new" crossings count; it does not require the crossing to be recent only.
   */
  async is1dMaStructure(symbol) {
    try {
      // MACD checks first
      const hist = await this.computeMacdHistogram(symbol, 'D');
      if (hist && hist.length >= 1) {
        const last = hist[hist.length - 1];
        const prev = hist.length >= 2 ? hist[hist.length - 2] : null;

        // Positive histogram passes
        if (typeof last.histogram === 'number' && last.histogram > 0) {
          logger.debug({ symbol }, 'is1dMaStructure: positive MACD histogram detected');
          return true;
        }

        // Negative but rising passes
        if (prev && typeof prev.histogram === 'number' && typeof last.histogram === 'number') {
          if (last.histogram < 0 && last.histogram > prev.histogram) {
            logger.debug({ symbol }, 'is1dMaStructure: negative-but-rising MACD histogram detected');
            return true;
          }
        }

        // MACD flip passes
        try {
          const flip = await this.isMacdFlip(symbol, 'D');
          if (flip) {
            logger.debug({ symbol }, 'is1dMaStructure: MACD flip detected');
            return true;
          }
        } catch (e) {
          logger.debug({ e, symbol }, 'is1dMaStructure: isMacdFlip threw error');
        }
      }

      // SMA structural check against daily closes
      const series = await this.getKlineSeries(symbol, 'D', 400);
      const closes = series.map(s => s.close);

      if (!closes || closes.length < 28) {
        logger.debug({ symbol, length: closes.length }, 'is1dMaStructure: not enough candles for SMA check');
        return false;
      }

      const sma7 = SMA.calculate({ period: 7, values: closes });
      const sma14 = SMA.calculate({ period: 14, values: closes });
      const sma28 = SMA.calculate({ period: 28, values: closes });

      if (!sma28 || sma28.length === 0) {
        return false;
      }

      const offset7 = sma7.length - sma28.length;
      const offset14 = sma14.length - sma28.length;

      // Check any historical or latest aligned index
      for (let i = 0; i < sma28.length; i++) {
        const a7 = sma7[offset7 + i];
        const a14 = sma14[offset14 + i];
        const a28 = sma28[i];

        if (typeof a7 === 'number' && typeof a14 === 'number' && typeof a28 === 'number') {
          if (a7 > a14 && a14 > a28) {
            logger.debug({ symbol, idx: i }, 'is1dMaStructure: SMA7 > SMA14 > SMA28 detected');
            return true;
          }
        }
      }

      const latest7 = sma7[sma7.length - 1];
      const latest14 = sma14[sma14.length - 1];
      const latest28 = sma28[sma28.length - 1];

      if (
        typeof latest7 === 'number' &&
        typeof latest14 === 'number' &&
        typeof latest28 === 'number'
      ) {
        if (latest7 > latest14 && latest14 > latest28) {
          logger.debug({ symbol }, 'is1dMaStructure: latest SMA7 > SMA14 > SMA28 detected');
          return true;
        }
      }

      return false;
    } catch (err) {
      logger.debug({ err, symbol }, 'is1dMaStructure error');
      return false;
    }
  },

  /**
   * Basic internal technical rating aggregator.
   * Returns numeric 0-100.
   */
  async computeTechnicalRating(symbol, timeframe) {
    try {
      const metrics = await this.getSignalMetrics(symbol, timeframe, []);
      let score = 0;

      const tv = Number(metrics.tv_score || 0);
      score += Math.max(0, Math.min(100, tv)) * 0.4;

      const rootMacd = metrics.root_macd;
      if (rootMacd && typeof rootMacd.histogram === 'number') {
        score += (rootMacd.histogram > 0 ? 20 : 0);
      }

      const mtf = metrics.mtf_status || {};
      const total = Object.keys(mtf).length || 0;
      if (total > 0) {
        const pos = Object.values(mtf).filter(v => v === 'POSITIVE').length;
        score += (pos / total) * 40;
      }

      return Math.round(Math.max(0, Math.min(100, score)));
    } catch (err) {
      logger.debug({ err, symbol, timeframe }, 'computeTechnicalRating error');
      return 0;
    }
  }
};
