const { MACD } = require('technicalindicators');
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

function normalizeTimeframe(timeframe) {
  const tf = String(timeframe || '').trim();

  if (/^(D|1D|1d|day)$/i.test(tf)) return 'D';
  return tf;
}

function timeframeMs(timeframe) {
  const tf = normalizeTimeframe(timeframe);

  if (tf === 'D') return 24 * 60 * 60 * 1000;
  if (tf === 'W') return 7 * 24 * 60 * 60 * 1000;
  if (tf === 'M') return 30 * 24 * 60 * 60 * 1000;

  const minutes = Number(tf);
  return Number.isFinite(minutes) && minutes > 0
    ? minutes * 60 * 1000
    : null;
}

function currentCandleOpenTime(timeframe, now = Date.now()) {
  const duration = timeframeMs(timeframe);
  if (!duration) return null;
  return Math.floor(now / duration) * duration;
}

module.exports = {
  normalizeTimeframe,

  async getKlineSeries(symbol, timeframe, limit = 500) {
    const db = dbModule.get();
    const normalizedTf = normalizeTimeframe(timeframe);

    const rows = db.prepare(`
      SELECT open_time, close
      FROM (
        SELECT open_time, close
        FROM klines
        WHERE symbol = ? AND timeframe = ?
        ORDER BY open_time DESC
        LIMIT ?
      )
      ORDER BY open_time ASC
    `).all(symbol, normalizedTf, limit);

    return rows
      .map(row => ({
        time: Number(row.open_time),
        close: Number(row.close)
      }))
      .filter(row => Number.isFinite(row.time) && Number.isFinite(row.close));
  },

  async computeMacdHistogram(symbol, timeframe) {
    try {
      const normalizedTf = normalizeTimeframe(timeframe);
      const series = await this.getKlineSeries(symbol, normalizedTf, 500);

      const closes = series.map(item => item.close);
      if (closes.length < 35) return null;

      const output = MACD.calculate({
        values: closes,
        ...macdOptions
      });

      if (!Array.isArray(output) || output.length === 0) {
        return null;
      }

      const offset = closes.length - output.length;

      return output
        .map((item, index) => ({
          time: series[offset + index] ? series[offset + index].time : null,
          MACD: item.MACD,
          signal: item.signal,
          histogram: item.histogram
        }))
        .filter(item =>
          Number.isFinite(item.time) &&
          Number.isFinite(item.histogram)
        );
    } catch (err) {
      logger.debug({ err, symbol, timeframe }, 'computeMacdHistogram error');
      return null;
    }
  },

  /**
   * Returns the latest closed MACD histogram values.
   *
   * If the database contains the currently forming candle, it is excluded.
   * If the database contains only closed candles, the latest row is treated
   * as closed. This prevents the signal from being delayed by one candle.
   */
  async getClosedMacdHistogram(symbol, timeframe) {
    const normalizedTf = normalizeTimeframe(timeframe);
    const histogram = await this.computeMacdHistogram(symbol, normalizedTf);

    if (!histogram || histogram.length < 2) return null;

    const currentOpen = currentCandleOpenTime(normalizedTf);

    let closed = histogram;

    if (currentOpen !== null) {
      closed = histogram.filter(item => item.time < currentOpen);
    }

    if (closed.length < 2) {
      return null;
    }

    return closed;
  },

  /**
   * Detect a strict negative-to-positive transition on the latest closed
   * candle only.
   */
  async getMacdFlipEvent(symbol, timeframe) {
    try {
      const normalizedTf = normalizeTimeframe(timeframe);
      const closed = await this.getClosedMacdHistogram(symbol, normalizedTf);

      if (!closed || closed.length < 2) return null;

      const previous = closed[closed.length - 2];
      const latest = closed[closed.length - 1];

      if (
        previous.histogram < 0 &&
        latest.histogram > 0
      ) {
        const candleTime = Number(latest.time);

        return {
          symbol,
          timeframe: normalizedTf,
          candleTime,
          eventId: `${symbol}_${normalizedTf}_${candleTime}`,
          previousHistogram: previous.histogram,
          latestHistogram: latest.histogram
        };
      }

      return null;
    } catch (err) {
      logger.debug({ err, symbol, timeframe }, 'getMacdFlipEvent error');
      return null;
    }
  },

  async isMacdFlip(symbol, timeframe) {
    const event = await this.getMacdFlipEvent(symbol, timeframe);
    return Boolean(event);
  },

  /**
   * Loop 3 uses the same closed-candle transition, but poller.js ensures
   * that the event is evaluated only once when a new root candle opens.
   */
  async isMacdFlipAtOpen(symbol, timeframe) {
    const event = await this.getMacdFlipEvent(symbol, timeframe);
    return Boolean(event);
  },

  async getMtfStatus(symbol, tfs = []) {
    const status = {};

    for (const tf of tfs) {
      try {
        const histogram = await this.getClosedMacdHistogram(symbol, tf);

        if (!histogram || histogram.length === 0) {
          status[tf] = 'UNKNOWN';
          continue;
        }

        const latest = histogram[histogram.length - 1];

        if (latest.histogram > 0) {
          status[tf] = 'POSITIVE';
        } else if (latest.histogram < 0) {
          status[tf] = 'NEGATIVE';
        } else {
          status[tf] = 'UNKNOWN';
        }
      } catch (err) {
        logger.debug({ err, symbol, tf }, 'getMtfStatus error');
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
        tvScore = tvResult && typeof tvResult.score === 'number'
          ? tvResult.score
          : 0;
        tvSource = tvResult && tvResult.source
          ? tvResult.source
          : 'error';
      } catch (err) {
        logger.debug({ err, symbol }, 'getSignalMetrics: TV score fetch failed');
      }

      const mtfStatus = await this.getMtfStatus(symbol, mtfTfs);
      const rootHist = await this.getClosedMacdHistogram(symbol, rootTf);
      const rootMacd = rootHist && rootHist.length
        ? rootHist[rootHist.length - 1]
        : null;

      let marketDataResult = null;

      try {
        marketDataResult = await marketData.updateSymbolMarketData(symbol);
      } catch (err) {
        logger.debug({ err, symbol }, 'getSignalMetrics: market data update failed');
      }

      return {
        tv_score: tvScore,
        tv_source: tvSource,
        mtf_status: mtfStatus,
        root_macd: rootMacd
          ? {
              macd: rootMacd.MACD,
              signal: rootMacd.signal,
              histogram: rootMacd.histogram
            }
          : null,
        market_data: marketDataResult
          ? {
              price: marketDataResult.price || 0,
              volume_24h_usdt: marketDataResult.volume_24h_usdt || 0,
              volume_change_pct: marketDataResult.volume_change_pct || null,
              market_cap: marketDataResult.market_cap || null
            }
          : null
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
