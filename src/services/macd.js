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

function toFiniteNumber(value) {
  const number = Number(value);

  return Number.isFinite(number)
    ? number
    : null;
}

module.exports = {
  async getKlineSeries(
    symbol,
    timeframe,
    limit = 500
  ) {
    const db = dbModule.get();

    const rows = db
      .prepare(
        `
          SELECT open_time, close
          FROM (
            SELECT open_time, close
            FROM klines
            WHERE symbol = ?
              AND timeframe = ?
            ORDER BY open_time DESC
            LIMIT ?
          )
          ORDER BY open_time ASC
        `
      )
      .all(symbol, timeframe, limit);

    return rows
      .map((row) => ({
        time: Number(row.open_time),
        close: toFiniteNumber(row.close)
      }))
      .filter((row) => {
        return (
          Number.isFinite(row.time) &&
          row.close !== null
        );
      });
  },

  async computeMacdHistogram(symbol, timeframe) {
    try {
      const series = await this.getKlineSeries(
        symbol,
        timeframe,
        500
      );

      if (series.length < 35) {
        return null;
      }

      const values = series.map((item) => item.close);

      const output = MACD.calculate({
        values,
        ...macdOptions
      });

      if (!Array.isArray(output) || output.length === 0) {
        return null;
      }

      const offset = series.length - output.length;

      return output
        .map((item, index) => {
          const source = series[offset + index];

          return {
            time: source.time,
            MACD: toFiniteNumber(item.MACD),
            signal: toFiniteNumber(item.signal),
            histogram: toFiniteNumber(item.histogram)
          };
        })
        .filter((item) => {
          return (
            item.MACD !== null &&
            item.signal !== null &&
            item.histogram !== null
          );
        });
    } catch (err) {
      logger.debug(
        {
          err,
          symbol,
          timeframe
        },
        'macd.computeMacdHistogram failed'
      );

      return null;
    }
  },

  /*
   * Retained for compatibility.
   * Evaluates the latest two available histogram values.
   */
  async isMacdFlip(symbol, timeframe) {
    try {
      const histogram =
        await this.computeMacdHistogram(
          symbol,
          timeframe
        );

      if (!histogram || histogram.length < 2) {
        return false;
      }

      const previous =
        histogram[histogram.length - 2];

      const current =
        histogram[histogram.length - 1];

      const flipped =
        previous.histogram < 0 &&
        current.histogram > 0;

      if (flipped) {
        logger.info(
          {
            symbol,
            timeframe,
            previousHistogram: previous.histogram,
            currentHistogram: current.histogram
          },
          'macd: negative-to-positive flip detected'
        );
      }

      return flipped;
    } catch (err) {
      logger.debug(
        {
          err,
          symbol,
          timeframe
        },
        'macd.isMacdFlip failed'
      );

      return false;
    }
  },

  /*
   * Used by the five-minute boundary scan.
   *
   * When a new root candle opens, the candle that should be
   * evaluated is the previous candle, because that candle has
   * just closed. This prevents evaluation of the still-forming
   * current candle.
   */
  async isMacdFlipAtClosedCandle(
    symbol,
    timeframe,
    closedOpenTime
  ) {
    try {
      const histogram =
        await this.computeMacdHistogram(
          symbol,
          timeframe
        );

      if (!histogram || histogram.length < 2) {
        return false;
      }

      const targetTime = Number(closedOpenTime);

      const currentIndex = histogram.findIndex(
        (item) => Number(item.time) === targetTime
      );

      if (currentIndex < 1) {
        logger.debug(
          {
            symbol,
            timeframe,
            closedOpenTime
          },
          'macd: closed candle not present in histogram'
        );

        return false;
      }

      const previous =
        histogram[currentIndex - 1];

      const current =
        histogram[currentIndex];

      const flipped =
        previous.histogram < 0 &&
        current.histogram > 0;

      if (flipped) {
        logger.info(
          {
            symbol,
            timeframe,
            closedOpenTime,
            previousHistogram: previous.histogram,
            currentHistogram: current.histogram
          },
          'macd: flip detected on closed candle'
        );
      }

      return flipped;
    } catch (err) {
      logger.debug(
        {
          err,
          symbol,
          timeframe,
          closedOpenTime
        },
        'macd.isMacdFlipAtClosedCandle failed'
      );

      return false;
    }
  },

  async getMtfStatus(symbol, timeframes = []) {
    const status = {};

    for (const timeframe of timeframes) {
      try {
        const histogram =
          await this.computeMacdHistogram(
            symbol,
            timeframe
          );

        if (!histogram || histogram.length === 0) {
          status[timeframe] = 'UNKNOWN';
          continue;
        }

        const last =
          histogram[histogram.length - 1];

        if (last.histogram > 0) {
          status[timeframe] = 'POSITIVE';
        } else if (last.histogram < 0) {
          status[timeframe] = 'NEGATIVE';
        } else {
          status[timeframe] = 'UNKNOWN';
        }
      } catch (err) {
        logger.debug(
          {
            err,
            symbol,
            timeframe
          },
          'macd.getMtfStatus failed'
        );

        status[timeframe] = 'UNKNOWN';
      }
    }

    return status;
  },

  async getSignalMetrics(
    symbol,
    rootTf,
    mtfTfs = []
  ) {
    try {
      let tvScore = 0;
      let tvSource = 'fallback';

      try {
        const result =
          await tradingview.fetchTvRatingForSymbol(
            symbol
          );

        tvScore =
          result && typeof result.score === 'number'
            ? result.score
            : 0;

        tvSource =
          result && result.source
            ? result.source
            : 'error';
      } catch (err) {
        logger.debug(
          {
            err,
            symbol
          },
          'macd.getSignalMetrics: TradingView failed'
        );

        tvSource = 'error';
      }

      const mtfStatus =
        await this.getMtfStatus(symbol, mtfTfs);

      const rootHistogram =
        await this.computeMacdHistogram(
          symbol,
          rootTf
        );

      const rootMacd =
        rootHistogram &&
        rootHistogram.length > 0
          ? rootHistogram[rootHistogram.length - 1]
          : null;

      let marketDataResult = null;

      try {
        marketDataResult =
          await marketData.updateSymbolMarketData(
            symbol
          );
      } catch (err) {
        logger.debug(
          {
            err,
            symbol
          },
          'macd.getSignalMetrics: market data update failed'
        );
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
              volume_24h_usdt:
                marketDataResult.volume_24h_usdt || 0,
              volume_change_pct:
                marketDataResult.volume_change_pct || null,
              market_cap:
                marketDataResult.market_cap || null
            }
          : null
      };
    } catch (err) {
      logger.error(
        {
          err,
          symbol
        },
        'macd.getSignalMetrics failed'
      );

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
