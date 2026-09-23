const dbModule = require('../db');
const wsManager = require('./bybitWs');
const macd = require('./macd');
const tradeManager = require('./tradeManager');
const marketData = require('./marketData');
const tradingview = require('./tradingview');
const notificationQueue = require('./notificationQueue');
const config = require('../config');
const logger = require('pino')();

let openTradesAllowed = true;

/*
 * This map is only a concurrency guard.
 *
 * It must not suppress a symbol/timeframe for an hour. Once the current
 * signal-processing operation finishes, its key is removed immediately.
 */
const inProgress = new Map();

function setOpenTradesAllowed(value) {
  openTradesAllowed = !!value;

  logger.info(
    { openTradesAllowed },
    'signalManager: openTradesAllowed set'
  );
}

function getProcessingKey({
  symbol,
  root_tf,
  candle_open_time
}) {
  const candlePart =
    candle_open_time !== null &&
    candle_open_time !== undefined
      ? String(candle_open_time)
      : 'live';

  return `${symbol}:${root_tf}:${candlePart}`;
}

function normalizeScore(value, fallback = 0) {
  const number = Number(value);

  return Number.isFinite(number)
    ? number
    : fallback;
}

module.exports = {
  start() {
    logger.info('SignalManager started');
  },

  setOpenTradesAllowed,

  /**
   * Process a root-timeframe MACD signal.
   *
   * notifyImmediately:
   * - true: enqueue the completed signal for realtime Telegram delivery
   * - false: return the completed signal to the caller, which can later
   *   enqueue it as part of a startup or root-candle summary
   */
  async handleRootSignal({
    symbol,
    root_tf,
    detected_at = Date.now(),
    candle_open_time = null,
    notifyImmediately = true,
    notificationType = 'signal'
  } = {}) {
    if (!symbol || !root_tf) {
      logger.warn(
        { symbol, root_tf },
        'handleRootSignal: missing symbol or root timeframe'
      );

      return null;
    }

    const key = getProcessingKey({
      symbol,
      root_tf,
      candle_open_time
    });

    if (inProgress.has(key)) {
      logger.debug(
        { key, symbol, root_tf, candle_open_time },
        'handleRootSignal: identical signal is already being processed'
      );

      return null;
    }

    inProgress.set(key, true);

    try {
      logger.info(
        {
          symbol,
          root_tf,
          candle_open_time,
          notificationType,
          notifyImmediately
        },
        'Root signal received'
      );

      let marketDataResult = null;

      try {
        marketDataResult =
          await marketData.updateSymbolMarketData(symbol);

        if (!marketDataResult) {
          marketDataResult = {
            price: 0,
            volume_24h_usdt: 0,
            volume_change_pct: null,
            market_cap: null
          };
        }
      } catch (err) {
        logger.warn(
          {
            err,
            symbol
          },
          'handleRootSignal: market data fetch failed; using fallback values'
        );

        marketDataResult = {
          price: 0,
          volume_24h_usdt: 0,
          volume_change_pct: null,
          market_cap: null
        };
      }

      let tv = {
        score: 0,
        source: 'error'
      };

      try {
        logger.debug(
          { symbol },
          'handleRootSignal: fetching TV rating'
        );

        const tvResult =
          await tradingview.getOrFetchTvRatingCached(symbol);

        if (
          tvResult &&
          typeof tvResult.score === 'number' &&
          Number.isFinite(tvResult.score)
        ) {
          tv = {
            score: tvResult.score,
            source: tvResult.source || 'unknown'
          };

          logger.info(
            {
              symbol,
              score: tv.score,
              source: tv.source
            },
            'TV rating acquired'
          );
        } else {
          logger.warn(
            { symbol, tvResult },
            'TV rating fetch returned invalid result; using zero'
          );
        }
      } catch (err) {
        logger.warn(
          {
            err: err && err.message
              ? err.message
              : err,
            symbol
          },
          'handleRootSignal: TV rating fetch failed; using zero'
        );
      }

      try {
        if (
          wsManager &&
          typeof wsManager.subscribeSymbolMTF === 'function'
        ) {
          wsManager.subscribeSymbolMTF(
            symbol,
            Array.isArray(config.MTF_TFS)
              ? config.MTF_TFS
              : []
          );
        }
      } catch (err) {
        logger.debug(
          {
            err,
            symbol
          },
          'handleRootSignal: failed to subscribe symbol MTF data'
        );
      }

      const alignment =
        await this.evaluateMtfAlignment(symbol);

      const alignmentTimeframes =
        Object.keys(alignment || {});

      const positiveCount =
        alignmentTimeframes.reduce(
          (count, timeframe) => {
            return count +
              (
                alignment[timeframe] &&
                alignment[timeframe].positive
                  ? 1
                  : 0
              );
          },
          0
        );

      const mtfScore =
        alignmentTimeframes.length > 0
          ? positiveCount / alignmentTimeframes.length
          : 0;

      const decision =
        await this.applyDecision(alignment);

      const meta = {
        tvScore: normalizeScore(tv.score),
        tvSource: tv.source || 'error',
        mtfScore,
        alignment,
        acceptReason: decision?.reason || null,
        decision: decision?.decision || 'monitor',
        marketData: marketDataResult || {}
      };

      dbModule.insertSignal({
        symbol,
        root_tf,
        detected_at,
        candle_open_time,
        state: 'detected',
        meta
      });

      const signalObj = {
        key,
        symbol,
        root_tf,
        candle_open_time,
        detected_at,
        state: 'detected',
        meta,
        notificationType
      };

      if (notifyImmediately) {
        logger.debug(
          {
            symbol,
            root_tf,
            candle_open_time,
            notificationType
          },
          'handleRootSignal: enqueueing realtime signal'
        );

        notificationQueue.enqueueSignal(
          signalObj,
          'realtime'
        );
      } else {
        logger.debug(
          {
            symbol,
            root_tf,
            candle_open_time,
            notificationType
          },
          'handleRootSignal: returning signal for batch notification'
        );
      }

      /*
       * Do not return early when notifyImmediately is false.
       *
       * Boundary scans use notifyImmediately=false so the signal can be
       * included in the "New Root Candle Open" summary. Trade-opening logic
       * must still run afterward.
       */
      if (
        decision &&
        decision.decision === 'accept'
      ) {
        if (!config.OPENTRADE) {
          logger.info(
            { symbol },
            'Accepted signal but OPENTRADE is disabled'
          );
        } else if (!openTradesAllowed) {
          logger.info(
            { symbol },
            'Accepted signal but opening trades is currently disabled'
          );
        } else {
          let passesMarketFilters = true;

          const minMarketCap =
            Number(config.MIN_MARKET_CAP) || 0;

          if (minMarketCap > 0) {
            const marketCap =
              Number(marketDataResult?.market_cap);

            if (
              !Number.isFinite(marketCap) ||
              marketCap < minMarketCap
            ) {
              passesMarketFilters = false;

              logger.info(
                {
                  symbol,
                  market_cap: marketDataResult?.market_cap,
                  minimum: minMarketCap
                },
                'Signal filtered by MIN_MARKET_CAP for trade opening'
              );
            }
          }

          const minVolume =
            Number(config.MIN_24H_USDT_VOLUME) || 0;

          if (minVolume > 0) {
            const volume =
              Number(marketDataResult?.volume_24h_usdt);

            if (
              !Number.isFinite(volume) ||
              volume < minVolume
            ) {
              passesMarketFilters = false;

              logger.info(
                {
                  symbol,
                  volume_24h_usdt: marketDataResult?.volume_24h_usdt,
                  minimum: minVolume
                },
                'Signal filtered by MIN_24H_USDT_VOLUME for trade opening'
              );
            }
          }

          const minVolumeChange = Number(
            config.MIN_24H_VOLUME_CHANGE_PCT
          );

          if (Number.isFinite(minVolumeChange)) {
            const volumeChange =
              marketDataResult?.volume_change_pct;

            if (
              volumeChange === null ||
              volumeChange === undefined
            ) {
              if (minVolumeChange > 0) {
                passesMarketFilters = false;

                logger.info(
                  { symbol },
                  'Signal filtered because volume change is unavailable'
                );
              }
            } else if (
              Number(volumeChange) < minVolumeChange
            ) {
              passesMarketFilters = false;

              logger.info(
                {
                  symbol,
                  volume_change_pct: volumeChange,
                  minimum: minVolumeChange
                },
                'Signal filtered by MIN_24H_VOLUME_CHANGE_PCT for trade opening'
              );
            }
          }

          if (passesMarketFilters) {
            try {
              await tradeManager.openTrade({
                symbol,
                root_tf,
                alignment,
                meta
              });

              logger.info(
                {
                  symbol,
                  root_tf,
                  candle_open_time
                },
                'handleRootSignal: trade opening initiated'
              );
            } catch (err) {
              logger.error(
                {
                  err,
                  symbol,
                  root_tf
                },
                'handleRootSignal: trade opening failed'
              );
            }
          } else {
            logger.info(
              { symbol },
              'Accepted signal but market filters prevented trade opening'
            );
          }
        }
      }

      return signalObj;
    } catch (err) {
      logger.error(
        {
          err,
          symbol,
          root_tf,
          candle_open_time
        },
        'handleRootSignal: unexpected error'
      );

      return null;
    } finally {
      /*
       * This is a concurrency lock only.
       *
       * Removing it immediately allows a later root candle to generate a
       * new signal. The notification queue separately deduplicates the same
       * candle using candle_open_time.
       */
      inProgress.delete(key);
    }
  },

  async evaluateMtfAlignment(symbol) {
    const result = {};

    const timeframes = Array.isArray(config.MTF_TFS)
      ? config.MTF_TFS
      : [];

    for (const timeframe of timeframes) {
      const tf = String(timeframe);

      try {
        const histogram =
          await macd.computeMacdHistogram(symbol, tf);

        if (
          !Array.isArray(histogram) ||
          histogram.length === 0
        ) {
          result[tf] = {
            ok: false,
            positive: false
          };

          continue;
        }

        const last =
          histogram[histogram.length - 1];

        const previous =
          histogram[histogram.length - 2] || last;

        const lastHistogram =
          Number(last?.histogram);

        const previousHistogram =
          Number(previous?.histogram);

        const validLastHistogram =
          Number.isFinite(lastHistogram);

        const validPreviousHistogram =
          Number.isFinite(previousHistogram);

        result[tf] = {
          histogram: validLastHistogram
            ? lastHistogram
            : null,
          macd: last?.MACD ?? null,
          signal: last?.signal ?? null,
          rising:
            validLastHistogram &&
            validPreviousHistogram
              ? lastHistogram > previousHistogram
              : false,
          positive:
            validLastHistogram
              ? lastHistogram > 0
              : false,
          ok: validLastHistogram
        };
      } catch (err) {
        logger.debug(
          {
            err,
            symbol,
            tf
          },
          'evaluateMtfAlignment: timeframe evaluation failed'
        );

        result[tf] = {
          ok: false,
          positive: false
        };
      }
    }

    return result;
  },

  async applyDecision(alignment = {}) {
    const timeframes =
      alignment && typeof alignment === 'object'
        ? Object.keys(alignment)
        : [];

    if (timeframes.length === 0) {
      return {
        decision: 'reject',
        reason: 'no_mtf_data'
      };
    }

    const allPositive =
      timeframes.every((timeframe) => {
        return (
          alignment[timeframe] &&
          alignment[timeframe].positive
        );
      });

    if (allPositive) {
      return {
        decision: 'accept',
        reason: 'all_positive'
      };
    }

    const negativeTimeframes =
      timeframes.filter((timeframe) => {
        return (
          alignment[timeframe] &&
          !alignment[timeframe].positive
        );
      });

    const onlyNegativeTimeframeIsDaily =
      negativeTimeframes.length === 1 &&
      negativeTimeframes[0].toUpperCase() === 'D';

    if (onlyNegativeTimeframeIsDaily) {
      const daily =
        alignment[negativeTimeframes[0]];

      if (daily && daily.rising) {
        return {
          decision: 'accept',
          reason: 'daily_rising'
        };
      }

      return {
        decision: 'monitor',
        reason: 'daily_not_rising'
      };
    }

    if (negativeTimeframes.length >= 1) {
      return {
        decision: 'monitor',
        reason: 'some_negative'
      };
    }

    return {
      decision: 'reject',
      reason: 'unknown'
    };
  }
};
