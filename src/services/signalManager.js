// src/services/signalManager.js
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

function setOpenTradesAllowed(value) {
  openTradesAllowed = Boolean(value);

  logger.info(
    { openTradesAllowed },
    'signalManager: openTradesAllowed set'
  );
}

/*
 * Prevents concurrent processing of the same symbol/timeframe during the
 * current process lifetime. Persistent event deduplication must happen in
 * poller.js and notificationQueue.js, not before notification delivery here.
 */
const inProgress = new Set();

function normalizeEventId(symbol, rootTf, eventId, candleTime) {
  if (eventId) {
    return String(eventId);
  }

  if (!symbol || !rootTf) {
    return null;
  }

  return [
    String(symbol),
    String(rootTf),
    String(Number(candleTime || Date.now()))
  ].join('_');
}

function safeCandleTime(candleTime, detectedAt) {
  const value = Number(candleTime || detectedAt || Date.now());
  return Number.isFinite(value) ? value : Date.now();
}

module.exports = {
  start() {
    logger.info('SignalManager started');
  },

  setOpenTradesAllowed,

  async sendStartupSummary() {
    try {
      const snapshot = dbModule.getLatestSignalsSnapshot();

      if (!Array.isArray(snapshot) || snapshot.length === 0) {
        logger.info(
          'signalManager.sendStartupSummary: no signals in snapshot'
        );
        return false;
      }

      logger.info(
        { count: snapshot.length },
        'signalManager.sendStartupSummary: enqueueing startup batch'
      );

      return notificationQueue.enqueueStartupBatch(snapshot);
    } catch (err) {
      logger.warn(
        { err },
        'signalManager.sendStartupSummary failed'
      );
      return false;
    }
  },

  async handleRootSignal({
    symbol,
    root_tf,
    detected_at = Date.now(),
    notifyImmediately = true,
    eventId = null,
    candleTime = null
  } = {}) {
    if (!symbol || !root_tf) {
      logger.warn(
        { symbol, root_tf },
        'handleRootSignal: missing symbol or root timeframe'
      );
      return null;
    }

    const normalizedSymbol = String(symbol);
    const normalizedRootTf = String(root_tf);
    const processKey = `${normalizedSymbol}:${normalizedRootTf}`;

    if (inProgress.has(processKey)) {
      logger.debug(
        { processKey },
        'handleRootSignal: symbol/timeframe already in progress'
      );
      return null;
    }

    inProgress.add(processKey);

    const finalEventId = normalizeEventId(
      normalizedSymbol,
      normalizedRootTf,
      eventId,
      candleTime
    );

    const finalCandleTime = safeCandleTime(
      candleTime,
      detected_at
    );

    try {
      logger.info(
        {
          symbol: normalizedSymbol,
          root_tf: normalizedRootTf,
          eventId: finalEventId,
          candleTime: finalCandleTime,
          notifyImmediately
        },
        'Root signal received'
      );

      /*
       * Always fetch fresh market data.
       */
      let mdata;

      try {
        mdata = await marketData.updateSymbolMarketData(
          normalizedSymbol
        );

        if (!mdata) {
          mdata = {
            price: 0,
            volume_24h_usdt: 0,
            volume_change_pct: null,
            market_cap: null
          };
        }
      } catch (err) {
        logger.warn(
          { err, symbol: normalizedSymbol },
          'handleRootSignal: market data fetch failed, using zeros'
        );

        mdata = {
          price: 0,
          volume_24h_usdt: 0,
          volume_change_pct: null,
          market_cap: null
        };
      }

      /*
       * Fetch TradingView rating.
       */
      let tv = {
        score: 0,
        source: 'error'
      };

      try {
        const tvResult =
          await tradingview.getOrFetchTvRatingCached(
            normalizedSymbol
          );

        if (
          tvResult &&
          typeof tvResult.score === 'number'
        ) {
          tv = {
            score: tvResult.score,
            source: tvResult.source || 'unknown'
          };

          logger.info(
            {
              symbol: normalizedSymbol,
              score: tv.score,
              source: tv.source
            },
            'TV rating acquired'
          );
        } else {
          logger.warn(
            { symbol: normalizedSymbol },
            'TV rating returned invalid result'
          );
        }
      } catch (err) {
        logger.warn(
          {
            err: err && err.message
              ? err.message
              : err,
            symbol: normalizedSymbol
          },
          'handleRootSignal: TV rating fetch failed'
        );
      }

      /*
       * Subscribe to configured MTF streams if available.
       */
      try {
        if (
          wsManager &&
          typeof wsManager.subscribeSymbolMTF === 'function'
        ) {
          wsManager.subscribeSymbolMTF(
            normalizedSymbol,
            config.MTF_TFS
          );
        }
      } catch (err) {
        logger.debug(
          {
            err,
            symbol: normalizedSymbol
          },
          'handleRootSignal: MTF subscription failed'
        );
      }

      const alignment = await this.evaluateMtfAlignment(
        normalizedSymbol
      );

      const mtfTfs = Object.keys(alignment || {});

      const positiveCount = mtfTfs.reduce(
        (count, timeframe) => {
          return count + (
            alignment[timeframe] &&
            alignment[timeframe].positive
              ? 1
              : 0
          );
        },
        0
      );

      const mtfScore = mtfTfs.length > 0
        ? positiveCount / mtfTfs.length
        : 0;

      const decision = await this.applyDecision(alignment);

      const meta = {
        eventId: finalEventId,
        candleTime: finalCandleTime,
        tvScore: Number(tv.score) || 0,
        tvSource: tv.source || 'error',
        mtfScore,
        alignment,
        acceptReason: decision && decision.reason
          ? decision.reason
          : null,
        decision: decision && decision.decision
          ? decision.decision
          : 'monitor',
        marketData: mdata || {}
      };

      const insertedSignalId = dbModule.insertSignal({
        symbol: normalizedSymbol,
        root_tf: normalizedRootTf,
        detected_at,
        state: 'detected',
        meta
      });

      if (!insertedSignalId) {
        logger.warn(
          {
            symbol: normalizedSymbol,
            root_tf: normalizedRootTf,
            eventId: finalEventId
          },
          'handleRootSignal: database insert failed'
        );

        return null;
      }

      const signalObj = {
        key: `${normalizedSymbol}:${normalizedRootTf}`,
        symbol: normalizedSymbol,
        root_tf: normalizedRootTf,
        detected_at,
        state: 'detected',
        eventId: finalEventId,
        candleTime: finalCandleTime,
        meta
      };

      /*
       * Realtime path:
       *
       * Only enqueue here. The notification queue marks the event as sent
       * after Telegram delivery succeeds.
       */
      if (notifyImmediately) {
        const queued = notificationQueue.enqueueSignal(
          signalObj,
          'realtime'
        );

        if (!queued) {
          logger.warn(
            {
              symbol: normalizedSymbol,
              root_tf: normalizedRootTf,
              eventId: finalEventId
            },
            'handleRootSignal: realtime notification was not queued'
          );

          return null;
        }

        logger.info(
          {
            symbol: normalizedSymbol,
            root_tf: normalizedRootTf,
            eventId: finalEventId
          },
          'handleRootSignal: realtime notification queued'
        );
      }

      /*
       * Startup and candle-open paths return the signal object to poller.js.
       * poller.js later submits the returned objects to enqueueStartupBatch().
       */
      if (!notifyImmediately) {
        logger.info(
          {
            symbol: normalizedSymbol,
            root_tf: normalizedRootTf,
            eventId: finalEventId
          },
          'handleRootSignal: returning signal for batch notification'
        );

        return signalObj;
      }

      /*
       * Opening a trade is independent from Telegram delivery.
       */
      if (
        decision &&
        decision.decision === 'accept'
      ) {
        if (!config.OPENTRADE) {
          logger.info(
            { symbol: normalizedSymbol },
            'Accept but OPENTRADE disabled; skipping openTrade'
          );
        } else if (!openTradesAllowed) {
          logger.info(
            { symbol: normalizedSymbol },
            'Accept but open trades are not yet enabled'
          );
        } else {
          let passFilters = true;

          if (Number(config.MIN_MARKET_CAP || 0) > 0) {
            if (
              !mdata ||
              !mdata.market_cap ||
              Number(mdata.market_cap) <
                Number(config.MIN_MARKET_CAP)
            ) {
              passFilters = false;

              logger.info(
                {
                  symbol: normalizedSymbol,
                  market_cap: mdata
                    ? mdata.market_cap
                    : null
                },
                'Filtered by MIN_MARKET_CAP'
              );
            }
          }

          if (
            Number(config.MIN_24H_USDT_VOLUME || 0) > 0
          ) {
            if (
              !mdata ||
              !mdata.volume_24h_usdt ||
              Number(mdata.volume_24h_usdt) <
                Number(config.MIN_24H_USDT_VOLUME)
            ) {
              passFilters = false;

              logger.info(
                {
                  symbol: normalizedSymbol,
                  volume_24h_usdt: mdata
                    ? mdata.volume_24h_usdt
                    : null
                },
                'Filtered by MIN_24H_USDT_VOLUME'
              );
            }
          }

          const minVolumeChange = Number(
            config.MIN_24H_VOLUME_CHANGE_PCT
          );

          if (Number.isFinite(minVolumeChange)) {
            const volumeChange = mdata
              ? mdata.volume_change_pct
              : null;

            if (
              volumeChange === null ||
              volumeChange === undefined
            ) {
              if (minVolumeChange > 0) {
                passFilters = false;

                logger.info(
                  { symbol: normalizedSymbol },
                  'Filtered because volume change is unavailable'
                );
              }
            } else if (
              Number(volumeChange) < minVolumeChange
            ) {
              passFilters = false;

              logger.info(
                {
                  symbol: normalizedSymbol,
                  volume_change_pct: volumeChange
                },
                'Filtered by MIN_24H_VOLUME_CHANGE_PCT'
              );
            }
          }

          if (passFilters) {
            try {
              await tradeManager.openTrade({
                symbol: normalizedSymbol,
                root_tf: normalizedRootTf,
                alignment,
                meta
              });

              logger.info(
                { symbol: normalizedSymbol },
                'handleRootSignal: trade opening initiated'
              );
            } catch (err) {
              logger.error(
                {
                  err,
                  symbol: normalizedSymbol
                },
                'handleRootSignal: openTrade failed'
              );
            }
          } else {
            logger.info(
              { symbol: normalizedSymbol },
              'Decision accepted but market filters prevented trade opening'
            );
          }
        }
      }

      return signalObj;
    } catch (err) {
      logger.error(
        {
          err,
          symbol: normalizedSymbol,
          root_tf: normalizedRootTf,
          eventId: finalEventId
        },
        'handleRootSignal error'
      );

      return null;
    } finally {
      inProgress.delete(processKey);
    }
  },

  async evaluateMtfAlignment(symbol) {
    const result = {};
    const timeframes = Array.isArray(config.MTF_TFS)
      ? config.MTF_TFS
      : [];

    for (const tf of timeframes) {
      try {
        const histogram =
          await macd.getClosedMacdHistogram(symbol, tf);

        if (!histogram || histogram.length === 0) {
          result[tf] = {
            ok: false,
            positive: false
          };
          continue;
        }

        const latest =
          histogram[histogram.length - 1];

        const previous =
          histogram[histogram.length - 2] || latest;

        result[tf] = {
          histogram: latest.histogram,
          macd: latest.MACD,
          signal: latest.signal,
          rising: latest.histogram > previous.histogram,
          positive: latest.histogram > 0,
          ok: true,
          candleTime: latest.time
        };
      } catch (err) {
        logger.debug(
          { err, symbol, tf },
          'evaluateMtfAlignment error for timeframe'
        );

        result[tf] = {
          ok: false,
          positive: false
        };
      }
    }

    return result;
  },

  async applyDecision(alignment) {
    const tfList = Object.keys(alignment || {});

    if (tfList.length === 0) {
      return {
        decision: 'reject',
        reason: 'no_mtf_data'
      };
    }

    const allPositive = tfList.every((tf) => {
      return alignment[tf] &&
        alignment[tf].positive === true;
    });

    if (allPositive) {
      return {
        decision: 'accept',
        reason: 'all_positive'
      };
    }

    const negatives = tfList.filter((tf) => {
      return alignment[tf] &&
        alignment[tf].positive !== true;
    });

    if (
      negatives.length === 1 &&
      String(negatives[0]).toUpperCase() === 'D'
    ) {
      const daily = alignment.D;

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

    if (negatives.length >= 1) {
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
