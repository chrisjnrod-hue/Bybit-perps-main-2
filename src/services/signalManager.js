const dbModule = require('../db');
const wsManager = require('./bybitWs');
const macd = require('./macd');
const telegram = require('./telegram');
const tradeManager = require('./tradeManager');
const marketData = require('./marketData');
const tradingview = require('./tradingview');
const notificationQueue = require('./notificationQueue');
const config = require('../config');
const logger = require('pino')();

let openTradesAllowed = true;

function setOpenTradesAllowed(v) {
  openTradesAllowed = !!v;
  logger.info({ openTradesAllowed }, 'signalManager: openTradesAllowed set');
}

const inProgress = new Map();

module.exports = {
  start() {
    logger.info('SignalManager started');
  },

  setOpenTradesAllowed,

  async handleRootSignal({
    symbol,
    root_tf,
    detected_at = Date.now(),
    candle_open_time = null,
    event_type = 'root_flip',
    notifyImmediately = true
  } = {}) {
    const normalizedSymbol = String(symbol || '').toUpperCase();
    const normalizedTf = String(root_tf || '');
    const candleKey =
      candle_open_time === null || candle_open_time === undefined
        ? 'unknown'
        : String(candle_open_time);

    const key = [
      normalizedSymbol,
      normalizedTf,
      candleKey
    ].join(':');

    if (inProgress.has(key)) {
      logger.debug(
        { key },
        'handleRootSignal: same candle already in progress'
      );

      return null;
    }

    inProgress.set(key, true);

    try {
      logger.info(
        {
          symbol: normalizedSymbol,
          root_tf: normalizedTf,
          candle_open_time,
          event_type
        },
        'Root signal received'
      );

      let mdata;

      try {
        mdata =
          await marketData.updateSymbolMarketData(
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
          {
            err,
            symbol: normalizedSymbol
          },
          'handleRootSignal: market data failed'
        );

        mdata = {
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
        const tvRes =
          await tradingview.getOrFetchTvRatingCached(
            normalizedSymbol
          );

        if (
          tvRes &&
          typeof tvRes.score === 'number'
        ) {
          tv = {
            score: tvRes.score,
            source: tvRes.source || 'unknown'
          };
        }
      } catch (err) {
        logger.warn(
          {
            err,
            symbol: normalizedSymbol
          },
          'handleRootSignal: TV rating failed'
        );
      }

      try {
        wsManager.subscribeSymbolMTF(
          normalizedSymbol,
          config.MTF_TFS
        );
      } catch (_) {
        // Best effort.
      }

      const alignment =
        await this.evaluateMtfAlignment(
          normalizedSymbol
        );

      const mtfTfs = Object.keys(alignment || {});

      const positiveCount = mtfTfs.filter(
        (tf) =>
          alignment[tf] &&
          alignment[tf].positive
      ).length;

      const mtfScore =
        mtfTfs.length > 0
          ? positiveCount / mtfTfs.length
          : 0;

      const accept =
        await this.applyDecision(alignment);

      const meta = {
        tvScore: tv.score || 0,
        tvSource: tv.source || 'error',
        mtfScore,
        alignment,
        acceptReason:
          accept && accept.reason
            ? accept.reason
            : null,
        decision:
          accept && accept.decision
            ? accept.decision
            : 'monitor',
        marketData: mdata || {},
        eventType: event_type
      };

      dbModule.insertSignal({
        symbol: normalizedSymbol,
        root_tf: normalizedTf,
        detected_at,
        candle_open_time,
        state: 'detected',
        meta
      });

      const signalObj = {
        key,
        symbol: normalizedSymbol,
        root_tf: normalizedTf,
        candle_open_time,
        detected_at,
        event_type,
        state: 'detected',
        meta
      };

      if (notifyImmediately) {
        notificationQueue.enqueueSignal(
          signalObj,
          'realtime'
        );
      }

      if (
        accept &&
        accept.decision === 'accept'
      ) {
        if (!config.OPENTRADE) {
          logger.info(
            { symbol: normalizedSymbol },
            'Accept but OPENTRADE disabled'
          );
        } else if (!openTradesAllowed) {
          logger.info(
            { symbol: normalizedSymbol },
            'Accept but opening trades is disabled'
          );
        } else {
          let passFilters = true;

          if (
            config.MIN_MARKET_CAP > 0 &&
            (
              !mdata ||
              !mdata.market_cap ||
              Number(mdata.market_cap) <
                config.MIN_MARKET_CAP
            )
          ) {
            passFilters = false;
          }

          if (
            config.MIN_24H_USDT_VOLUME > 0 &&
            (
              !mdata ||
              !mdata.volume_24h_usdt ||
              Number(mdata.volume_24h_usdt) <
                config.MIN_24H_USDT_VOLUME
            )
          ) {
            passFilters = false;
          }

          if (
            Number.isFinite(
              config.MIN_24H_VOLUME_CHANGE_PCT
            )
          ) {
            const change =
              mdata &&
              mdata.volume_change_pct;

            if (
              change === null ||
              change === undefined
            ) {
              if (
                config.MIN_24H_VOLUME_CHANGE_PCT > 0
              ) {
                passFilters = false;
              }
            } else if (
              change <
              config.MIN_24H_VOLUME_CHANGE_PCT
            ) {
              passFilters = false;
            }
          }

          if (passFilters) {
            try {
              await tradeManager.openTrade({
                symbol: normalizedSymbol,
                root_tf: normalizedTf,
                alignment,
                meta
              });
            } catch (err) {
              logger.error(
                {
                  err,
                  symbol: normalizedSymbol
                },
                'handleRootSignal: openTrade failed'
              );
            }
          }
        }
      }

      return signalObj;
    } catch (err) {
      logger.error(
        {
          err,
          symbol: normalizedSymbol,
          root_tf: normalizedTf,
          candle_open_time
        },
        'handleRootSignal failed'
      );

      return null;
    } finally {
      setTimeout(
        () => inProgress.delete(key),
        60 * 60 * 1000
      );
    }
  },

  async evaluateMtfAlignment(symbol) {
    const result = {};
    for (const tf of config.MTF_TFS) {
      try {
        const hist = await macd.computeMacdHistogram(symbol, tf);
        if (!hist || hist.length === 0) {
          result[tf] = { ok: false, positive: false };
          continue;
        }
        const last = hist[hist.length - 1];
        const prev = hist[hist.length - 2] || last;
        result[tf] = {
          histogram: last.histogram,
          macd: last.MACD,
          signal: last.signal,
          rising: last.histogram > prev.histogram,
          positive: last.histogram > 0,
          ok: true
        };
      } catch (err) {
        logger.debug({ err, symbol, tf }, 'evaluateMtfAlignment error for timeframe');
        result[tf] = { ok: false, positive: false };
      }
    }
    return result;
  },

  async applyDecision(alignment) {
    const tfList = Object.keys(alignment);
    if (!tfList || tfList.length === 0) {
      return { decision: 'reject', reason: 'no_mtf_data' };
    }

    let allPositive = tfList.every(tf => alignment[tf] && alignment[tf].positive);
    if (allPositive) return { decision: 'accept', reason: 'all_positive' };

    const negatives = tfList.filter(tf => alignment[tf] && !alignment[tf].positive);
    if (negatives.length === 1 && negatives[0].toUpperCase() === 'D') {
      const d = alignment['D'];
      if (d && d.rising) return { decision: 'accept', reason: 'daily_rising' };
      return { decision: 'monitor', reason: 'daily_not_rising' };
    }

    if (negatives.length >= 1) {
      return { decision: 'monitor', reason: 'some_negative' };
    }

    return { decision: 'reject', reason: 'unknown' };
  }
};
