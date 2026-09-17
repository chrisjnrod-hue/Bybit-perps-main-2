// src/services/signalManager.js
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

function normalizeEventId(symbol, root_tf, eventId, candleTime) {
  if (eventId) return eventId;
  if (!symbol || !root_tf) return null;
  return `${String(symbol)}_${String(root_tf)}_${Number(candleTime || Date.now())}`;
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
        logger.info('signalManager.sendStartupSummary: no signals in snapshot');
        return;
      }

      logger.info({ count: snapshot.length }, 'signalManager.sendStartupSummary: enqueueing startup batch');
      notificationQueue.enqueueStartupBatch(snapshot);
    } catch (err) {
      logger.warn({ err }, 'signalManager.sendStartupSummary failed');
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
    const key = `${symbol}:${root_tf}`;
    if (inProgress.has(key)) {
      logger.debug({ key }, 'handleRootSignal: already in progress');
      return null;
    }
    inProgress.set(key, true);

    const finalEventId = normalizeEventId(symbol, root_tf, eventId, candleTime);

    try {
      logger.info({ symbol, root_tf, eventId: finalEventId }, 'Root signal received');

      // ALWAYS fetch fresh market data
      let mdata = null;
      try {
        mdata = await marketData.updateSymbolMarketData(symbol);
        if (!mdata) {
          mdata = { price: 0, volume_24h_usdt: 0, volume_change_pct: null, market_cap: null };
        }
      } catch (err) {
        logger.warn({ err, symbol }, 'handleRootSignal: market data fetch failed, using zeros');
        mdata = { price: 0, volume_24h_usdt: 0, volume_change_pct: null, market_cap: null };
      }

      // TV rating
      let tv = { score: 0, source: 'error' };
      try {
        logger.debug({ symbol }, 'handleRootSignal: fetching TV rating');
        const tvRes = await tradingview.getOrFetchTvRatingCached(symbol);
        if (tvRes && typeof tvRes.score === 'number') {
          tv = { score: tvRes.score, source: tvRes.source || 'unknown' };
          logger.info({ symbol, score: tv.score, source: tv.source }, 'TV rating acquired');
        } else {
          logger.warn({ symbol }, 'TV rating fetch returned invalid result, using zero');
          tv = { score: 0, source: 'error' };
        }
      } catch (err) {
        logger.warn({ err: err && err.message, symbol }, 'handleRootSignal: TV rating fetch error, using zero');
        tv = { score: 0, source: 'error' };
      }

      try { wsManager.subscribeSymbolMTF(symbol, config.MTF_TFS); } catch (e) { /* ignore */ }

      const alignment = await this.evaluateMtfAlignment(symbol);
      const mtfTfs = Object.keys(alignment || {});
      const positiveCount = mtfTfs.reduce((acc, t) => acc + (alignment[t] && alignment[t].positive ? 1 : 0), 0);
      const mtfScore = mtfTfs.length ? (positiveCount / mtfTfs.length) : 0;

      const accept = await this.applyDecision(alignment);

      const meta = {
        eventId: finalEventId,
        candleTime: Number(candleTime || detected_at || Date.now()),
        tvScore: tv.score || 0,
        tvSource: tv.source || 'error',
        mtfScore,
        alignment,
        acceptReason: accept && accept.reason ? accept.reason : null,
        decision: accept && accept.decision ? accept.decision : 'monitor',
        marketData: mdata || {}
      };

      dbModule.insertSignal({ symbol, root_tf, detected_at, state: 'detected', meta });

      const signalObj = {
        key,
        symbol,
        root_tf,
        detected_at,
        state: 'detected',
        eventId: finalEventId,
        candleTime: Number(candleTime || detected_at || Date.now()),
        meta
      };

      if (notifyImmediately) {
        logger.debug({ symbol, root_tf, eventId: finalEventId }, 'handleRootSignal: enqueuing realtime signal to notification queue');
        notificationQueue.enqueueSignal(signalObj, 'realtime');
      } else {
        logger.debug({ symbol, root_tf, eventId: finalEventId }, 'handleRootSignal: notifyImmediately=false, returning signal object');
        return signalObj;
      }

      if (accept && accept.decision === 'accept') {
        if (!config.OPENTRADE) {
          logger.info({ symbol }, 'Accept but OPENTRADE disabled; skipping openTrade');
        } else if (!openTradesAllowed) {
          logger.info({ symbol }, 'Accept but open trades not yet enabled (waiting for first boundary)');
        } else {
          let passFilters = true;
          if (config.MIN_MARKET_CAP > 0) {
            if (!mdata || !mdata.market_cap || Number(mdata.market_cap) < config.MIN_MARKET_CAP) {
              passFilters = false;
              logger.info({ symbol, market_cap: mdata?.market_cap }, 'Filtered out by MIN_MARKET_CAP (for opening only)');
            }
          }
          if (config.MIN_24H_USDT_VOLUME > 0) {
            if (!mdata || !mdata.volume_24h_usdt || Number(mdata.volume_24h_usdt) < config.MIN_24H_USDT_VOLUME) {
              passFilters = false;
              logger.info({ symbol, volume_24h_usdt: mdata?.volume_24h_usdt }, 'Filtered out by MIN_24H_USDT_VOLUME (for opening only)');
            }
          }
          if (isFinite(config.MIN_24H_VOLUME_CHANGE_PCT)) {
            const change = mdata?.volume_change_pct;
            if (change === null || change === undefined) {
              if (config.MIN_24H_VOLUME_CHANGE_PCT > 0) {
                passFilters = false;
                logger.info({ symbol }, 'No previous volume to compute change; filtered by MIN_24H_VOLUME_CHANGE_PCT (for opening only)');
              }
            } else {
              if (change < config.MIN_24H_VOLUME_CHANGE_PCT) {
                passFilters = false;
                logger.info({ symbol, volume_change_pct: change }, 'Filtered out by MIN_24H_VOLUME_CHANGE_PCT (for opening only)');
              }
            }
          }

          if (passFilters) {
            try {
              await tradeManager.openTrade({ symbol, root_tf, alignment, meta });
              logger.info({ symbol }, 'handleRootSignal: trade opening initiated');
            } catch (err) {
              logger.error({ err, symbol }, 'handleRootSignal: openTrade error');
            }
          } else {
            logger.info({ symbol }, 'Decision accepted but market filters prevented opening a trade');
          }
        }
      }

      return signalObj;
    } catch (err) {
      logger.error({ err, symbol, root_tf }, 'handleRootSignal error');
      return null;
    } finally {
      setTimeout(() => inProgress.delete(key), 60 * 60 * 1000);
    }
  },

  async evaluateMtfAlignment(symbol) {
    const result = {};

    for (const tf of config.MTF_TFS) {
      try {
        const hist = await macd.getClosedMacdHistogram(symbol, tf);

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
          ok: true,
          candleTime: last.time
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
