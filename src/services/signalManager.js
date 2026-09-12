const dbModule = require('../db');
const wsManager = require('./bybitWs');
const macd = require('./macd');
const telegram = require('./telegram');
const tradeManager = require('./tradeManager');
const marketData = require('./marketData');
const tradingview = require('./tradingview');
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

  /**
   * handleRootSignal:
   * - notifyImmediately: if true (default) send telegram block immediately; otherwise persist signal and return it for caller to notify later
   * - returns the persisted signal object
   * - ALWAYS fetches fresh market data and TV rating for complete signal block
   */
  async handleRootSignal({ symbol, root_tf, detected_at = Date.now(), notifyImmediately = true } = {}) {
    const key = `${symbol}:${root_tf}`;
    if (inProgress.has(key)) {
      logger.debug({ key }, 'handleRootSignal: already in progress');
      return null;
    }
    inProgress.set(key, true);
    try {
      logger.info({ symbol, root_tf }, 'Root signal received');

      // ALWAYS fetch fresh market data (best-effort, with fallbacks)
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

      // FETCH TV RATING WITH CACHING (checks DB first, retries if needed)
      let tv = { score: 0, source: 'error' };
      try {
        logger.debug({ symbol }, 'handleRootSignal: fetching TV rating (cached or fresh)');
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

      // Subscribe to MTF websockets (for alignment updates)
      try { wsManager.subscribeSymbolMTF(symbol, config.MTF_TFS); } catch (e) { /* ignore */ }

      // Evaluate MTF alignment
      const alignment = await this.evaluateMtfAlignment(symbol);
      const mtfTfs = Object.keys(alignment || {});
      const positiveCount = mtfTfs.reduce((acc, t) => acc + (alignment[t] && alignment[t].positive ? 1 : 0), 0);
      const mtfScore = mtfTfs.length ? (positiveCount / mtfTfs.length) : 0;

      // Apply decision rules
      const accept = await this.applyDecision(alignment);

      // Compose meta and persist signal to DB
      const meta = {
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
        meta
      };

      if (notifyImmediately) {
        // send telegram block immediately with all metrics
        try {
          await telegram.sendRootSignalBlock({
            symbol,
            root_tf,
            alignment,
            detected_at,
            accept,
            marketData: mdata || {},
            tvScore: tv.score || 0,
            tvSource: tv.source || 'error',
            mtfScore
          });
          logger.info({ symbol, root_tf, tvScore: tv.score }, 'Telegram root signal block sent');
        } catch (err) {
          logger.warn({ err, symbol }, 'handleRootSignal: failed to send telegram block');
        }
      } else {
        logger.debug({ symbol, root_tf }, 'handleRootSignal: notifyImmediately=false, returning signal object');
        return signalObj;
      }

      // Only when decision is 'accept' do we attempt to open a trade
      if (accept && accept.decision === 'accept') {
        if (!config.OPENTRADE) {
          logger.info({ symbol }, 'Accept but OPENTRADE disabled; skipping openTrade');
        } else if (!openTradesAllowed) {
          logger.info({ symbol }, 'Accept but open trades not yet enabled (waiting for first boundary)');
        } else {
          // Apply market-level filters
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

  /**
   * evaluateMtfAlignment: Returns detailed alignment object with histogram, MACD, signal, rising, positive
   */
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

  /**
   * applyDecision: Determine signal acceptance based on alignment
   * - All positive: accept
   * - Only daily negative and rising: accept
   * - Some negative: monitor
   * - Otherwise: reject
   */
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
  },

  /**
   * sendStartupSummary: builds a snapshot of latest root signals and sends initial telegram message
   */
  async sendStartupSummary() {
    try {
      const db = dbModule;
      const snapshot = db.getLatestSignalsSnapshot();
      const telegramSvc = require('./telegram');

      await telegramSvc.sendStartupSummary({ snapshot });
    } catch (e) {
      logger.debug({ e }, 'sendStartupSummary failed');
    }
  },

  /**
   * handleNewRootCandle: Called when new root candle opens
   */
  async handleNewRootCandle(newRootTfs = []) {
    try {
      const db = dbModule;
      const snapshot = db.getLatestSignalsSnapshot();
      const telegramSvc = require('./telegram');
      await telegramSvc.sendRootCandleUpdate({ snapshot, newRootTfs });
    } catch (e) {
      logger.debug({ e, newRootTfs }, 'handleNewRootCandle failed');
    }
  }
};
