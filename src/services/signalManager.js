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

// Timeout helper to prevent hanging
function withTimeout(promise, ms, label) {
  return Promise.race([
    promise,
    new Promise((_, reject) => 
      setTimeout(() => reject(new Error(`Timeout (${ms}ms) on ${label}`)), ms)
    )
  ]);
}

module.exports = {
  start() {
    logger.info('SignalManager started');
  },

  setOpenTradesAllowed,

  /**
   * handleRootSignal: FAST PATH - return signals quickly to Telegram
   * - Fetches market data and TV rating with SHORT timeouts (1-2 seconds max)
   * - If slow services fail, uses fallback values and continues
   * - Returns signal immediately for caller to send via Telegram
   * - notifyImmediately=true (LOOP 3): sends to Telegram here
   * - notifyImmediately=false (LOOP 1, 2): returns signal for batch sending
   */
  async handleRootSignal({ symbol, root_tf, detected_at = Date.now(), notifyImmediately = true } = {}) {
    const key = `${symbol}:${root_tf}`;
    if (inProgress.has(key)) {
      logger.debug({ key }, 'handleRootSignal: already in progress');
      return null;
    }
    inProgress.set(key, true);
    try {
      logger.info({ symbol, root_tf, notifyImmediately }, 'Root signal received');

      // FAST PATH: Fetch market data with 2-second timeout
      let mdata = { price: 0, volume_24h_usdt: 0, volume_change_pct: null, market_cap: null };
      try {
        logger.debug({ symbol }, 'Attempting to fetch market data (2s timeout)');
        mdata = await withTimeout(
          marketData.updateSymbolMarketData(symbol),
          2000,
          'marketData.updateSymbolMarketData'
        );
        if (!mdata) mdata = { price: 0, volume_24h_usdt: 0, volume_change_pct: null, market_cap: null };
        logger.debug({ symbol, price: mdata.price }, 'Market data acquired');
      } catch (err) {
        logger.warn({ err: err.message, symbol }, 'Market data fetch failed/timeout, using fallback');
        mdata = { price: 0, volume_24h_usdt: 0, volume_change_pct: null, market_cap: null };
      }

      // FAST PATH: Fetch TV rating with 1-second timeout
      let tv = { score: 0, source: 'fallback' };
      try {
        logger.debug({ symbol }, 'Attempting to fetch TV rating (1s timeout)');
        const tvRes = await withTimeout(
          tradingview.getOrFetchTvRatingCached(symbol),
          1000,
          'tradingview.getOrFetchTvRatingCached'
        );
        if (tvRes && typeof tvRes.score === 'number') {
          tv = { score: tvRes.score, source: tvRes.source || 'cached' };
          logger.debug({ symbol, score: tv.score }, 'TV rating acquired');
        }
      } catch (err) {
        logger.warn({ err: err.message, symbol }, 'TV rating fetch failed/timeout, using zero');
        tv = { score: 0, source: 'fallback' };
      }

      // Subscribe to MTF websockets (non-blocking)
      try { wsManager.subscribeSymbolMTF(symbol, config.MTF_TFS); } catch (e) { /* ignore */ }

      // Evaluate MTF alignment (should be fast, already computed by macd)
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
      logger.info({ symbol, root_tf }, 'Signal persisted to DB');

      const signalObj = {
        key,
        symbol,
        root_tf,
        detected_at,
        state: 'detected',
        meta
      };

      // ✅ LOOP 3 ONLY: Send telegram immediately when notifyImmediately=true
      if (notifyImmediately === true) {
        try {
          logger.info({ symbol, root_tf }, 'LOOP 3: Sending signal to Telegram immediately');
          await telegram.sendNewSignalSingleBlock(signalObj);
          logger.info({ symbol, root_tf }, 'Telegram signal sent (LOOP 3 - candle open)');
        } catch (err) {
          logger.warn({ err: err.message, symbol }, 'Failed to send telegram block');
        }
      } else {
        logger.debug({ symbol, root_tf }, 'LOOP 1/2: Returning signal for batch sending');
      }

      // Trade opening logic (non-blocking, in background)
      if (accept && accept.decision === 'accept' && config.OPENTRADE && openTradesAllowed) {
        this.attemptOpenTrade({ symbol, root_tf, alignment, meta, mdata }).catch(err => {
          logger.warn({ err: err.message, symbol }, 'Background trade opening failed');
        });
      }

      return signalObj;
    } catch (err) {
      logger.error({ err: err.message, symbol, root_tf }, 'handleRootSignal error');
      return null;
    } finally {
      setTimeout(() => inProgress.delete(key), 60 * 60 * 1000);
    }
  },

  /**
   * Attempt to open a trade (run in background, non-blocking)
   */
  async attemptOpenTrade({ symbol, root_tf, alignment, meta, mdata }) {
    if (!config.OPENTRADE) {
      logger.info({ symbol }, 'OPENTRADE disabled');
      return;
    }
    if (!openTradesAllowed) {
      logger.info({ symbol }, 'Open trades not yet enabled');
      return;
    }

    // Apply market-level filters
    let passFilters = true;
    if (config.MIN_MARKET_CAP > 0) {
      if (!mdata || !mdata.market_cap || Number(mdata.market_cap) < config.MIN_MARKET_CAP) {
        passFilters = false;
        logger.info({ symbol }, 'Filtered by MIN_MARKET_CAP');
      }
    }
    if (config.MIN_24H_USDT_VOLUME > 0) {
      if (!mdata || !mdata.volume_24h_usdt || Number(mdata.volume_24h_usdt) < config.MIN_24H_USDT_VOLUME) {
        passFilters = false;
        logger.info({ symbol }, 'Filtered by MIN_24H_USDT_VOLUME');
      }
    }
    if (isFinite(config.MIN_24H_VOLUME_CHANGE_PCT)) {
      const change = mdata?.volume_change_pct;
      if (change === null || change === undefined) {
        if (config.MIN_24H_VOLUME_CHANGE_PCT > 0) {
          passFilters = false;
          logger.info({ symbol }, 'Filtered by MIN_24H_VOLUME_CHANGE_PCT (no data)');
        }
      } else {
        if (change < config.MIN_24H_VOLUME_CHANGE_PCT) {
          passFilters = false;
          logger.info({ symbol }, 'Filtered by MIN_24H_VOLUME_CHANGE_PCT');
        }
      }
    }

    if (passFilters) {
      try {
        await tradeManager.openTrade({ symbol, root_tf, alignment, meta });
        logger.info({ symbol }, 'Trade opening initiated');
      } catch (err) {
        logger.error({ err: err.message, symbol }, 'Trade opening error');
      }
    } else {
      logger.info({ symbol }, 'Market filters prevented trade opening');
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
        logger.debug({ err: err.message, symbol, tf }, 'MTF alignment error');
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
   * handleNewRootCandle: Called when new root candle opens (LOOP 2)
   * Sends MTF alignment alerts via sendRootCandleUpdate
   */
  async handleNewRootCandle(newRootTfs = []) {
    try {
      logger.info({ newRootTfs }, 'handleNewRootCandle: fetching latest signals snapshot');
      const db = dbModule;
      const snapshot = db.getLatestSignalsSnapshot();
      
      logger.info({ snapshotCount: snapshot.length, newRootTfs }, 'Sending telegram update for new root candles');
      const telegramSvc = require('./telegram');
      await telegramSvc.sendRootCandleUpdate({ snapshot, newRootTfs });
      logger.info({ newRootTfs }, 'handleNewRootCandle: telegram update sent');
    } catch (e) {
      logger.debug({ e: e.message, newRootTfs }, 'handleNewRootCandle failed');
    }
  }
};
