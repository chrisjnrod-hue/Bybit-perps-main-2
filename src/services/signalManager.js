// src/services/signalManager.js
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

// NEW: Signal cache for synchronization across blocks
const signalCache = new Map(); // symbol:root_tf -> signal object

function setOpenTradesAllowed(v) {
  openTradesAllowed = !!v;
  logger.info({ openTradesAllowed }, 'signalManager: openTradesAllowed set');
}

const inProgress = new Map();

module.exports = {
  start() {
    logger.info('SignalManager started');
    // NEW: Sync signals from DB on startup
    this.syncSignalsFromDb();
  },

  setOpenTradesAllowed,

  // NEW: Sync in-memory signal cache with DB snapshot
  syncSignalsFromDb() {
    try {
      const db = dbModule;
      const snapshot = db.getLatestSignalsSnapshot();
      signalCache.clear();
      for (const s of snapshot) {
        const key = `${s.symbol}:${s.root_tf}`;
        signalCache.set(key, s);
      }
      logger.info({ cachedSignals: signalCache.size }, 'SignalManager: synced signal cache from DB');
    } catch (e) {
      logger.debug({ e }, 'syncSignalsFromDb failed');
    }
  },

  /**
   * handleRootSignal - UPDATED FOR SYNC
   */
  async handleRootSignal({ symbol, root_tf, detected_at = Date.now(), notifyImmediately = true } = {}) {
    const key = `${symbol}:${root_tf}`;
    
    // NEW: Check if already cached (avoid duplicate processing)
    if (signalCache.has(key)) {
      const cached = signalCache.get(key);
      logger.debug({ key, cachedAt: cached.detected_at }, 'handleRootSignal: signal already in cache, returning cached version');
      return cached;
    }

    if (inProgress.has(key)) {
      logger.debug({ key }, 'handleRootSignal: already in progress');
      return null;
    }

    inProgress.set(key, true);
    try {
      logger.info({ symbol, root_tf }, 'Root signal received (new)');

      // Fetch market data
      const mdata = await marketData.updateSymbolMarketData(symbol);

      // Subscribe to MTF websockets
      try { wsManager.subscribeSymbolMTF(symbol, config.MTF_TFS); } catch (e) { /* ignore */ }

      const alignment = await this.evaluateMtfAlignment(symbol);
      const mtfTfs = Object.keys(alignment || {});
      const positiveCount = mtfTfs.reduce((acc, t) => acc + (alignment[t] && alignment[t].positive ? 1 : 0), 0);
      const mtfScore = mtfTfs.length ? (positiveCount / mtfTfs.length) : 0;

      const accept = await this.applyDecision(alignment);

      // Fetch TV rating (best-effort)
      let tv = { score: 0, source: 'none' };
      try {
        const tvRes = await tradingview.fetchTvRatingForSymbol(symbol);
        if (tvRes && typeof tvRes.score === 'number') tv = tvRes;
      } catch (e) {
        logger.debug({ e, symbol }, 'TV rating fetch failed');
      }

      // UPDATED: Comprehensive meta object with all signal data
      const meta = {
        tvScore: tv.score || 0,
        tvSource: tv.source || 'unknown',
        mtfScore,
        alignment,
        acceptReason: accept && accept.reason ? accept.reason : null,
        decision: accept && accept.decision ? accept.decision : 'monitor',
        marketData: mdata ? {
          symbol,
          price: mdata.price,
          volume_24h_usdt: mdata.volume_24h_usdt,
          prev_volume_24h_usdt: mdata.prev_volume_24h,
          volume_change_pct: mdata.volume_change_pct,
          market_cap: mdata.market_cap
        } : {},
        // NEW: MTF status snapshot for consistency
        mtfStatus: alignment || {}
      };

      dbModule.insertSignal({ symbol, root_tf, detected_at, state: 'detected', meta });

      // UPDATED: Create signal object with ALL fields for consistency
      const signalObj = {
        key,
        symbol,
        root_tf,
        detected_at,
        state: 'detected',
        meta,
        tvScore: tv.score || 0,
        mtfScore,
        alignment,
        // ADDED: These ensure per-signal blocks match startup snapshot
        marketData: mdata,
        decision: accept?.decision || 'monitor',
        acceptReason: accept?.reason || null
      };

      // NEW: Cache this signal for consistency
      signalCache.set(key, signalObj);
      logger.info({ key, cached: true }, 'Signal cached for synchronization');

      if (notifyImmediately) {
        // send telegram block immediately with all data
        await telegram.sendRootSignalBlock({
          symbol,
          root_tf,
          alignment,
          detected_at,
          accept,
          marketData: mdata,
          tvScore: tv.score || 0,
          mtfScore
        });
      } else {
        // do not notify now - return signal object so caller (poller) can notify at boundary
        return signalObj;
      }

      // Only when decision is 'accept' do we attempt to open a trade.
      if (accept && accept.decision === 'accept') {
        if (!config.OPENTRADE) {
          logger.info({ symbol }, 'Accept but OPENTRADE disabled; skipping openTrade');
        } else if (!openTradesAllowed) {
          logger.info({ symbol }, 'Accept but open trades not yet enabled (waiting for first boundary)');
        } else {
          // Apply market-level filters
          let passFilters = true;
          if (config.MIN_MARKET_CAP > 0) {
            if (!mdata?.market_cap || Number(mdata.market_cap) < config.MIN_MARKET_CAP) {
              passFilters = false;
              logger.info({ symbol, market_cap: mdata?.market_cap }, 'Filtered out by MIN_MARKET_CAP (for opening only)');
            }
          }
          if (config.MIN_24H_USDT_VOLUME > 0) {
            if (!mdata?.volume_24h_usdt || Number(mdata.volume_24h_usdt) < config.MIN_24H_USDT_VOLUME) {
              passFilters = false;
              logger.info({ symbol, volume_24h_usdt: mdata?.volume_24h_usdt }, 'Filtered out by MIN_24H_USDT_VOLUME (for opening only)');
            }
          }
          if (isFinite(config.MIN_24H_VOLUME_CHANGE_PCT)) {
            const change = mdata?.volume_change_pct;
            if (change === null) {
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
            await tradeManager.openTrade({ symbol, root_tf, alignment, meta });
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
   * evaluateMtfAlignment() - returns detailed alignment object
   */
  async evaluateMtfAlignment(symbol) {
    const result = {};
    for (const tf of config.MTF_TFS) {
      const hist = await macd.computeMacdHistogram(symbol, tf);
      if (!hist || hist.length === 0) {
        result[tf] = { ok: false };
        continue;
      }
      const last = hist[hist.length - 1];
      const prev = hist[hist.length - 2] || last;
      result[tf] = {
        histogram: last.histogram,
        macd: last.MACD,
        signal: last.signal,
        rising: last.histogram > prev.histogram,
        positive: last.histogram > 0
      };
    }
    return result;
  },

  async applyDecision(alignment) {
    const tfList = Object.keys(alignment);
    let allPositive = tfList.every(tf => alignment[tf].positive);
    if (allPositive) return { decision: 'accept', reason: 'all_positive' };
    const negatives = tfList.filter(tf => !alignment[tf].positive);
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
   * sendStartupSummary - USES CACHED SIGNALS
   */
  async sendStartupSummary() {
    try {
      const db = dbModule;
      const snapshot = db.getLatestSignalsSnapshot();
      
      // NEW: Merge with cache to ensure consistency
      for (const s of snapshot) {
        const key = `${s.symbol}:${s.root_tf}`;
        if (!signalCache.has(key)) {
          signalCache.set(key, s);
        }
      }

      const telegramSvc = require('./telegram');
      await telegramSvc.sendStartupSummary({ snapshot });
    } catch (e) {
      logger.debug({ e }, 'sendStartupSummary failed');
    }
  },

  /**
   * handleNewRootCandle - USES CACHED SIGNALS
   */
  async handleNewRootCandle(newRootTfs = []) {
    try {
      const db = dbModule;
      const snapshot = db.getLatestSignalsSnapshot();
      
      // NEW: Ensure cache is fresh
      for (const s of snapshot) {
        const key = `${s.symbol}:${s.root_tf}`;
        if (!signalCache.has(key)) {
          signalCache.set(key, s);
        }
      }

      const telegramSvc = require('./telegram');
      await telegramSvc.sendRootCandleUpdate({ snapshot, newRootTfs });
    } catch (e) {
      logger.debug({ e, newRootTfs }, 'handleNewRootCandle failed');
    }
  },

  // NEW: Public method to get cached signal
  getSignal(symbol, root_tf) {
    const key = `${symbol}:${root_tf}`;
    return signalCache.get(key) || null;
  },

  // NEW: Public method to get all cached signals
  getAllSignals() {
    return Array.from(signalCache.values());
  }
};
