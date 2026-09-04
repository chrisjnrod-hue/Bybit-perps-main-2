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

// Signal cache for synchronization
const signalCache = new Map();

function setOpenTradesAllowed(v) {
  openTradesAllowed = !!v;
  logger.info({ openTradesAllowed }, 'signalManager: openTradesAllowed set');
}

const inProgress = new Map();

module.exports = {
  start() {
    logger.info('SignalManager started');
    this.syncSignalsFromDb();
  },

  setOpenTradesAllowed,

  // Sync in-memory signal cache with DB snapshot
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
   * handleRootSignal - UPDATED TO FETCH 5m/15m MTF DATA
   */
  async handleRootSignal({ symbol, root_tf, detected_at = Date.now(), notifyImmediately = true } = {}) {
    const key = `${symbol}:${root_tf}`;
    
    // Check if already cached
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

      // CRITICAL: Seed 5m/15m klines IMMEDIATELY so MTF can be computed
      logger.info({ symbol }, 'Seeding 5m/15m klines for MTF computation');
      try {
        const poller = require('./poller');
        await Promise.all([
          poller.seedKlinesForSymbol(symbol, '5'),
          poller.seedKlinesForSymbol(symbol, '15')
        ]);
        logger.info({ symbol }, '5m/15m klines seeded successfully');
      } catch (e) {
        logger.debug({ e, symbol }, 'Error seeding 5m/15m klines (continuing anyway)');
      }

      // Subscribe to MTF websockets
      try { wsManager.subscribeSymbolMTF(symbol, config.MTF_TFS); } catch (e) { /* ignore */ }

      // CRITICAL: Evaluate alignment AFTER 5m/15m is seeded
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

      // Compose meta with POPULATED alignment
      const meta = {
        tvScore: tv.score || 0,
        tvSource: tv.source || 'unknown',
        mtfScore,
        alignment,           // NOW HAS 5m/15m WITH ACTUAL VALUES
        acceptReason: accept && accept.reason ? accept.reason : null,
        decision: accept && accept.decision ? accept.decision : 'monitor',
        marketData: mdata ? {
          symbol,
          price: mdata.price,
          volume_24h_usdt: mdata.volume_24h_usdt,
          prev_volume_24h_usdt: mdata.prev_volume_24h,
          volume_change_pct: mdata.volume_change_pct,
          market_cap: mdata.market_cap
        } : {}
      };

      dbModule.insertSignal({ symbol, root_tf, detected_at, state: 'detected', meta });

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
        marketData: mdata,
        decision: accept?.decision || 'monitor',
        acceptReason: accept?.reason || null
      };

      // Cache this signal
      signalCache.set(key, signalObj);
      logger.info({ key, cached: true, mtfStatus: alignment }, 'Signal cached with MTF data');

      if (notifyImmediately) {
        // send telegram block immediately with COMPLETE MTF data
        try {
          if (telegram.sendRootSignalBlock) {
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
          } else if (telegram.sendNewSignalSingleBlock) {
            await telegram.sendNewSignalSingleBlock(signalObj);
          } else {
            // fallback: generic startup summary call
            await telegram.sendStartupSummary ? telegram.sendStartupSummary({ snapshot: [signalObj] }) : null;
          }
        } catch (e) {
          logger.debug({ e, symbol }, 'Failed to send immediate root signal block (continuing)');
        }
      } else {
        return signalObj;
      }

      // Only attempt to open trade if decision is 'accept'
      if (accept && accept.decision === 'accept') {
        if (!config.OPENTRADE) {
          logger.info({ symbol }, 'Accept but OPENTRADE disabled');
        } else if (!openTradesAllowed) {
          logger.info({ symbol }, 'Accept but open trades not yet enabled');
        } else {
          // Apply market-level filters
          let passFilters = true;
          if (config.MIN_MARKET_CAP > 0) {
            if (!mdata?.market_cap || Number(mdata.market_cap) < config.MIN_MARKET_CAP) {
              passFilters = false;
              logger.info({ symbol, market_cap: mdata?.market_cap }, 'Filtered by MIN_MARKET_CAP');
            }
          }
          if (config.MIN_24H_USDT_VOLUME > 0) {
            if (!mdata?.volume_24h_usdt || Number(mdata.volume_24h_usdt) < config.MIN_24H_USDT_VOLUME) {
              passFilters = false;
              logger.info({ symbol }, 'Filtered by MIN_24H_USDT_VOLUME');
            }
          }
          if (isFinite(config.MIN_24H_VOLUME_CHANGE_PCT)) {
            const change = mdata?.volume_change_pct;
            if (change === null) {
              if (config.MIN_24H_VOLUME_CHANGE_PCT > 0) {
                passFilters = false;
                logger.info({ symbol }, 'Filtered by volume change (no data)');
              }
            } else if (change < config.MIN_24H_VOLUME_CHANGE_PCT) {
              passFilters = false;
              logger.info({ symbol, change }, 'Filtered by MIN_24H_VOLUME_CHANGE_PCT');
            }
          }

          if (passFilters) {
            await tradeManager.openTrade({ symbol, root_tf, alignment, meta });
          } else {
            logger.info({ symbol }, 'Accepted but market filters prevented trade');
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
      
      // Merge with cache to ensure consistency
      for (const s of snapshot) {
        const key = `${s.symbol}:${s.root_tf}`;
        if (!signalCache.has(key)) {
          signalCache.set(key, s);
        }
      }

      // compute counts per root_tf
      const counts = {};
      for (const s of snapshot) {
        counts[s.root_tf] = (counts[s.root_tf] || 0) + 1;
      }

      const telegramSvc = require('./telegram');

      // Prefer a dedicated header sender if available
      if (telegramSvc.sendStartupSummaryHeader) {
        try {
          await telegramSvc.sendStartupSummaryHeader({ counts, total: snapshot.length });
        } catch (e) {
          logger.debug({ e }, 'sendStartupSummary: header send failed (continuing)');
        }
      } else {
        // fallback: call existing summary (single message) if present
        try {
          if (telegramSvc.sendStartupSummary) {
            await telegramSvc.sendStartupSummary({ snapshot, counts });
          }
        } catch (e) {
          logger.debug({ e }, 'sendStartupSummary: fallback header send failed');
        }
      }

      // Send one signal block per signal (sequentially to avoid hitting rate limits)
      for (const s of snapshot) {
        try {
          if (telegramSvc.sendRootSignalBlock) {
            await telegramSvc.sendRootSignalBlock({
              symbol: s.symbol,
              root_tf: s.root_tf,
              detected_at: s.detected_at,
              alignment: s.meta?.alignment || null,
              tvScore: s.meta?.tvScore || 0,
              marketData: s.meta?.marketData || {}
            });
          } else if (telegramSvc.sendNewSignalSingleBlock) {
            await telegramSvc.sendNewSignalSingleBlock(s);
          } else {
            // Last fallback: send summary with single-signal payload
            await telegramSvc.sendStartupSummary ? telegramSvc.sendStartupSummary({ snapshot: [s] }) : null;
          }
        } catch (e) {
          logger.debug({ e, s }, 'sendStartupSummary: failed to send per-signal block (continuing)');
        }
      }
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
      
      // Ensure cache is fresh
      for (const s of snapshot) {
        const key = `${s.symbol}:${s.root_tf}`;
        if (!signalCache.has(key)) {
          signalCache.set(key, s);
        }
      }

      const telegramSvc = require('./telegram');
      await telegramSvc.sendRootCandleUpdate ? telegramSvc.sendRootCandleUpdate({ snapshot, newRootTfs }) : null;
    } catch (e) {
      logger.debug({ e, newRootTfs }, 'handleNewRootCandle failed');
    }
  },

  // Public method to get cached signal
  getSignal(symbol, root_tf) {
    const key = `${symbol}:${root_tf}`;
    return signalCache.get(key) || null;
  },

  // Public method to get all cached signals
  getAllSignals() {
    return Array.from(signalCache.values());
  }
};
