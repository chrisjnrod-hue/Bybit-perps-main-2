/**
 * src/services/signalManager.js
 *
 * Responsibilities:
 * - Orchestrates per-symbol "root" signals (market data, TV rating, MTF alignment, decision)
 * - Persists a signal snapshot to DB (via dbModule.insertSignal when available)
 * - Sends Telegram blocks immediately (or returns the signal object)
 * - Computes a fallback TV-like score when TradingView is unavailable
 *
 * This version:
 * - Ensures startup summary snapshot retrieval is robust (tries multiple sources)
 * - Ensures simulated recommendation lines are emitted even when OPENTRADES=false
 * - Defensive DB writes and non-fatal behavior on DB errors
 */

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

async function fetchLatestSignalsSnapshotFallback(limit = 500) {
  // Try multiple fallbacks to obtain a recent snapshot of signals for summaries
  try {
    // 1) Prefer dbModule.getLatestSignalsSnapshot()
    if (dbModule && typeof dbModule.getLatestSignalsSnapshot === 'function') {
      try {
        const snap = dbModule.getLatestSignalsSnapshot();
        if (Array.isArray(snap) && snap.length) return snap;
      } catch (e) {
        logger.debug({ e }, 'fetchLatestSignalsSnapshotFallback: getLatestSignalsSnapshot failed');
      }
    }

    // 2) Try dbModule.get().prepare(...) reading signals table if available
    if (dbModule && typeof dbModule.get === 'function') {
      try {
        const db = dbModule.get();
        // We will try to read a few recent signals; if meta stored as JSON string, parse it.
        const rows = db.prepare('SELECT key, symbol, root_tf, detected_at, state, meta FROM signals ORDER BY detected_at DESC LIMIT ?').all(limit || 500);
        if (rows && rows.length) {
          return rows.map(r => {
            let meta = r.meta;
            if (typeof meta === 'string') {
              try { meta = JSON.parse(meta); } catch (e) { /* keep as string */ }
            }
            return {
              key: r.key,
              symbol: r.symbol,
              root_tf: r.root_tf,
              detected_at: r.detected_at,
              state: r.state,
              meta
            };
          });
        }
      } catch (e) {
        logger.debug({ e }, 'fetchLatestSignalsSnapshotFallback: reading signals table failed');
      }
    }
  } catch (e) {
    logger.debug({ e }, 'fetchLatestSignalsSnapshotFallback: unexpected error');
  }

  return [];
}

module.exports = {
  start() {
    logger.info('SignalManager started');
  },

  setOpenTradesAllowed,

  /**
   * handleRootSignal:
   * - notifyImmediately: if true (default) send telegram block immediately; otherwise persist signal and return it for caller to notify later
   * - returns the persisted signal object (or null on error)
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

      // 1) Market data
      let mdata = null;
      try {
        mdata = await marketData.updateSymbolMarketData(symbol);
        if (!mdata) mdata = { price: 0, volume_24h_usdt: 0, volume_change_pct: null, market_cap: null };
      } catch (err) {
        logger.warn({ err, symbol }, 'handleRootSignal: market data fetch failed, using safe defaults');
        mdata = { price: 0, volume_24h_usdt: 0, volume_change_pct: null, market_cap: null };
      }

      // Normalize market data
      const normalizedMdata = (function(md) {
        const price = Number(md?.price ?? md?.last_price ?? md?.last ?? md?.close ?? 0) || 0;
        const vol24 = Number(md?.volume_24h_usdt ?? md?.volume_24h ?? md?.volumeUsd24h ?? md?.volume ?? md?.turnover24h ?? 0) || 0;
        const marketCapRaw = md?.market_cap ?? md?.marketCap ?? md?.marketCapUsd ?? null;
        const market_cap = marketCapRaw !== null && marketCapRaw !== undefined ? Number(marketCapRaw) : null;
        const volChangeRaw = (md?.volume_change_pct ?? md?.volumeChangePct ?? md?.volume_change ?? null);
        const volume_change_pct = (typeof volChangeRaw === 'number') ? volChangeRaw : (volChangeRaw !== null && !isNaN(Number(volChangeRaw)) ? Number(volChangeRaw) : null);

        return {
          price,
          market_cap,
          volume_24h_usdt: vol24,
          volume_change_pct,
          marketCap: market_cap,
          volume24h: vol24,
          volumeChangePct: volume_change_pct,
          raw: md || {}
        };
      })(mdata);

      // 2) TV rating (cached or fresh)
      let tv = { score: 0, score_pct: 0, source: 'error' };
      try {
        logger.debug({ symbol }, 'handleRootSignal: fetching TV rating (cached or fresh)');
        const tvRes = await tradingview.getOrFetchTvRatingCached(symbol);
        if (tvRes && typeof tvRes.score === 'number') {
          tv = {
            score: tvRes.score,
            score_pct: typeof tvRes.score_pct === 'number' ? tvRes.score_pct : Math.round((tvRes.score || 0) * 100),
            source: tvRes.source || 'unknown'
          };
          logger.info({ symbol, score: tv.score, score_pct: tv.score_pct, source: tv.source }, 'TV rating acquired');
        } else {
          logger.warn({ symbol }, 'TV rating fetch returned invalid result, using zero');
          tv = { score: 0, score_pct: 0, source: 'error' };
        }
      } catch (err) {
        logger.warn({ err: err && err.message, symbol }, 'handleRootSignal: TV rating fetch error, using zero');
        tv = { score: 0, score_pct: 0, source: 'error' };
      }

      // 3) Subscribe to MTF websockets (best-effort)
      try { wsManager.subscribeSymbolMTF(symbol, config.MTF_TFS); } catch (e) { logger.debug({ e }, 'subscribeSymbolMTF failed (non-fatal)'); }

      // 4) Evaluate MTF alignment (MACD)
      const alignment = await this.evaluateMtfAlignment(symbol);
      const mtfTfs = Object.keys(alignment || {});
      const positiveCount = mtfTfs.reduce((acc, t) => acc + (alignment[t] && alignment[t].positive ? 1 : 0), 0);
      const mtfScore = mtfTfs.length ? (positiveCount / mtfTfs.length) : 0;

      // 5) Compute fallback TV-like score if TV missing or fallback
      if ((tv.source && String(tv.source).toLowerCase().startsWith('fallback')) || tv.score === 0) {
        try {
          const macdPositiveFraction = mtfScore || 0;
          const volChangePct = (typeof normalizedMdata.volume_change_pct === 'number') ? normalizedMdata.volume_change_pct : 0;
          let fb = null;
          if (typeof tradingview.fallbackScore === 'function') {
            fb = tradingview.fallbackScore({ macdPositiveFraction, volChangePct });
          }
          if (fb && typeof fb.score === 'number') {
            logger.info({ symbol, computedFallbackScore: fb.score, computedFallbackPct: fb.score_pct }, 'Computed fallback TV-like score from MACD/volume');
            tv = { score: fb.score, score_pct: (typeof fb.score_pct === 'number' ? fb.score_pct : Math.round((fb.score || 0) * 100)), source: 'fallback_computed' };
          }
        } catch (e) {
          logger.debug({ e, symbol }, 'Failed to compute fallback TV score (continuing)');
        }
      }

      // 6) Apply decision rules
      const accept = await this.applyDecision(alignment);

      // 7) Compose meta and persist
      const meta = {
        tvScore: tv.score || 0,
        tvScorePct: tv.score_pct || 0,
        tvSource: tv.source || 'error',
        mtfScore,
        alignment,
        acceptReason: accept && accept.reason ? accept.reason : null,
        decision: accept && accept.decision ? accept.decision : 'monitor',
        marketData: normalizedMdata
      };

      // Persist using dbModule.insertSignal if available, otherwise try a safe DB write fallback
      try {
        if (dbModule && typeof dbModule.insertSignal === 'function') {
          dbModule.insertSignal({ symbol, root_tf, detected_at, state: 'detected', meta });
        } else if (dbModule && typeof dbModule.get === 'function') {
          const db = dbModule.get();
          // Try to ensure a signals table exists if used by the repo
          try {
            db.prepare(`CREATE TABLE IF NOT EXISTS signals (
              key TEXT PRIMARY KEY,
              symbol TEXT,
              root_tf TEXT,
              detected_at INTEGER,
              state TEXT,
              meta TEXT
            )`).run();
          } catch (e) { /* ignore create errors */ }

          try {
            const keyVal = `${symbol}:${root_tf}`;
            db.prepare('INSERT OR REPLACE INTO signals (key, symbol, root_tf, detected_at, state, meta) VALUES (?, ?, ?, ?, ?, ?)')
              .run(keyVal, symbol, root_tf, detected_at, 'detected', JSON.stringify(meta));
          } catch (e) {
            logger.debug({ e }, 'Fallback signals insert failed (non-fatal)');
          }
        } else {
          logger.debug('No DB persistence available for insertSignal');
        }
      } catch (e) {
        logger.debug({ e }, 'Signal persistence failed (non-fatal)');
      }

      // Build the signal object
      const signalObj = { key, symbol, root_tf, detected_at, state: 'detected', meta };

      // 8) Notify (telegram)
      if (notifyImmediately) {
        try {
          await telegram.sendRootSignalBlock({
            symbol,
            root_tf,
            alignment,
            detected_at,
            accept,
            marketData: normalizedMdata,
            tvScore: tv.score || 0,
            tvScorePct: tv.score_pct || 0,
            tvSource: tv.source || 'error',
            mtfScore
          });
          logger.info({ symbol, root_tf, tvScorePct: tv.score_pct }, 'Telegram root signal block sent');
        } catch (err) {
          logger.warn({ err, symbol }, 'handleRootSignal: failed to send telegram block');
        }
      } else {
        logger.debug({ symbol, root_tf }, 'handleRootSignal: notifyImmediately=false, returning signal object');
        return signalObj;
      }

      // 9) Open trade if accepted and allowed
      if (accept && accept.decision === 'accept') {
        if (!config.OPENTRADE) {
          logger.info({ symbol }, 'Accept but OPENTRADE disabled; skipping openTrade');
        } else if (!openTradesAllowed) {
          logger.info({ symbol }, 'Accept but open trades not yet enabled (waiting for first boundary)');
        } else {
          // apply filters
          let passFilters = true;

          if (config.MIN_MARKET_CAP > 0) {
            if (!normalizedMdata || !normalizedMdata.market_cap || Number(normalizedMdata.market_cap) < config.MIN_MARKET_CAP) {
              passFilters = false;
              logger.info({ symbol, market_cap: normalizedMdata?.market_cap }, 'Filtered out by MIN_MARKET_CAP');
            }
          }

          if (config.MIN_24H_USDT_VOLUME > 0) {
            if (!normalizedMdata || !normalizedMdata.volume_24h_usdt || Number(normalizedMdata.volume_24h_usdt) < config.MIN_24H_USDT_VOLUME) {
              passFilters = false;
              logger.info({ symbol, volume_24h_usdt: normalizedMdata?.volume_24h_usdt }, 'Filtered out by MIN_24H_USDT_VOLUME');
            }
          }

          if (isFinite(config.MIN_24H_VOLUME_CHANGE_PCT)) {
            const change = normalizedMdata?.volume_change_pct;
            if (change === null || change === undefined) {
              if (config.MIN_24H_VOLUME_CHANGE_PCT > 0) {
                passFilters = false;
                logger.info({ symbol }, 'No previous volume to compute change; filtered by MIN_24H_VOLUME_CHANGE_PCT');
              }
            } else {
              if (change < config.MIN_24H_VOLUME_CHANGE_PCT) {
                passFilters = false;
                logger.info({ symbol, volume_change_pct: change }, 'Filtered out by MIN_24H_VOLUME_CHANGE_PCT');
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
      // keep inProgress lock for a while to avoid dup work
      setTimeout(() => inProgress.delete(key), 60 * 60 * 1000);
    }
  },

  /**
   * evaluateMtfAlignment
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
   * applyDecision: determine acceptance
   */
  async applyDecision(alignment) {
    const tfList = Object.keys(alignment);
    if (!tfList || tfList.length === 0) return { decision: 'reject', reason: 'no_mtf_data' };

    const allPositive = tfList.every(tf => alignment[tf] && alignment[tf].positive);
    if (allPositive) return { decision: 'accept', reason: 'all_positive' };

    const negatives = tfList.filter(tf => alignment[tf] && !alignment[tf].positive);
    if (negatives.length === 1 && negatives[0].toUpperCase() === 'D') {
      const d = alignment['D'];
      if (d && d.rising) return { decision: 'accept', reason: 'daily_rising' };
      return { decision: 'monitor', reason: 'daily_not_rising' };
    }

    if (negatives.length >= 1) return { decision: 'monitor', reason: 'some_negative' };

    return { decision: 'reject', reason: 'unknown' };
  },

  /**
   * sendStartupSummary: builds snapshot and forwards to telegram sendStartupSummary()
   * - Ensures snapshot filled by trying multiple retrieval methods
   */
  async sendStartupSummary() {
    try {
      let snapshot = [];
      try {
        // Primary attempt
        if (dbModule && typeof dbModule.getLatestSignalsSnapshot === 'function') {
          snapshot = dbModule.getLatestSignalsSnapshot() || [];
        }
      } catch (e) {
        logger.debug({ e }, 'sendStartupSummary: primary snapshot retrieval failed');
      }

      if (!Array.isArray(snapshot) || snapshot.length === 0) {
        snapshot = await fetchLatestSignalsSnapshotFallback();
      }

      const telegramSvc = require('./telegram');
      await telegramSvc.sendStartupSummary({ snapshot });
    } catch (e) {
      logger.debug({ e }, 'sendStartupSummary failed');
    }
  },

  async handleNewRootCandle(newRootTfs = []) {
    try {
      let snapshot = [];
      try {
        if (dbModule && typeof dbModule.getLatestSignalsSnapshot === 'function') {
          snapshot = dbModule.getLatestSignalsSnapshot() || [];
        }
      } catch (e) {
        logger.debug({ e }, 'handleNewRootCandle: primary snapshot retrieval failed');
      }

      if (!Array.isArray(snapshot) || snapshot.length === 0) {
        snapshot = await fetchLatestSignalsSnapshotFallback();
      }

      const telegramSvc = require('./telegram');
      await telegramSvc.sendRootCandleUpdate({ snapshot, newRootTfs });
    } catch (e) {
      logger.debug({ e, newRootTfs }, 'handleNewRootCandle failed');
    }
  }
};
