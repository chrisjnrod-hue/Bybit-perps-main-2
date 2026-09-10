/**
 * src/services/signalManager.js
 * v0.4.0 Updates:
 * - Feature 1: Prioritizes root TFS (240, 1D, 60 weighted higher)
 * - Feature 2: Enforces MTF flip validation for 1D signals (1H, 4H, 1D flips only)
 * - Feature 3: Tracks signal state per 5-min boundary to deduplicate
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
const alignmentAlertsSent = new Map(); // Track alignment confirmations per symbol per 5-min block

async function fetchLatestSignalsSnapshotFallback(limit = 500) {
  try {
    if (dbModule && typeof dbModule.getLatestSignalsSnapshot === 'function') {
      try {
        const snap = dbModule.getLatestSignalsSnapshot();
        if (Array.isArray(snap) && snap.length) return snap;
      } catch (e) {
        logger.debug({ e }, 'fetchLatestSignalsSnapshotFallback: getLatestSignalsSnapshot failed');
      }
    }

    if (dbModule && typeof dbModule.get === 'function') {
      try {
        const db = dbModule.get();
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

/**
 * Get current 5-min boundary block ID (e.g., "2026-09-10_14:35:00")
 */
function getCurrentBoundaryBlockId() {
  const now = new Date();
  const mins = Math.floor(now.getUTCMinutes() / 5) * 5;
  const pad = (n) => String(n).padStart(2, '0');
  const date = `${now.getUTCFullYear()}-${pad(now.getUTCMonth() + 1)}-${pad(now.getUTCDate())}`;
  const time = `${pad(now.getUTCHours())}:${pad(mins)}:00`;
  return `${date}_${time}`;
}

/**
 * Feature 1: Prioritize root TFS by weight
 */
function evaluateTimeframeHierarchy(alignment) {
  const priorityMap = {};
  const rootPriority = config.ROOT_TFS_PRIORITY || ['240', 'D', '60'];
  
  for (const tf of Object.keys(alignment)) {
    let priority = 1;
    const tfStr = String(tf);
    
    if (rootPriority.includes(tfStr)) {
      const idx = rootPriority.indexOf(tfStr);
      priority = Math.max(3 - idx, 1);
    }
    
    priorityMap[tf] = priority;
  }
  
  return priorityMap;
}

/**
 * Feature 2: Validate MTF flip enforcement for 1D signals
 */
async function validateMtfFlipEnforcement(symbol, root_tf, alignment) {
  if (!config.ENFORCE_MTF_FLIP_1D) {
    return { valid: true, reason: 'MTF flip enforcement disabled' };
  }

  const rootTfStr = String(root_tf).toUpperCase();
  if (rootTfStr !== 'D' && rootTfStr !== '1D') {
    return { valid: true, reason: 'Not a 1D signal; enforcement N/A' };
  }

  const requiredTfs = ['60', '240', 'D'];
  const alignedTfs = Object.keys(alignment).filter(tf => alignment[tf] && alignment[tf].positive);

  const allRequired = requiredTfs.every(tf => alignedTfs.includes(tf));
  if (!allRequired) {
    logger.info({
      symbol,
      root_tf,
      required: requiredTfs,
      aligned: alignedTfs
    }, 'validateMtfFlipEnforcement: 1D signal rejected - not all MTF (1H/4H/1D) aligned');
    return { valid: false, reason: '1D signal requires 1H, 4H, 1D all aligned' };
  }

  return { valid: true, reason: '1D signal MTF flip validated' };
}

module.exports = {
  start() {
    logger.info('SignalManager started');
  },

  setOpenTradesAllowed,

  /**
   * handleRootSignal: Applies all 3 features
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

      // 2) TV rating
      let tv = { score: 0, score_pct: 0, source: 'error' };
      try {
        logger.debug({ symbol }, 'handleRootSignal: fetching TV rating');
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

      // 3) Subscribe to MTF websockets
      try { wsManager.subscribeSymbolMTF(symbol, config.MTF_TFS); } catch (e) { logger.debug({ e }, 'subscribeSymbolMTF failed (non-fatal)'); }

      // 4) Evaluate MTF alignment
      const alignment = await this.evaluateMtfAlignment(symbol);
      const mtfTfs = Object.keys(alignment || {});
      const positiveCount = mtfTfs.reduce((acc, t) => acc + (alignment[t] && alignment[t].positive ? 1 : 0), 0);
      const mtfScore = mtfTfs.length ? (positiveCount / mtfTfs.length) : 0;

      // 5) Compute fallback TV-like score if needed
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
      const accept = await this.applyDecision(alignment, symbol, root_tf);

      // 7) Feature 2: Validate MTF flip enforcement for 1D signals
      let mtfFlipValidation = { valid: true, reason: 'N/A' };
      if (accept && accept.decision === 'accept') {
        mtfFlipValidation = await validateMtfFlipEnforcement(symbol, root_tf, alignment);
        if (!mtfFlipValidation.valid) {
          accept.decision = 'reject';
          accept.reason = mtfFlipValidation.reason;
          logger.info({ symbol, root_tf, reason: mtfFlipValidation.reason }, 'Decision rejected due to MTF flip enforcement');
        }
      }

      // 8) Compose meta and persist
      const meta = {
        tvScore: tv.score || 0,
        tvScorePct: tv.score_pct || 0,
        tvSource: tv.source || 'error',
        mtfScore,
        alignment,
        acceptReason: accept && accept.reason ? accept.reason : null,
        decision: accept && accept.decision ? accept.decision : 'monitor',
        mtfFlipValidation,
        marketData: normalizedMdata,
        boundaryBlockId: getCurrentBoundaryBlockId()
      };

      // Persist signal
      try {
        if (dbModule && typeof dbModule.insertSignal === 'function') {
          dbModule.insertSignal({ symbol, root_tf, detected_at, state: 'detected', meta });
        } else if (dbModule && typeof dbModule.get === 'function') {
          const db = dbModule.get();
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
        }
      } catch (e) {
        logger.debug({ e }, 'Signal persistence failed (non-fatal)');
      }

      const signalObj = { key, symbol, root_tf, detected_at, state: 'detected', meta };

      // 9) Notify (telegram)
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
      }

      // 10) Open trade if accepted
      if (accept && accept.decision === 'accept') {
        if (!config.OPENTRADE) {
          logger.info({ symbol }, 'Accept but OPENTRADE disabled; skipping openTrade');
        } else if (!openTradesAllowed) {
          logger.info({ symbol }, 'Accept but open trades not yet enabled (waiting for first boundary)');
        } else {
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
   * Feature 1: Uses timeframe hierarchy for weighted evaluation
   */
  async applyDecision(alignment, symbol = null, root_tf = null) {
    const tfList = Object.keys(alignment);
    if (!tfList || tfList.length === 0) return { decision: 'reject', reason: 'no_mtf_data' };

    const hierarchy = config.PRIORITIZE_ROOT_TFS ? evaluateTimeframeHierarchy(alignment) : {};
    
    const allPositive = tfList.every(tf => alignment[tf] && alignment[tf].positive);
    if (allPositive) {
      const priorityInfo = config.PRIORITIZE_ROOT_TFS ? ` (hierarchy: ${JSON.stringify(hierarchy)})` : '';
      return { decision: 'accept', reason: `all_positive${priorityInfo}` };
    }

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
   * sendStartupSummary: builds snapshot and forwards to telegram
   */
  async sendStartupSummary() {
    try {
      let snapshot = [];
      try {
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
  },

  /**
   * Feature 3 helper: Check for alignment confirmation
   * Sends alert when ALL MTF TFS align for a symbol
   */
  async checkAlignmentConfirmation(symbol) {
    if (!config.ALIGNMENT_CONFIRMATION_ALERT) {
      return;
    }

    try {
      const alignment = await this.evaluateMtfAlignment(symbol);
      const mtfTfs = Object.keys(alignment || {});
      
      if (mtfTfs.length === 0) return;

      const allPositive = mtfTfs.every(tf => alignment[tf] && alignment[tf].positive);
      if (!allPositive) return;

      const blockId = getCurrentBoundaryBlockId();
      const confirmKey = `${blockId}:${symbol}`;

      if (alignmentAlertsSent.has(confirmKey)) {
        return;
      }

      alignmentAlertsSent.set(confirmKey, true);

      const telegramSvc = require('./telegram');
      await telegramSvc.sendAlignmentConfirmation({
        symbol,
        alignment,
        blockId
      });

      logger.info({ symbol, blockId }, 'Alignment confirmation sent');

      // Cleanup old entries (older than 30 min)
      const now = new Date();
      const cutoff = now.getTime() - 30 * 60 * 1000;
      for (const [key] of alignmentAlertsSent) {
        try {
          const blockPart = key.split(':')[0];
          const [datePart, timePart] = blockPart.split('_');
          const [year, month, day] = datePart.split('-').map(Number);
          const [hour, min] = timePart.split(':').map(Number);
          const blockTime = new Date(Date.UTC(year, month - 1, day, hour, min, 0)).getTime();
          
          if (blockTime < cutoff) {
            alignmentAlertsSent.delete(key);
          }
        } catch (e) {
          // ignore cleanup errors
        }
      }
    } catch (e) {
      logger.debug({ e, symbol }, 'checkAlignmentConfirmation error');
    }
  }
};
