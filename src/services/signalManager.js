// signalManager.js
const dbModule = require('../db');
const macd = require('./macd');
const tradeManager = require('./tradeManager');
const marketData = require('./marketData');
const tradingview = require('./tradingview');
const notificationQueue = require('./notificationQueue');
const config = require('../config');
const logger = require('pino')();

let openTradesAllowed = true;

function setOpenTradesAllowed(value) {
  openTradesAllowed = !!value;

  logger.info(
    { openTradesAllowed },
    'signalManager: openTradesAllowed set'
  );
}

/*
 * This lock only prevents the same event from being processed
 * concurrently. It must not use only symbol:root_tf because a symbol
 * can legitimately produce:
 *
 *   root_open:BTCUSDT:60:candle
 *   midcandle:BTCUSDT:60:candle
 *
 * during the same root candle.
 */
const inProgress = new Map();

function normalizeEventType(signalType) {
  if (
    typeof signalType === 'string' &&
    signalType.trim()
  ) {
    return signalType.trim();
  }

  return null;
}

function buildEventKey({
  symbol,
  root_tf,
  eventId,
  candle_open_time,
  detected_at
}) {
  const eventPart =
    eventId !== null &&
    eventId !== undefined &&
    String(eventId).trim()
      ? String(eventId).trim()
      : (
          candle_open_time !== null &&
          candle_open_time !== undefined &&
          Number.isFinite(Number(candle_open_time))
            ? `candle:${Number(candle_open_time)}`
            : `detected:${Number(detected_at)}`
        );

  return [
    String(symbol || ''),
    String(root_tf || ''),
    eventPart
  ].join(':');
}

function buildFallbackMarketData() {
  return {
    price: 0,
    volume_24h_usdt: 0,
    volume_change_pct: null,
    market_cap: null
  };
}

function calculateMtfScore(alignment) {
  const values = Object.values(alignment || {});

  if (values.length === 0) {
    return 0;
  }

  const positiveCount = values.filter((value) => {
    return value && value.positive;
  }).length;

  return positiveCount / values.length;
}

// NEW: small helper to provide account equity (USD) to other modules
async function getAccountEquityUsd() {
  try {
    if (tradeManager && typeof tradeManager.getAccountInfo === 'function') {
      const info = await tradeManager.getAccountInfo();
      if (info) {
        // adapt to multiple possible shapes returned by tradeManager
        const equityCandidates = [
          info.equityUsd,
          info.equity,
          info.totalEquityUsd,
          info.balanceUsd,
          info.balance,
          info.usdBalance
        ];
        for (const cand of equityCandidates) {
          if (cand !== undefined && cand !== null && Number.isFinite(Number(cand))) {
            const val = Number(cand);
            if (val > 0) return val;
          }
        }
      }
    }
  } catch (err) {
    logger.debug({ err }, 'signalManager.getAccountEquityUsd: tradeManager.getAccountInfo failed');
  }

  const fallback = Number(config.DEFAULT_ACCOUNT_EQUITY_USD || 10000);
  return fallback;
}

// NEW: TradingView normalization helper
function normalizeTvResult(tvRes = null, opts = {}) {
  // Normalize to:
  // { score: 0..1, score_pct: 0..100, tvScore: 0..100, source: '...', raw: tvRes, fresh: boolean }
  if (!tvRes) {
    return { score: 0, score_pct: 0, tvScore: 0, source: 'error', raw: null, fresh: false };
  }

  const out = {
    score: 0,
    score_pct: 0,
    tvScore: 0,
    source: tvRes.source || tvRes.tvSource || 'unknown',
    raw: tvRes,
    fresh: true
  };

  // Accept multiple shapes
  let rawVal = null;
  if (typeof tvRes.tvScore === 'number') rawVal = Number(tvRes.tvScore);
  else if (typeof tvRes.score_pct === 'number') rawVal = Number(tvRes.score_pct);
  else if (typeof tvRes.scorePct === 'number') rawVal = Number(tvRes.scorePct);
  else if (typeof tvRes.score === 'number') rawVal = Number(tvRes.score);
  else if (typeof tvRes.score === 'string' && !isNaN(Number(tvRes.score))) rawVal = Number(tvRes.score);

  if (rawVal !== null && Number.isFinite(rawVal)) {
    if (rawVal >= 0 && rawVal <= 1) {
      out.score = rawVal;
      out.score_pct = Math.round(rawVal * 100);
      out.tvScore = out.score_pct;
    } else {
      const pct = Math.round(Math.max(0, Math.min(100, rawVal)));
      out.score_pct = pct;
      out.tvScore = pct;
      out.score = pct / 100;
    }
  } else {
    out.score = 0;
    out.score_pct = 0;
    out.tvScore = 0;
  }

  // Freshness: optional TTL check
  const ttl = Number(opts.tvCacheTtlMs || config.TV_CACHE_TTL_MS || (15 * 60 * 1000));
  if (ttl > 0) {
    const tsCandidates = [
      tvRes.updatedAt,
      tvRes.updated_at,
      tvRes.timestamp,
      tvRes.ts,
      tvRes.fetched_at
    ];
    const ts = tsCandidates.find((v) => typeof v === 'number' || (typeof v === 'string' && !isNaN(Number(v))));
    if (ts) {
      const tsNum = Number(ts);
      if (Date.now() - tsNum > ttl) {
        out.fresh = false;
      }
    }
  }

  return out;
}

/**
 * Compute combined confidence (0..100) used by poller/signalManager.
 * - mtfScore is 0..1
 * - tvNormalized is the result of normalizeTvResult(...)
 *
 * Options:
 * - weights: { mtf: 0..1, tv: 0..1 } (defaults mtf:0.8, tv:0.2)
 * - tvStalePenalty: fraction to reduce tv weight when tvNormalized.fresh===false (default 0.5)
 */
function computeCombinedConfidence(mtfScore = 0, tvNormalized = null, opts = {}) {
  const weights = opts.weights || { mtf: 0.8, tv: 0.2 };
  const tvStalePenalty = typeof opts.tvStalePenalty === 'number' ? opts.tvStalePenalty : 0.5;

  const mtfPart = Math.max(0, Math.min(1, Number(mtfScore || 0))) * (weights.mtf || 0.8);

  let tvPart = 0;
  if (tvNormalized && typeof tvNormalized.score === 'number') {
    const tvWeight = (weights.tv || 0.2) * (tvNormalized.fresh === false ? tvStalePenalty : 1);
    tvPart = Math.max(0, Math.min(1, Number(tvNormalized.score || 0))) * tvWeight;
  }

  // Normalize to 0..1 range relative to sum of weights
  const weightSum = (weights.mtf || 0.8) + (weights.tv || 0.2) * (tvNormalized && tvNormalized.fresh === false ? tvStalePenalty : 1);
  const normalized = weightSum > 0 ? (mtfPart + tvPart) / weightSum : 0;

  return Math.round(Math.max(0, Math.min(100, normalized * 100)));
}

module.exports = {
  start() {
    logger.info('SignalManager started');
  },

  setOpenTradesAllowed,

  // expose helper for account equity
  getAccountEquityUsd,

  // expose tv normalizer & confidence
  normalizeTvResult,
  computeCombinedConfidence,

  /**
   * Process a root, root-open, or mid-candle signal.
   *
   * notifyImmediately:
   *   true  - enqueue immediately
   *   false - return the signal to the caller so the caller can batch it
   *
   * signalType:
   *   new_root_candle
   *   midcandle_update
   *   mtf_alignment
   *   or another queue-supported type
   *
   * New optional params:
   *   decisionOverride: { decision: 'accept'|'monitor'|'reject', reason: '...' }
   *   metaOverride: arbitrary object to merge into persisted meta
   */
  async handleRootSignal({
    symbol,
    root_tf,
    detected_at = Date.now(),
    candle_open_time = null,
    eventId = null,
    signalType = null,
    notifyImmediately = true,
    decisionOverride = null,
    metaOverride = null
  } = {}) {
    if (!symbol || !root_tf) {
      logger.warn(
        {
          symbol,
          root_tf
        },
        'handleRootSignal: symbol and root_tf are required'
      );

      return null;
    }

    const normalizedSignalType =
      normalizeEventType(signalType);

    const key = buildEventKey({
      symbol,
      root_tf,
      eventId,
      candle_open_time,
      detected_at
    });

    if (inProgress.has(key)) {
      logger.debug(
        { key, symbol, root_tf, eventId },
        'handleRootSignal: event already in progress'
      );

      return null;
    }

    inProgress.set(key, true);

    try {
      logger.info(
        {
          symbol,
          root_tf,
          eventId,
          signalType: normalizedSignalType,
          candle_open_time
        },
        'Root signal received'
      );

      /*
       * Fetch fresh market data. Failure is non-fatal because the
       * signal should still be available for notification and review.
       */
      let mdata = null;

      try {
        mdata =
          await marketData.updateSymbolMarketData(
            symbol
          );

        if (!mdata) {
          mdata = buildFallbackMarketData();
        }
      } catch (err) {
        logger.warn(
          {
            err,
            symbol
          },
          'handleRootSignal: market data fetch failed, using zeros'
        );

        mdata = buildFallbackMarketData();
      }

      /*
       * Fetch the cached or fresh TradingView rating and normalize it.
       */
      let tv = { score: 0, score_pct: 0, tvScore: 0, source: 'error', raw: null, fresh: false };

      try {
        logger.debug(
          { symbol },
          'handleRootSignal: fetching TV rating'
        );

        // Prefer the cached helper if tradingview module exposes it.
        let tvRes;
        if (tradingview && typeof tradingview.getOrFetchTvRatingCached === 'function') {
          tvRes = await tradingview.getOrFetchTvRatingCached(symbol);
        } else if (tradingview && typeof tradingview.fetchTvRatingForSymbol === 'function') {
          tvRes = await tradingview.fetchTvRatingForSymbol(symbol);
        } else {
          tvRes = null;
        }

        const normalized = normalizeTvResult(tvRes);
        tv = {
          score: normalized.score,
          score_pct: normalized.score_pct,
          tvScore: normalized.tvScore,
          source: normalized.source,
          raw: normalized.raw,
          fresh: normalized.fresh
        };

        logger.info(
          {
            symbol,
            tvScore: tv.tvScore,
            source: tv.source,
            fresh: tv.fresh
          },
          'TV rating acquired (normalized)'
        );
      } catch (err) {
        logger.warn(
          {
            err: err && err.message,
            symbol
          },
          'handleRootSignal: TV rating fetch failed'
        );
      }

      /*
       * Evaluate MTF alignment using the current MACD histogram data.
       * If caller provided a decisionOverride, we will still fetch alignment
       * to include in persisted meta, but we will not let it override the
       * provided decision.
       */
      let alignment = {};
      let mtfScore = 0;
      let computedDecision = null;
      let computedReason = null;

      try {
        alignment = await this.evaluateMtfAlignment(symbol);
        mtfScore = calculateMtfScore(alignment);

        // Only compute applyDecision if the caller did not pass a decisionOverride
        if (!decisionOverride) {
          const decisionObj = await this.applyDecision(alignment);
          computedDecision = decisionObj && decisionObj.decision ? decisionObj.decision : 'monitor';
          computedReason = decisionObj && decisionObj.reason ? decisionObj.reason : null;
        } else {
          computedDecision = decisionOverride.decision || 'monitor';
          computedReason = decisionOverride.reason || null;
        }
      } catch (err) {
        logger.debug({ err, symbol }, 'handleRootSignal: evaluateMtfAlignment/applyDecision failed');
        // fallback
        computedDecision = computedDecision || (decisionOverride ? decisionOverride.decision : 'monitor');
      }

      /*
       * If TradingView failed or returned nothing, compute a fallback score
       * using MTF-positive fraction and available volume change.
       */
      if (
        (!tv || Number(tv.score_pct || tv.tvScore || 0) === 0) &&
        typeof tradingview.fallbackScore === 'function'
      ) {
        try {
          const fallback = tradingview.fallbackScore({
            macdPositiveFraction: typeof mtfScore === 'number' ? mtfScore : 0,
            volChangePct: Number.isFinite(Number(mdata?.volume_change_pct))
              ? Number(mdata.volume_change_pct)
              : 0
          });

          if (
            fallback &&
            Number.isFinite(Number(fallback.score_pct))
          ) {
            const norm = normalizeTvResult(fallback);
            tv = {
              score: norm.score,
              score_pct: norm.score_pct,
              tvScore: norm.tvScore,
              source: 'computed_fallback',
              raw: norm.raw,
              fresh: norm.fresh
            };

            logger.info(
              {
                symbol,
                mtfScore,
                volume_change_pct: mdata?.volume_change_pct,
                fallbackScore: tv.score_pct
              },
              'handleRootSignal: computed fallback TV score (normalized)'
            );
          }
        } catch (err) {
          logger.debug(
            {
              err,
              symbol
            },
            'handleRootSignal: fallback TV score generation failed'
          );
        }
      }

      /*
       * Build meta and persist signal. If caller provided metaOverride, merge it in
       * so poller or other callers can preserve enriched metadata.
       */
      const baseMeta = {
        tvScore: Number(tv.tvScore || 0),
        score_pct: Number(tv.score_pct || 0),
        scorePct: Number(tv.score_pct || 0),
        tvScoreNormalized: Number(tv.score || 0),
        tvSource: tv.source || 'error',
        mtfScore,
        alignment,
        acceptReason: computedReason,
        decision: computedDecision || (decisionOverride ? decisionOverride.decision : 'monitor'),
        marketData: mdata || {}
      };

      const meta = Object.assign({}, baseMeta, metaOverride || {});

      /*
       * Keep this insert compatible with the existing database API.
       * Event metadata remains on signalObj and is available to the
       * notification queue even if the current schema has no eventId
       * or candle_open_time columns.
       */
      try {
        if (dbModule && typeof dbModule.insertSignal === 'function') {
          dbModule.insertSignal({
            symbol,
            root_tf,
            detected_at,
            state: 'detected',
            meta
          });
        } else {
          logger.debug('handleRootSignal: dbModule.insertSignal not available; skipping DB insert');
        }
      } catch (err) {
        logger.debug({ err, symbol, root_tf }, 'handleRootSignal: insertSignal failed (non-fatal)');
      }

      const signalObj = {
        key,
        symbol,
        root_tf,
        detected_at,
        candle_open_time,
        eventId,
        signalType: normalizedSignalType,
        notificationType: normalizedSignalType,
        state: 'detected',
        meta
      };

      /*
       * Immediate notifications are used by callers that do not need
       * a summary batch. Boundary and root-open scans pass false and
       * enqueue the returned object themselves.
       */
      if (notifyImmediately) {
        const queueType =
          normalizedSignalType || 'realtime';

        logger.debug(
          {
            symbol,
            root_tf,
            eventId,
            queueType,
            notificationType: signalObj.notificationType
          },
          'handleRootSignal: enqueuing immediate notification'
        );

        notificationQueue.enqueueSignal(
          signalObj,
          queueType
        );
      } else {
        logger.debug(
          {
            symbol,
            root_tf,
            eventId,
            signalType: normalizedSignalType
          },
          'handleRootSignal: notifyImmediately=false, returning signal'
        );
      }

      /*
       * Trade opening is still performed only for accepted signals.
       * Use the final decision (preference: decisionOverride -> computedDecision).
       */
      const finalDecision = (decisionOverride && decisionOverride.decision) ? decisionOverride.decision : (computedDecision || 'monitor');

      if (
        finalDecision &&
        finalDecision === 'accept'
      ) {
        if (!config.OPENTRADE) {
          logger.info(
            {
              symbol,
              root_tf,
              eventId
            },
            'Accept but OPENTRADE disabled; skipping openTrade'
          );
        } else if (!openTradesAllowed) {
          logger.info(
            {
              symbol,
              root_tf,
              eventId
            },
            'Accept but open trades are not yet enabled'
          );
        } else {
          let passFilters = true;

          if (config.MIN_MARKET_CAP > 0) {
            if (
              !mdata ||
              !mdata.market_cap ||
              Number(mdata.market_cap) <
                config.MIN_MARKET_CAP
            ) {
              passFilters = false;

              logger.info(
                {
                  symbol,
                  market_cap: mdata?.market_cap
                },
                'Filtered out by MIN_MARKET_CAP for opening'
              );
            }
          }

          if (config.MIN_24H_USDT_VOLUME > 0) {
            if (
              !mdata ||
              !mdata.volume_24h_usdt ||
              Number(mdata.volume_24h_usdt) <
                config.MIN_24H_USDT_VOLUME
            ) {
              passFilters = false;

              logger.info(
                {
                  symbol,
                  volume_24h_usdt:
                    mdata?.volume_24h_usdt
                },
                'Filtered out by MIN_24H_USDT_VOLUME for opening'
              );
            }
          }

          if (
            Number.isFinite(
              Number(config.MIN_24H_VOLUME_CHANGE_PCT)
            )
          ) {
            const minimumVolumeChange =
              Number(config.MIN_24H_VOLUME_CHANGE_PCT);

            const change =
              mdata?.volume_change_pct;

            if (
              change === null ||
              change === undefined
            ) {
              if (minimumVolumeChange > 0) {
                passFilters = false;

                logger.info(
                  {
                    symbol
                  },
                  'No previous volume available; filtered by MIN_24H_VOLUME_CHANGE_PCT'
                );
              }
            } else if (
              Number(change) <
              minimumVolumeChange
            ) {
              passFilters = false;

              logger.info(
                {
                  symbol,
                  volume_change_pct: change
                },
                'Filtered out by MIN_24H_VOLUME_CHANGE_PCT for opening'
              );
            }
          }

          if (passFilters) {
            try {
              await tradeManager.openTrade({
                symbol,
                root_tf,
                alignment,
                meta,
                eventId,
                signalType: normalizedSignalType
              });

              logger.info(
                {
                  symbol,
                  root_tf,
                  eventId
                },
                'handleRootSignal: trade opening initiated'
              );
            } catch (err) {
              logger.error(
                {
                  err,
                  symbol,
                  root_tf,
                  eventId
                },
                'handleRootSignal: openTrade error'
              );
            }
          } else {
            logger.info(
              {
                symbol,
                root_tf,
                eventId
              },
              'Accepted signal blocked by market filters'
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
          eventId,
          signalType: normalizedSignalType
        },
        'handleRootSignal error'
      );

      return null;
    } finally {
      /*
       * Delete only this event's lock. A later root-open or mid-candle
       * event for the same symbol/timeframe remains independent.
       */
      inProgress.delete(key);
    }
  },

  /**
   * Return detailed MTF alignment for the configured MTF timeframes.
   */
  async evaluateMtfAlignment(symbol) {
    const result = {};
    const mtfTfs = Array.isArray(config.MTF_TFS)
      ? config.MTF_TFS
      : [];

    for (const configuredTf of mtfTfs) {
      const tf = String(configuredTf);

      try {
        const hist =
          await macd.computeMacdHistogram(
            symbol,
            tf
          );

        if (
          !Array.isArray(hist) ||
          hist.length === 0
        ) {
          result[tf] = {
            ok: false,
            positive: false
          };

          continue;
        }

        const last = hist[hist.length - 1];
        const previous =
          hist[hist.length - 2] || last;

        const lastHistogram =
          Number(last?.histogram);

        const previousHistogram =
          Number(previous?.histogram);

        result[tf] = {
          histogram: lastHistogram,
          macd: last?.MACD,
          signal: last?.signal,
          rising:
            Number.isFinite(lastHistogram) &&
            Number.isFinite(previousHistogram)
              ? lastHistogram > previousHistogram
              : false,
          positive:
            Number.isFinite(lastHistogram)
              ? lastHistogram > 0
              : false,
          ok:
            Number.isFinite(lastHistogram)
        };
      } catch (err) {
        logger.debug(
          {
            err,
            symbol,
            tf
          },
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

  /**
   * Determine signal acceptance from MTF alignment.
   *
   * Rules:
   * - all configured timeframes positive: accept
   * - only daily timeframe negative and rising: accept
   * - one or more negative timeframes: monitor
   * - no usable data: reject
   */
  async applyDecision(alignment = {}) {
    const tfList = Object.keys(alignment || {});

    if (tfList.length === 0) {
      return {
        decision: 'reject',
        reason: 'no_mtf_data'
      };
    }

    const usableTfList = tfList.filter((tf) => {
      return alignment[tf] && alignment[tf].ok !== false;
    });

    if (usableTfList.length === 0) {
      return {
        decision: 'reject',
        reason: 'no_mtf_data'
      };
    }

    const allPositive = usableTfList.every((tf) => {
      return (
        alignment[tf] &&
        alignment[tf].positive
      );
    });

    if (allPositive) {
      return {
        decision: 'accept',
        reason: 'all_positive'
      };
    }

    const negatives = usableTfList.filter((tf) => {
      return !(
        alignment[tf] &&
        alignment[tf].positive
      );
    });

    if (
      negatives.length === 1 &&
      String(negatives[0]).toUpperCase() === 'D'
    ) {
      const daily = alignment[negatives[0]];

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
