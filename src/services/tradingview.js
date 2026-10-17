// tradingview.js
const fetch = require('node-fetch');
const logger = require('pino')();
const dbModule = require('../db');

const TV_SCANNER_URL = 'https://scanner.tradingview.com/crypto/scan';
const TV_SYMBOL_SEARCH_URL = 'https://symbol-search.tradingview.com/symbol_search/';

/*
 Config / env-driven defaults:
 - TRADINGVIEW_EXCHANGE_CANDIDATES (CSV string) controls which exchanges to try and in which order.
 - TV_USE_SYMBOL_SEARCH (boolean) attempts symbol_search fallback when direct scanner hits are empty.
 - TV_REQUEST_TIMEOUT_MS controls HTTP timeout for TV calls.
 - TV_CACHE_TTL_MS controls how long to cache TV results in DB (default 1 hour)
*/
const DEFAULT_EXCHANGES = (process.env.TRADINGVIEW_EXCHANGE_CANDIDATES || 'BYBIT,BINANCE,KUCOIN')
  .split(',')
  .map(s => s.trim())
  .filter(Boolean);

const TV_USE_SYMBOL_SEARCH = String(process.env.TV_USE_SYMBOL_SEARCH || 'true').toLowerCase() !== 'false';
const TV_SYMBOL_SEARCH_LIMIT = Number(process.env.TV_SYMBOL_SEARCH_LIMIT || 20);
const TV_REQUEST_TIMEOUT_MS = Number(process.env.TV_REQUEST_TIMEOUT_MS || 12000);
const TV_CACHE_TTL_MS = Number(process.env.TV_CACHE_TTL_MS || (60 * 60 * 1000)); // 1 hour

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

/**
 * normalizeRecommendToScore
 * Accepts a numeric TradingView Recommend.All value in 1..5 and returns 0..1 float and percent 0..100
 */
function normalizeRecommendToScore(recommend) {
  if (typeof recommend !== 'number' || !Number.isFinite(recommend)) {
    return { score: 0, scorePct: 0 };
  }
  // clip to 1..5
  const clamped = Math.max(1, Math.min(5, recommend));
  const score = (clamped - 1) / 4; // 1 -> 0.0, 5 -> 1.0
  const pct = Math.round(score * 100);
  return { score: Math.round(score * 100) / 100, scorePct: pct };
}

/**
 * parseRecommendFromResponse(json, preferColumnName)
 * Uses json.columns (if present) to find the exact index of the Recommend.All column.
 * Falls back to a safe scan only if no column info is present.
 */
function parseRecommendFromResponse(json, preferColumnName = 'Recommend.All') {
  if (!json || !Array.isArray(json.data) || json.data.length === 0) return null;

  const row = json.data[0];
  const d = Array.isArray(row.d) ? row.d : [];

  // If server provided column metadata, use it to find Recommend.All explicitly
  if (Array.isArray(json.columns) && json.columns.length > 0) {
    // json.columns entries may look like "Recommend.All|1" or "Recommend.All"
    const idx = json.columns.findIndex((c) => {
      if (!c) return false;
      const cc = String(c).toLowerCase();
      return cc.startsWith(preferColumnName.toLowerCase());
    });

    if (idx >= 0 && typeof d[idx] !== 'undefined' && d[idx] !== null) {
      const val = d[idx];
      const parsed = (typeof val === 'number') ? val
        : (typeof val === 'string' && val.trim() !== '') ? Number(val)
        : (val && typeof val === 'object' && typeof val.v !== 'undefined') ? Number(val.v)
        : NaN;
      return Number.isFinite(parsed) ? parsed : null;
    }
  }

  // Fallback: robust scanning but prefer numbers in 1..5 and ignore other small numerics that resemble RSI etc.
  for (const v of d) {
    if (typeof v === 'number' && v >= 1 && v <= 5) return v;
    if (typeof v === 'string') {
      const n = Number(v);
      if (Number.isFinite(n) && n >= 1 && n <= 5) return n;
    }
    if (v && typeof v === 'object') {
      // sometimes fields are objects {v: <value>, fmt: ...}
      const n = typeof v.v !== 'undefined' ? Number(v.v) : NaN;
      if (Number.isFinite(n) && n >= 1 && n <= 5) return n;
    }
  }

  return null;
}

/**
 * tryScannerForTicker (robust)
 * Uses explicit column metadata and adds Origin/Referer headers to better match web requests.
 */
async function tryScannerForTicker(ticker, timeoutMs = TV_REQUEST_TIMEOUT_MS) {
  const body = {
    symbols: { tickers: [ticker], query: { types: [] } },
    // keep Recommend.All first to increase chance server returns it in expected index
    columns: ['Recommend.All|1', 'Recommend.Other|1', 'RSI|14', 'momentum|14']
  };

  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const res = await fetch(TV_SCANNER_URL, {
      method: 'POST',
      body: JSON.stringify(body),
      headers: {
        'Content-Type': 'application/json',
        'User-Agent': 'Mozilla/5.0 (compatible; bot)',
        'Origin': 'https://www.tradingview.com',
        'Referer': 'https://www.tradingview.com/'
      },
      signal: controller.signal
    });

    clearTimeout(timeoutId);

    if (!res.ok) {
      logger.debug({ ticker, status: res.status }, 'TV scanner non-ok response');
      return null;
    }

    const json = await res.json().catch(() => null);
    if (!json) return null;

    // Prefer parsing using columns metadata
    const recommend = parseRecommendFromResponse(json, 'Recommend.All');

    if (recommend === null) {
      logger.debug({ ticker, jsonSnippet: { columns: json.columns?.slice?.(0,10), row: json.data?.[0]?.d?.slice?.(0,10) } }, 'TV scanner: Recommend.All not found in response');
      return null;
    }

    // convert 1..5 to 0..1 and pct
    const clamped = Math.max(1, Math.min(5, Number(recommend)));
    const score = (clamped - 1) / 4;
    const scorePct = Math.round(score * 100);

    return {
      source: 'tradingview',
      score,
      score_pct: scorePct,
      scorePct,
      tvScore: scorePct,
      raw: json.data?.[0]?.d || [],
      columns: json.columns || [],
      exchange: ticker.split(':')[0] || null
    };
  } catch (err) {
    const name = (err && err.name) ? err.name : 'Error';
    logger.debug({ ticker, err: err && err.message }, `TV scanner request failed (${name})`);
    clearTimeout(timeoutId);
    return null;
  }
}

/**
 * findCanonicalTvTicker(symbol, exchangeCandidates)
 * Optional helper that queries TradingView symbol search to find a matching ticker for the symbol on the given exchange.
 * Returns a string like "BYBIT:BTCUSDT" or null.
 */
async function findCanonicalTvTicker(symbol, exchangeCandidates = DEFAULT_EXCHANGES, timeoutMs = 5000) {
  if (!TV_USE_SYMBOL_SEARCH) return null;
  // try each exchange until we find a plausible match
  for (const ex of exchangeCandidates) {
    try {
      const q = `?text=${encodeURIComponent(symbol)}&exchange=${encodeURIComponent(ex)}&limit=${TV_SYMBOL_SEARCH_LIMIT}`;
      const url = `${TV_SYMBOL_SEARCH_URL}${q}`;
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), timeoutMs);

      const res = await fetch(url, { signal: controller.signal, headers: { 'User-Agent': 'Mozilla/5.0 (compatible; bot)' } });
      clearTimeout(timeoutId);

      if (!res.ok) continue;
      const json = await res.json().catch(() => null);
      if (!Array.isArray(json) || json.length === 0) continue;

      // prefer exact symbol match on 'symbol' or 'short_name'
      const preferred = json.find((item) => {
        // item.symbol is like "BYBIT:BTCUSDT" or long format
        const sym = String(item?.symbol ?? '').toUpperCase();
        const short = String(item?.short_name ?? '').toUpperCase();
        return sym.endsWith(`:${String(symbol).toUpperCase()}`) || short === String(symbol).toUpperCase();
      });

      const pick = preferred || json[0];
      if (pick && pick.symbol) {
        // ensure proper formatting "EXCHANGE:SYMBOL"
        const resolved = String(pick.symbol).toUpperCase();
        logger.debug({ symbol, exchange: ex, resolved, pick }, 'TV symbol_search picked ticker');
        return resolved;
      }
    } catch (err) {
      logger.debug({ err, symbol, ex }, 'TV symbol_search attempt failed; continuing to next exchange');
    }
  }
  return null;
}

/**
 * fetchTvRatingForSymbol with improved reliability:
 * - tries configured exchanges in priority order
 * - validates Recommend.All by scanning returned row.d and columns metadata
 * - optionally uses symbol_search fallback to find canonical ticker
 */
async function fetchTvRatingForSymbol(symbol, maxRetries = 3) {
  if (!symbol) {
    logger.warn('fetchTvRatingForSymbol: symbol is empty');
    return { source: 'error', score: 0, score_pct: 0, scorePct: 0, tvScore: 0 };
  }

  const exchangeCandidates = Array.isArray(DEFAULT_EXCHANGES) && DEFAULT_EXCHANGES.length > 0
    ? DEFAULT_EXCHANGES
    : ['BYBIT', 'BINANCE', 'KUCOIN'];

  logger.debug({ symbol, exchanges: exchangeCandidates }, 'fetchTvRatingForSymbol: starting fetch');

  // Try direct scanner calls first (exchange priority order)
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    for (const ex of exchangeCandidates) {
      try {
        const ticker = `${ex}:${symbol}`;
        logger.debug({ attempt, exchange: ex, ticker }, 'TV fetch attempt');

        const res = await tryScannerForTicker(ticker, TV_REQUEST_TIMEOUT_MS);
        if (res && res.source === 'tradingview') {
          logger.info({ symbol, ticker, score: res.score, scorePct: res.scorePct, attempt, exchange: ex }, '✅ TV rating fetched successfully');
          return res;
        }
      } catch (err) {
        logger.debug({ err, symbol, exchange: ex, attempt }, 'TV fetch attempt failed');
      }
    }

    // If direct attempts fail AND symbol_search is enabled, try to resolve canonical ticker then re-run scanner
    if (TV_USE_SYMBOL_SEARCH) {
      try {
        const resolved = await findCanonicalTvTicker(symbol, exchangeCandidates);
        if (resolved) {
          logger.debug({ symbol, resolved }, 'TV symbol_search resolved ticker; trying scanner on resolved ticker');
          const res = await tryScannerForTicker(resolved, TV_REQUEST_TIMEOUT_MS);
          if (res && res.source === 'tradingview') {
            logger.info({ symbol, resolved, score: res.score, scorePct: res.scorePct }, '✅ TV rating fetched via symbol_search -> scanner');
            return res;
          }
        }
      } catch (err) {
        logger.debug({ err, symbol }, 'TV symbol_search attempt failed');
      }
    }

    // backoff before next round
    if (attempt < maxRetries) {
      const backoffMs = 1000 * attempt;
      logger.debug({ symbol, attempt, nextRetry: backoffMs, maxRetries }, 'TV fetch retry scheduled');
      await sleep(backoffMs);
    }
  }

  logger.warn({ symbol, attempts: maxRetries, exchanges: exchangeCandidates }, '⚠️ TV rating unavailable after all retries, using fallback score of 0');
  return { source: 'fallback', score: 0, score_pct: 0, scorePct: 0, tvScore: 0, raw: null };
}

/**
 * getOrFetchTvRatingCached(symbol)
 * - Checks DB cache (configurable TTL)
 * - If fresh cache exists, returns it immediately (with aliases)
 * - Otherwise fetches fresh from TV API and caches result (only caches true TV results)
 * - Graceful fallback if DB unavailable
 *
 * Returns object with { score (0..1), score_pct (0..100), scorePct, tvScore, source, exchange, cached }
 */
async function getOrFetchTvRatingCached(symbol) {
  if (!symbol) return { source: 'error', score: 0, score_pct: 0, scorePct: 0, tvScore: 0 };

  try {
    const db = dbModule.get();
    if (!db) throw new Error('DB not available');

    // Ensure table exists (no-op if already present)
    try {
      db.prepare(`
        CREATE TABLE IF NOT EXISTS tv_ratings (
          symbol TEXT PRIMARY KEY,
          score REAL DEFAULT 0,
          source TEXT,
          exchange TEXT,
          updated_at INTEGER DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_tv_ratings_updated_at ON tv_ratings(updated_at);
      `).run();
    } catch (err) {
      logger.debug({ err }, 'tv_ratings table creation/check failed (continuing without cache)');
    }

    // Check cache (TTL from config or default)
    const cutoff = Date.now() - (Number(process.env.TV_CACHE_TTL_MS || TV_CACHE_TTL_MS));
    const cached = db.prepare(`
      SELECT score, source, exchange, updated_at
      FROM tv_ratings
      WHERE symbol = ? AND updated_at > ?
    `).get(symbol, cutoff);

    if (cached) {
      const ageMinutes = Math.round((Date.now() - cached.updated_at) / 60000);
      const rawScore = Number(cached.score) || 0;
      const scorePct = Math.round(rawScore * 100);
      logger.info({ symbol, source: cached.source, score: rawScore, scorePct, ageMinutes }, '⏱️ TV rating retrieved from cache');

      return {
        source: 'cache',
        score: rawScore,
        score_pct: scorePct,
        scorePct: scorePct,
        tvScore: scorePct,
        exchange: cached.exchange || null,
        cached: true
      };
    }
  } catch (err) {
    logger.debug({ err: err && err.message, symbol }, 'Cache check failed (will fetch fresh)');
  }

  // Fetch fresh from TV API
  logger.debug({ symbol }, 'Fetching fresh TV rating from API');
  const fresh = await fetchTvRatingForSymbol(symbol);

  // Try to cache result only if source === 'tradingview'
  try {
    const db = dbModule.get();
    if (db && fresh && fresh.source === 'tradingview') {
      db.prepare(`
        INSERT OR REPLACE INTO tv_ratings 
        (symbol, score, source, exchange, updated_at) 
        VALUES (?, ?, ?, ?, ?)
      `).run(symbol, fresh.score, fresh.source, fresh.exchange || null, Date.now());
      logger.debug({ symbol, score: fresh.score, source: fresh.source }, 'TV rating cached');
    }
  } catch (err) {
    logger.debug({ err: err && err.message, symbol }, 'Failed to cache TV rating (continuing)');
  }

  // Ensure returned object always has score_pct, aliases, and exchange
  const normalizedScore = typeof fresh.score === 'number' ? fresh.score : 0;
  const normalizedPct = typeof fresh.score_pct === 'number' ? fresh.score_pct : Math.round(normalizedScore * 100);

  return {
    source: fresh.source || 'unknown',
    score: normalizedScore,
    score_pct: normalizedPct,
    scorePct: normalizedPct,
    tvScore: normalizedPct,
    exchange: fresh.exchange || null,
    raw: fresh.raw || null
  };
}

/**
 * fallbackScore(macdPositiveFraction, volChangePct)
 * Calculates a score based on MACD and volume metrics when TV data unavailable
 * - 60% weight on MACD positive fraction
 * - 40% weight on volume change normalization
 */
function fallbackScore({ macdPositiveFraction = 0.5, volChangePct = 0.0 } = {}) {
  try {
    const volNorm = Math.max(0, Math.min(1, (volChangePct + 100) / 200));
    const score = Math.max(0, Math.min(1, 0.6 * macdPositiveFraction + 0.4 * volNorm));
    const pct = Math.round(score * 100);
    return { score: Math.round(score * 100) / 100, score_pct: pct, scorePct: pct, tvScore: pct };
  } catch (err) {
    logger.debug({ err }, 'fallbackScore calculation error');
    return { score: 0, score_pct: 0, scorePct: 0, tvScore: 0 };
  }
}

/**
 * clearOldTvRatingCache(olderThanHours)
 */
function clearOldTvRatingCache(olderThanHours = 24) {
  try {
    const db = dbModule.get();
    if (!db) return;

    const cutoffTime = Date.now() - (olderThanHours * 3600000);
    const stmt = db.prepare('DELETE FROM tv_ratings WHERE updated_at < ?');
    const result = stmt.run(cutoffTime);

    if (result.changes > 0) {
      logger.info({ deletedCount: result.changes, olderThanHours }, '🗑️ Cleared old TV ratings from cache');
    }
  } catch (err) {
    logger.debug({ err: err && err.message }, 'Failed to clear old TV ratings cache');
  }
}

/**
 * testTvApi(symbol)
 * - tries the configured exchange set and logs results (keeps behavior but improved parsing)
 */
async function testTvApi(symbol = 'BTCUSDT') {
  logger.info({ symbol }, '🧪 Testing TV API connectivity...');

  const results = [];
  const exchanges = Array.isArray(DEFAULT_EXCHANGES) && DEFAULT_EXCHANGES.length > 0 ? DEFAULT_EXCHANGES : ['BYBIT', 'BINANCE', 'KUCOIN', 'COINBASE'];

  for (const ex of exchanges) {
    try {
      const ticker = `${ex}:${symbol}`;
      const body = {
        symbols: { tickers: [ticker], query: { types: [] } },
        columns: ['Recommend.All|1']
      };

      const startTime = Date.now();
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), TV_REQUEST_TIMEOUT_MS);

      const res = await fetch(TV_SCANNER_URL, {
        method: 'POST',
        body: JSON.stringify(body),
        headers: {
          'Content-Type': 'application/json',
          'User-Agent': 'Mozilla/5.0 (compatible; bot)',
          'Origin': 'https://www.tradingview.com',
          'Referer': 'https://www.tradingview.com/'
        },
        signal: controller.signal
      });

      clearTimeout(timeoutId);
      const elapsed = Date.now() - startTime;

      if (res.ok) {
        const json = await res.json().catch(() => null);
        const row = json?.data?.[0];
        const d = row?.d ?? [];
        const recommend = parseRecommendFromResponse(json, 'Recommend.All');
        const rawScore = (typeof recommend === 'number') ? Math.max(0, Math.min(1, (recommend - 1) / 4)) : null;
        results.push({
          exchange: ex,
          ticker,
          status: '✅ OK',
          response_time_ms: elapsed,
          recommend,
          score: rawScore !== null ? Math.round(rawScore * 100) / 100 : 'N/A',
          rawRow: d,
          columns: json?.columns || []
        });
      } else {
        results.push({
          exchange: ex,
          ticker,
          status: `❌ HTTP ${res.status}`,
          response_time_ms: elapsed
        });
      }
    } catch (err) {
      results.push({
        exchange: ex,
        ticker: `${ex}:${symbol}`,
        status: `❌ ${err.name || 'Error'}`,
        error: err.message
      });
    }
  }

  logger.info({ results }, '📊 TV API test results');
  return results;
}

module.exports = {
  fetchTvRatingForSymbol,
  getOrFetchTvRatingCached,
  fallbackScore,
  clearOldTvRatingCache,
  testTvApi
};
