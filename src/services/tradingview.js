// tradingview.js
const fetch = require('node-fetch');
const logger = require('pino')();
const dbModule = require('../db');

const TV_SCANNER_URL = 'https://scanner.tradingview.com/crypto/scan';
const TV_SYMBOL_SEARCH_URL = 'https://symbol-search.tradingview.com/symbol_search/';

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
 * FIXED: Supports TradingView scanner Recommend.All value in range [-1, 1] (-1 = Strong Sell, 0 = Neutral, 1 = Strong Buy)
 * as well as legacy [1, 5] range, returning 0..1 float and percent 0..100.
 */
function normalizeRecommendToScore(recommend) {
  if (typeof recommend !== 'number' || !Number.isFinite(recommend)) {
    return { score: 0, scorePct: 0 };
  }

  let score = 0;
  if (recommend >= -1 && recommend <= 1) {
    score = (recommend + 1) / 2; // maps -1..1 to 0..1
  } else if (recommend >= 1 && recommend <= 5) {
    score = (recommend - 1) / 4; // maps 1..5 to 0..1
  } else {
    score = Math.max(0, Math.min(1, (recommend + 1) / 2));
  }

  const clampedScore = Math.max(0, Math.min(1, score));
  const pct = Math.round(clampedScore * 100);
  return { score: Math.round(clampedScore * 100) / 100, scorePct: pct };
}

function parseRecommendFromResponse(json, preferColumnName = 'Recommend.All') {
  if (!json || !Array.isArray(json.data) || json.data.length === 0) return null;

  const row = json.data[0];
  const d = Array.isArray(row.d) ? row.d : [];

  if (Array.isArray(json.columns) && json.columns.length > 0) {
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

  for (const v of d) {
    if (typeof v === 'number' && v >= -1 && v <= 1) return v;
    if (typeof v === 'string') {
      const n = Number(v);
      if (Number.isFinite(n) && n >= -1 && n <= 1) return n;
    }
    if (v && typeof v === 'object') {
      const n = typeof v.v !== 'undefined' ? Number(v.v) : NaN;
      if (Number.isFinite(n) && n >= -1 && n <= 1) return n;
    }
  }

  return null;
}

async function tryScannerForTicker(ticker, timeoutMs = TV_REQUEST_TIMEOUT_MS) {
  const body = {
    symbols: { tickers: [ticker], query: { types: [] } },
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

    const recommend = parseRecommendFromResponse(json, 'Recommend.All');
    if (recommend === null) {
      logger.debug({ ticker }, 'TV scanner: Recommend.All not found in response');
      return null;
    }

    const { score, scorePct } = normalizeRecommendToScore(recommend);

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

async function findCanonicalTvTicker(symbol, exchangeCandidates = DEFAULT_EXCHANGES, timeoutMs = 5000) {
  if (!TV_USE_SYMBOL_SEARCH) return null;

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

      const preferred = json.find((item) => {
        const sym = String(item?.symbol ?? '').toUpperCase();
        const short = String(item?.short_name ?? '').toUpperCase();
        return sym.endsWith(`:${String(symbol).toUpperCase()}`) || short === String(symbol).toUpperCase();
      });

      const pick = preferred || json[0];
      if (pick && pick.symbol) {
        const resolved = String(pick.symbol).toUpperCase();
        return resolved;
      }
    } catch (err) {
      logger.debug({ err, symbol, ex }, 'TV symbol_search attempt failed');
    }
  }
  return null;
}

async function fetchTvRatingForSymbol(symbol, maxRetries = 3) {
  if (!symbol) {
    return { source: 'error', score: 0, score_pct: 0, scorePct: 0, tvScore: 0 };
  }

  const exchangeCandidates = Array.isArray(DEFAULT_EXCHANGES) && DEFAULT_EXCHANGES.length > 0
    ? DEFAULT_EXCHANGES
    : ['BYBIT', 'BINANCE', 'KUCOIN'];

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    for (const ex of exchangeCandidates) {
      try {
        const ticker = `${ex}:${symbol}`;
        const res = await tryScannerForTicker(ticker, TV_REQUEST_TIMEOUT_MS);
        if (res && res.source === 'tradingview') {
          return res;
        }
      } catch (err) {}
    }

    if (TV_USE_SYMBOL_SEARCH) {
      try {
        const resolved = await findCanonicalTvTicker(symbol, exchangeCandidates);
        if (resolved) {
          const res = await tryScannerForTicker(resolved, TV_REQUEST_TIMEOUT_MS);
          if (res && res.source === 'tradingview') {
            return res;
          }
        }
      } catch (err) {}
    }

    if (attempt < maxRetries) {
      await sleep(1000 * attempt);
    }
  }

  return { source: 'fallback', score: 0, score_pct: 0, scorePct: 0, tvScore: 0, raw: null };
}

async function getOrFetchTvRatingCached(symbol) {
  if (!symbol) return { source: 'error', score: 0, score_pct: 0, scorePct: 0, tvScore: 0 };

  try {
    const db = dbModule.get();
    if (!db) throw new Error('DB not available');

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
    } catch (err) {}

    const cutoff = Date.now() - (Number(process.env.TV_CACHE_TTL_MS || TV_CACHE_TTL_MS));
    const cached = db.prepare(`
      SELECT score, source, exchange, updated_at
      FROM tv_ratings
      WHERE symbol = ? AND updated_at > ?
    `).get(symbol, cutoff);

    if (cached) {
      const rawScore = Number(cached.score) || 0;
      const scorePct = Math.round(rawScore * 100);

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
  } catch (err) {}

  const fresh = await fetchTvRatingForSymbol(symbol);

  try {
    const db = dbModule.get();
    if (db && fresh && fresh.source === 'tradingview') {
      db.prepare(`
        INSERT OR REPLACE INTO tv_ratings 
        (symbol, score, source, exchange, updated_at) 
        VALUES (?, ?, ?, ?, ?)
      `).run(symbol, fresh.score, fresh.source, fresh.exchange || null, Date.now());
    }
  } catch (err) {}

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

function fallbackScore({ macdPositiveFraction = 0.5, volChangePct = 0.0 } = {}) {
  try {
    const volNorm = Math.max(0, Math.min(1, (volChangePct + 100) / 200));
    const calculatedFraction = Math.max(0, Math.min(1, 0.6 * macdPositiveFraction + 0.4 * volNorm));
    const pct = Math.round(calculatedFraction * 100);
    return { 
      score: Math.round(calculatedFraction * 100) / 100, 
      score_pct: pct, 
      scorePct: pct, 
      tvScore: pct 
    };
  } catch (err) {
    return { score: 0, score_pct: 0, scorePct: 0, tvScore: 0 };
  }
}

function clearOldTvRatingCache(olderThanHours = 24) {
  try {
    const db = dbModule.get();
    if (!db) return;

    const cutoffTime = Date.now() - (olderThanHours * 3600000);
    db.prepare('DELETE FROM tv_ratings WHERE updated_at < ?').run(cutoffTime);
  } catch (err) {}
}

async function testTvApi(symbol = 'BTCUSDT') {
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
        const recommend = parseRecommendFromResponse(json, 'Recommend.All');
        const { score, scorePct } = normalizeRecommendToScore(recommend);
        results.push({
          exchange: ex,
          ticker,
          status: '✅ OK',
          response_time_ms: elapsed,
          recommend,
          score,
          scorePct
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

  return results;
}

module.exports = {
  fetchTvRatingForSymbol,
  getOrFetchTvRatingCached,
  fallbackScore,
  clearOldTvRatingCache,
  testTvApi
};
