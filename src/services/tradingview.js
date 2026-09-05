const fetch = require('node-fetch');
const logger = require('pino')();
const dbModule = require('../db');

const TV_SCANNER_URL = 'https://scanner.tradingview.com/crypto/scan';

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

/**
 * fetchTvRatingForSymbol with retry logic (maxRetries = 3)
 * - Tries multiple exchanges (BYBIT, BINANCE, etc.)
 * - Retries with exponential backoff if all exchanges fail
 * - Falls back to zero score if all retries exhausted
 */
async function fetchTvRatingForSymbol(symbol, maxRetries = 3) {
  if (!symbol) {
    logger.warn('fetchTvRatingForSymbol: symbol is empty');
    return { source: 'error', score: 0 };
  }

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    const exchangeCandidates = (process.env.TRADINGVIEW_EXCHANGE_CANDIDATES || 'BYBIT,BINANCE').split(',').map(s => s.trim());
    
    for (const ex of exchangeCandidates) {
      try {
        const ticker = `${ex}:${symbol}`;
        const body = {
          symbols: { tickers: [ticker], query: { types: [] } },
          columns: ['Recommend.All|1', 'Recommend.Other|1', 'RSI|14', 'momentum|14']
        };
        
        const res = await fetch(TV_SCANNER_URL, {
          method: 'POST',
          body: JSON.stringify(body),
          headers: { 'Content-Type': 'application/json' },
          timeout: 8000
        });

        if (!res.ok) {
          logger.debug({ exchange: ex, symbol, status: res.status, attempt }, 'TV API non-ok response');
          continue;
        }

        const json = await res.json();
        
        if (json && json.data && Array.isArray(json.data) && json.data.length > 0) {
          const row = json.data[0];
          const recommend = row.d && row.d[0];
          
          let score = 0;
          if (typeof recommend === 'number') {
            // TradingView Recommend.All: 1 (strong sell) ... 5 (strong buy)
            // map 5 -> 1.0, 1 -> 0.0, normalize to 0..1 where 1 is best (buy)
            score = Math.max(0, Math.min(1, (recommend - 1) / 4));
          }
          
          logger.info({ symbol, ticker, recommend, score, attempt }, 'TV rating fetched successfully');
          return { source: 'tradingview', score: Math.round(score * 100) / 100, raw: row.d };
        }
      } catch (err) {
        logger.debug({ err: err && err.message ? err.message : err, symbol, exchange: ex, attempt }, 'TV fetch attempt failed');
        continue;
      }
    }

    // Retry with exponential backoff
    if (attempt < maxRetries) {
      const backoffMs = 1000 * attempt;
      logger.debug({ symbol, attempt, nextRetry: backoffMs, maxRetries }, 'TV fetch retry scheduled');
      await sleep(backoffMs);
    }
  }

  logger.warn({ symbol, attempts: maxRetries, exchanges: (process.env.TRADINGVIEW_EXCHANGE_CANDIDATES || 'BYBIT,BINANCE') }, 'TV rating unavailable after all retries, using fallback score of 0');
  return { source: 'fallback', score: 0, raw: null };
}

/**
 * getOrFetchTvRatingCached(symbol)
 * - Checks DB cache (valid for 1 hour)
 * - If fresh cache exists, returns it immediately
 * - Otherwise fetches fresh from TV API and caches result
 * - Graceful fallback if DB unavailable
 */
async function getOrFetchTvRatingCached(symbol) {
  if (!symbol) return { source: 'error', score: 0 };

  try {
    const db = dbModule.get();
    if (!db) throw new Error('DB not available');

    // Check if tv_ratings table exists, create if not
    try {
      db.prepare(`
        CREATE TABLE IF NOT EXISTS tv_ratings (
          symbol TEXT PRIMARY KEY,
          score REAL DEFAULT 0,
          source TEXT,
          updated_at INTEGER DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_tv_ratings_updated_at ON tv_ratings(updated_at);
      `).run();
    } catch (err) {
      logger.debug({ err }, 'tv_ratings table creation/check failed (continuing without cache)');
    }

    // Check cache (within 1 hour = 3600000 ms)
    const cached = db.prepare(`
      SELECT score, source, updated_at 
      FROM tv_ratings 
      WHERE symbol = ? AND updated_at > ?
    `).get(symbol, Date.now() - 3600000);
    
    if (cached) {
      logger.debug({ symbol, source: cached.source, age: Date.now() - cached.updated_at }, 'TV rating retrieved from cache');
      return { source: 'cache', score: cached.score, cached: true };
    }
  } catch (err) {
    logger.debug({ err: err && err.message, symbol }, 'Cache check failed (will fetch fresh)');
  }

  // Fetch fresh from TV API
  const fresh = await fetchTvRatingForSymbol(symbol);

  // Try to cache result
  try {
    const db = dbModule.get();
    if (db) {
      db.prepare(`
        INSERT OR REPLACE INTO tv_ratings 
        (symbol, score, source, updated_at) 
        VALUES (?, ?, ?, ?)
      `).run(symbol, fresh.score, fresh.source, Date.now());
      logger.debug({ symbol, score: fresh.score, source: fresh.source }, 'TV rating cached');
    }
  } catch (err) {
    logger.debug({ err: err && err.message, symbol }, 'Failed to cache TV rating (continuing)');
  }

  return fresh;
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
    return Math.round(score * 100) / 100;
  } catch (err) {
    logger.debug({ err }, 'fallbackScore calculation error');
    return 0;
  }
}

/**
 * clearOldTvRatingCache(olderThanHours)
 * - Cleans up TV ratings cache older than specified hours
 * - Useful to run periodically to keep DB size reasonable
 */
function clearOldTvRatingCache(olderThanHours = 24) {
  try {
    const db = dbModule.get();
    if (!db) return;

    const cutoffTime = Date.now() - (olderThanHours * 3600000);
    const stmt = db.prepare('DELETE FROM tv_ratings WHERE updated_at < ?');
    const result = stmt.run(cutoffTime);
    
    if (result.changes > 0) {
      logger.info({ deletedCount: result.changes, olderThanHours }, 'Cleared old TV ratings from cache');
    }
  } catch (err) {
    logger.debug({ err: err && err.message }, 'Failed to clear old TV ratings cache');
  }
}

module.exports = {
  fetchTvRatingForSymbol,
  getOrFetchTvRatingCached,
  fallbackScore,
  clearOldTvRatingCache
};
