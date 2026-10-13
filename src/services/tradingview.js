const fetch = require('node-fetch');
const logger = require('pino')();
const dbModule = require('../db');

const TV_SCANNER_URL = 'https://scanner.tradingview.com/crypto/scan';

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

async function fetchTvRatingForSymbol(symbol, maxRetries = 3) {
  if (!symbol) {
    logger.warn('fetchTvRatingForSymbol: symbol is empty');
    return { source: 'error', score: 0, score_pct: 0, scorePct: 0, tvScore: 0 };
  }

  const exchangeCandidates = (process.env.TRADINGVIEW_EXCHANGE_CANDIDATES || 'BYBIT,BINANCE,KUCOIN')
    .split(',')
    .map(s => s.trim())
    .filter(s => s.length > 0);

  logger.debug({ symbol, exchanges: exchangeCandidates }, 'fetchTvRatingForSymbol: starting fetch');

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    for (const ex of exchangeCandidates) {
      try {
        const ticker = `${ex}:${symbol}`;
        const body = {
          symbols: { tickers: [ticker], query: { types: [] } },
          columns: ['Recommend.All|1', 'Recommend.Other|1', 'RSI|14', 'momentum|14']
        };

        logger.debug({ attempt, exchange: ex, ticker }, 'TV fetch attempt');

        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 12000);

        const res = await fetch(TV_SCANNER_URL, {
          method: 'POST',
          body: JSON.stringify(body),
          headers: {
            'Content-Type': 'application/json',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36'
          },
          signal: controller.signal
        });

        clearTimeout(timeoutId);

        if (!res.ok) {
          logger.debug({ exchange: ex, symbol, status: res.status, attempt }, 'TV API non-ok response');
          continue;
        }

        const json = await res.json().catch(() => null);

        if (json && json.data && Array.isArray(json.data) && json.data.length > 0) {
          const row = json.data[0];
          const recommend = row.d && row.d[0];

          let score = 0;
          if (typeof recommend === 'number') {
            // TradingView Recommend.All: 1 (strong sell) ... 5 (strong buy)
            // map 5 -> 1.0, 1 -> 0.0, normalize to 0..1 where 1 is best (buy)
            score = Math.max(0, Math.min(1, (recommend - 1) / 4));
          }

          const scorePct = Math.round(score * 100);

          logger.info({ symbol, ticker, recommend, score, scorePct, attempt, exchange: ex }, '✅ TV rating fetched successfully');

          return {
            source: 'tradingview',
            score,                   // 0..1 float
            score_pct: scorePct,     // snake_case percent
            scorePct: scorePct,      // camelCase percent alias
            tvScore: scorePct,       // legacy alias used by some consumers
            raw: row.d,
            exchange: ex
          };
        } else {
          logger.debug({ symbol, exchange: ex, body: json }, 'TV API returned no data for ticker');
        }
      } catch (err) {
        const errorType = err.name === 'AbortError' ? 'timeout' : 'network';
        logger.debug({ errorType, err: err && err.message ? err.message : err, symbol, exchange: ex, attempt }, 'TV fetch attempt failed');
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

  logger.warn({ symbol, attempts: maxRetries, exchanges: exchangeCandidates }, '⚠️ TV rating unavailable after all retries, using fallback score of 0');
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
    } catch (err) {
      logger.debug({ err }, 'tv_ratings table creation/check failed (continuing without cache)');
    }

    // Check cache (within 2 hours = 7200000 ms)
    const cached = db.prepare(`
      SELECT score, source, exchange, updated_at
      FROM tv_ratings
      WHERE symbol = ? AND updated_at > ?
    `).get(symbol, Date.now() - 7200000);

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
    } else {
      logger.debug({ symbol }, 'TV rating cache miss or entry stale');
    }
  } catch (err) {
    logger.debug({ err: err && err.message, symbol }, 'Cache check failed (will fetch fresh)');
  }

  logger.debug({ symbol }, 'Fetching fresh TV rating from API');
  const fresh = await fetchTvRatingForSymbol(symbol);

  try {
    const db = dbModule.get();
    if (db) {
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

  const normalizedScore = typeof fresh.score === 'number' ? fresh.score : 0;
  const normalizedPct = typeof fresh.score_pct === 'number' ? fresh.score_pct : Math.round(normalizedScore * 100);

  logger.debug({ symbol, fresh, normalizedScore, normalizedPct }, 'TV rating final result');

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
    const score = Math.max(0, Math.min(1, 0.6 * macdPositiveFraction + 0.4 * volNorm));
    const pct = Math.round(score * 100);
    return { score: Math.round(score * 100) / 100, score_pct: pct, scorePct: pct, tvScore: pct };
  } catch (err) {
    logger.debug({ err }, 'fallbackScore calculation error');
    return { score: 0, score_pct: 0, scorePct: 0, tvScore: 0 };
  }
}

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

async function testTvApi(symbol = 'BTCUSDT') {
  logger.info({ symbol }, '🧪 Testing TV API connectivity...');

  const results = [];
  const exchanges = ['BYBIT', 'BINANCE', 'KUCOIN', 'COINBASE'];

  for (const ex of exchanges) {
    try {
      const ticker = `${ex}:${symbol}`;
      const body = {
        symbols: { tickers: [ticker], query: { types: [] } },
        columns: ['Recommend.All|1']
      };

      const startTime = Date.now();
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 12000);

      const res = await fetch(TV_SCANNER_URL, {
        method: 'POST',
        body: JSON.stringify(body),
        headers: { 'Content-Type': 'application/json' },
        signal: controller.signal
      });

      clearTimeout(timeoutId);
      const elapsed = Date.now() - startTime;

      if (res.ok) {
        const json = await res.json();
        const data = json?.data?.[0]?.d?.[0];
        const rawScore = (typeof data === 'number') ? Math.max(0, Math.min(1, (data - 1) / 4)) : null;
        results.push({
          exchange: ex,
          ticker,
          status: '✅ OK',
          response_time_ms: elapsed,
          score: rawScore !== null ? Math.round(rawScore * 100) / 100 : 'N/A',
          score_pct: rawScore !== null ? Math.round(rawScore * 100) : 'N/A'
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
