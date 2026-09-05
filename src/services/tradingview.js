const fetch = require('node-fetch');
const logger = require('pino')();

const TV_SCANNER_URL = 'https://scanner.tradingview.com/crypto/scan';

/**
 * fetchTvRatingForSymbol(symbol)
 * Attempts to fetch TradingView Recommend.All score for symbol
 * Tries multiple exchange candidates (BYBIT, BINANCE, etc.)
 * Falls back to calculated score if TV API fails
 */
async function fetchTvRatingForSymbol(symbol) {
  if (!symbol) return { source: 'error', score: 0 };

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
        logger.debug({ exchange: ex, symbol, status: res.status }, 'TV API non-ok response');
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
        
        logger.info({ symbol, ticker, recommend, score }, 'TV rating fetched successfully');
        return { source: 'tradingview', score: Math.round(score * 100) / 100, raw: row.d };
      }
    } catch (err) {
      logger.debug({ err: err && err.message ? err.message : err, symbol, exchange: ex }, 'TV fetch attempt failed');
      continue;
    }
  }

  // Fallback: return zero score when all TV attempts fail
  logger.warn({ symbol, exchanges: exchangeCandidates }, 'TV rating unavailable for all exchanges, using fallback');
  return { source: 'fallback', score: 0, raw: null };
}

/**
 * fallbackScore(macdPositiveFraction, volChangePct)
 * Calculates a score based on MACD and volume metrics when TV data unavailable
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

module.exports = {
  fetchTvRatingForSymbol,
  fallbackScore
};
