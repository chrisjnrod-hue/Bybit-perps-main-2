const fetch = require('node-fetch');
const logger = require('pino')();

const TV_SCANNER_URL = 'https://scanner.tradingview.com/crypto/scan';

async function fetchTvRatingForSymbol(symbol) {
  const exchangeCandidates = (process.env.TRADINGVIEW_EXCHANGE_CANDIDATES || 'BYBIT,BINANCE').split(',').map(s => s.trim());
  for (const ex of exchangeCandidates) {
    try {
      const ticker = `${ex}:${symbol}`;
      const body = {
        symbols: { tickers: [ticker], query: { types: [] } },
        columns: ['Recommend.All|1','Recommend.Other|1','RSI|14','momentum|14']
      };
      const res = await fetch(TV_SCANNER_URL, { method: 'POST', body: JSON.stringify(body), headers: { 'Content-Type': 'application/json' } });
      if (!res.ok) continue;
      const json = await res.json();
      if (json && json.data && json.data.length) {
        const row = json.data[0];
        const recommend = row.d[0];
        let score = 0;
        if (typeof recommend === 'number') score = Math.max(0, Math.min(1, (5 - recommend) / 4));
        logger.info({ symbol, ticker, score }, 'TV rating fetched');
        return { source: 'tradingview', score, raw: row.d };
      }
    } catch (err) {
      logger.debug({ err, symbol, ex }, 'TV fetch attempt failed');
      continue;
    }
  }
  return { source: 'fallback', score: 0, raw: null };
}

function fallbackScore({ macdPositiveFraction = 0.5, volChangePct = 0.0 }) {
  const volNorm = Math.max(0, Math.min(1, (volChangePct + 100) / 200));
  return Math.max(0, Math.min(1, 0.6 * macdPositiveFraction + 0.4 * volNorm));
}

module.exports = {
  fetchTvRatingForSymbol,
  fallbackScore
};