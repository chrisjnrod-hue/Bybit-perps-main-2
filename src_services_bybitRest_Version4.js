const fetch = require('node-fetch');
const crypto = require('crypto');
const config = require('../config');
const logger = require('pino')();

const requestPublic = async (path, params = {}) => {
  const url = new URL(`${config.BYBIT_REST_BASE}${path}`);
  Object.entries(params).forEach(([k, v]) => url.searchParams.append(k, v));
  const res = await fetch(url.toString(), { method: 'GET' });
  let json;
  try { json = await res.json(); } catch (e) { json = null; }
  if (!res.ok) throw new Error(`HTTP ${res.status} ${res.statusText} ${JSON.stringify(json)}`);
  return json;
};

const nowTs = () => Date.now().toString();

function signV5(method, pathWithQuery, body, timestamp) {
  const payload = timestamp + method.toUpperCase() + pathWithQuery + (body ? JSON.stringify(body) : '');
  return crypto.createHmac('sha256', config.BYBIT_API_SECRET || '').update(payload).digest('hex');
}

async function requestV5Private(method, path, params = {}, body = null) {
  if (!config.BYBIT_API_KEY || !config.BYBIT_API_SECRET) throw new Error('Bybit API key/secret not set');
  const url = new URL(`${config.BYBIT_REST_BASE}${path}`);
  if (method.toUpperCase() === 'GET' && params && Object.keys(params).length) {
    Object.entries(params).forEach(([k, v]) => url.searchParams.append(k, v));
  }
  const pathWithQuery = url.pathname + (url.search || '');
  const timestamp = nowTs();
  const signature = signV5(method, pathWithQuery, body, timestamp);
  const headers = {
    'Content-Type': 'application/json',
    'x-bapi-key': config.BYBIT_API_KEY,
    'x-bapi-signature': signature,
    'x-bapi-timestamp': timestamp,
    'x-bapi-recv-window': '5000'
  };
  const opts = { method, headers };
  if (body && (method.toUpperCase() !== 'GET')) opts.body = JSON.stringify(body);
  const res = await fetch(url.toString(), opts);
  let json;
  try { json = await res.json(); } catch (e) { json = { error: 'invalid-json' }; }
  if (!res.ok || (json.retCode && json.retCode !== 0)) {
    logger.warn({ status: res.status, body: json }, 'Bybit v5 private request non-OK');
  }
  return json;
}

module.exports = {
  async fetchAllSymbols() {
    try {
      const r = await requestPublic('/v5/market/instruments-info', { category: 'linear' });
      if (r.result?.list) return r.result.list.map(s => ({
        symbol: s.symbol,
        base: s.baseCoin || '',
        quote: s.quoteCoin || 'USDT',
        raw: s
      }));
    } catch (err) {
      logger.warn({ err }, 'v5 instruments-info failed');
    }
    try {
      const data = await requestPublic('/v2/public/symbols');
      if (data && data.result) return data.result.map(s => ({
        symbol: s.name || s.symbol,
        base: s.base_currency || ''
      }));
    } catch (err) {
      logger.error({ err }, 'symbol fetch failed');
    }
    return [];
  },

  async fetchKlines(symbol, interval, limit = 200) {
    try {
      const resp = await requestPublic('/v2/public/kline', { symbol, interval, limit: String(limit) });
      if (!resp.result) return [];
      return resp.result.map(r => ({
        open_time: r.start_at || r[0],
        open: Number(r.open || r[1]),
        high: Number(r.high || r[2]),
        low: Number(r.low || r[3]),
        close: Number(r.close || r[4]),
        volume: Number(r.volume || r[5])
      }));
    } catch (err) {
      logger.error({ err, symbol, interval }, 'fetchKlines failed');
      return [];
    }
  },

  async fetchTicker24h(symbol) {
    try {
      const r = await requestPublic('/v2/public/tickers', { symbol });
      if (r && r.result && r.result.length) return r.result[0];
    } catch (err) {
      logger.warn({ err }, 'fetchTicker24h failed');
    }
    return null;
  },

  async getWalletBalance(coin = 'USDT') {
    const json = await requestV5Private('GET', '/v5/account/wallet-balance', { coin });
    if (json && json.result && json.result.list) {
      return json.result.list.find(r => r.coin === coin) || json.result.list[0];
    }
    return null;
  },

  async placeMarketOrderV5({ category = 'linear', symbol, side = 'Buy', qty, reduceOnly = false, tp = null, sl = null }) {
    const body = {
      category,
      symbol,
      side,
      orderType: 'Market',
      qty: String(qty),
      reduceOnly: reduceOnly
    };
    if (tp) body.takeProfit = String(tp);
    if (sl) body.stopLoss = String(sl);
    const json = await requestV5Private('POST', '/v5/order/create', {}, body);
    return json;
  },

  async setPositionTradingStop({ symbol, takeProfit = null, stopLoss = null }) {
    const body = { symbol };
    if (takeProfit !== null) body.takeProfit = String(takeProfit);
    if (stopLoss !== null) body.stopLoss = String(stopLoss);
    const json = await requestV5Private('POST', '/v5/position/trading-stop', {}, body);
    return json;
  }
};