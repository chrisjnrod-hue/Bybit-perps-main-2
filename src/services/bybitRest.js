/**
 * src/services/bybitRest.js
 *
 * Bybit REST helper with:
 * - probeHosts (resilient)
 * - fetchKlines / fetchAllSymbols with robust shape normalization
 * - fallback to CoinGecko markets when Bybit returns no symbols
 * - v5-signed helpers (wallet, orders, trading-stop, open orders/positions)
 */

const fetch = require('node-fetch');
const crypto = require('crypto');
const { URL } = require('url');
const logger = require('pino')();
const config = require('../config');

const HOST_CANDIDATES = [
  'https://api.bybit.com',
  'https://bybit.com',
  'https://bybits.com',
  'https://bybit.nl',
  'https://bybit.co.uk',
  'https://bybit.eu'
];

let chosenBase = null;

function getConfiguredBase() {
  const envBase = process.env.BYBIT_REST_BASE || (config && config.BYBIT_REST_BASE);
  if (!envBase) return null;
  return String(envBase).replace(/\/$/, '');
}

async function fetchWithTimeout(url, opts = {}, timeoutMs = 7000) {
  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const merged = { ...opts, signal: controller.signal };
    const res = await fetch(url, merged);
    clearTimeout(id);
    return res;
  } catch (err) {
    clearTimeout(id);
    throw err;
  }
}

function signV5Request(method, requestPath, body = '', apiSecret) {
  const timestamp = Date.now().toString();
  const payload = timestamp + method.toUpperCase() + requestPath + (body ? body : '');
  const signature = crypto.createHmac('sha256', apiSecret).update(payload).digest('hex');
  return { signature, timestamp };
}

function getBase() {
  const configured = getConfiguredBase();
  if (chosenBase) return chosenBase;
  if (configured) return configured;
  return HOST_CANDIDATES[0];
}

async function probeHosts(timeoutMs = 5000) {
  const configured = getConfiguredBase();
  const list = configured ? [configured, ...HOST_CANDIDATES.filter(h => h !== configured)] : HOST_CANDIDATES;

  logger.info({ candidates: list }, 'bybitRest: starting host probe');

  for (const host of list) {
    const testUrl = `${host.replace(/\/$/, '')}/v5/market/kline?category=linear&symbol=BTCUSDT&interval=60&limit=1`;
    try {
      const res = await fetchWithTimeout(testUrl, { method: 'GET' }, timeoutMs);
      logger.info({ host, status: res.status }, 'probeHosts: host responded');

      let json = null;
      try {
        json = await res.json();
      } catch (parseErr) {
        logger.debug({ host, err: parseErr && parseErr.message ? parseErr.message : String(parseErr) }, 'probeHosts: json parse failed for host');
      }

      if (res.ok) {
        if (json) {
          chosenBase = host.replace(/\/$/, '');
          logger.info({ chosenBase }, 'probeHosts: selected host (json ok)');
          return chosenBase;
        }
        chosenBase = host.replace(/\/$/, '');
        logger.info({ chosenBase, note: 'accepted despite json parse failure (HTTP OK)' }, 'probeHosts: selected host');
        return chosenBase;
      } else {
        logger.debug({ host, status: res.status }, 'probeHosts: non-ok response, trying next');
        continue;
      }
    } catch (err) {
      logger.debug({ host, err: err && err.message ? err.message : String(err) }, 'probeHosts: request failed, trying next');
      continue;
    }
  }

  const configuredBase = getConfiguredBase();
  if (configuredBase) {
    logger.warn({ configuredBase }, 'probeHosts: no host probe succeeded — falling back to configured BYBIT_REST_BASE');
    chosenBase = configuredBase;
    return chosenBase;
  }

  logger.warn('probeHosts: no hosts responded successfully and no configured BYBIT_REST_BASE');
  return null;
}

async function fetchKlines(symbol, interval, limit = 200) {
  const configured = getConfiguredBase();
  const hostList = configured ? [configured, ...HOST_CANDIDATES.filter(h => h !== configured)] : HOST_CANDIDATES;
  if (chosenBase && !hostList.includes(chosenBase)) hostList.unshift(chosenBase);

  const candidates = [
    { path: '/v5/market/kline', params: { category: 'linear', symbol, interval, limit: String(limit) } },
    { path: '/v2/public/kline', params: { symbol, interval, limit: String(limit) } },
    { path: '/v2/public/kline/list', params: { symbol, interval, limit: String(limit) } }
  ];

  for (const host of hostList) {
    for (const c of candidates) {
      try {
        const url = new URL(`${host.replace(/\/$/, '')}${c.path}`);
        Object.entries(c.params || {}).forEach(([k, v]) => {
          if (v !== undefined && v !== null) url.searchParams.append(k, String(v));
        });

        const res = await fetchWithTimeout(url.toString(), { method: 'GET' }, 6000);
        let json = null;
        try { json = await res.json(); } catch (e) { json = null; }

        if (!res.ok) {
          logger.debug({ host, path: c.path, status: res.status, body: json }, 'fetchKlines: HTTP error (non-OK)');
          continue;
        }

        if (json && json.result && Array.isArray(json.result.list)) {
          const list = json.result.list;
          return list.map(r => ({
            open_time: r.start || r.t || r.open_time || r[0],
            open: Number(r.open || r.o || r[1] || 0),
            high: Number(r.high || r.h || r[2] || 0),
            low: Number(r.low || r.l || r[3] || 0),
            close: Number(r.close || r.c || r[4] || 0),
            volume: Number(r.volume || r.v || r[5] || 0)
          }));
        }

        if (json && json.result && Array.isArray(json.result)) {
          const arr = json.result;
          if (arr.length && Array.isArray(arr[0])) {
            return arr.map(r => ({
              open_time: r[0],
              open: Number(r[1]),
              high: Number(r[2]),
              low: Number(r[3]),
              close: Number(r[4]),
              volume: Number(r[5])
            }));
          } else if (arr.length && typeof arr[0] === 'object') {
            return arr.map(r => ({
              open_time: r.start || r.start_at || r.t || r.open_time || r[0],
              open: Number(r.open || r.o || r[1] || 0),
              high: Number(r.high || r.h || r[2] || 0),
              low: Number(r.low || r.l || r[3] || 0),
              close: Number(r.close || r.c || r[4] || 0),
              volume: Number(r.volume || r.v || r[5] || 0)
            }));
          }
        }

        if (Array.isArray(json)) {
          if (json.length && Array.isArray(json[0])) {
            return json.map(r => ({
              open_time: r[0],
              open: Number(r[1]),
              high: Number(r[2]),
              low: Number(r[3]),
              close: Number(r[4]),
              volume: Number(r[5])
            }));
          } else if (json.length && typeof json[0] === 'object') {
            return json.map(r => ({
              open_time: r.start || r.start_at || r.t || r.open_time || r[0],
              open: Number(r.open || r.o || r[1] || 0),
              high: Number(r.high || r.h || r[2] || 0),
              low: Number(r.low || r.l || r[3] || 0),
              close: Number(r.close || r.c || r[4] || 0),
              volume: Number(r.volume || r.v || r[5] || 0)
            }));
          }
        }

        logger.debug({ host, path: c.path, body: json }, 'fetchKlines: unexpected shape, trying next candidate');
      } catch (err) {
        logger.debug({ host, path: c.path, err: err && err.message ? err.message : String(err) }, 'fetchKlines: candidate threw, trying next');
      }
    }
  }

  logger.error({ symbol, interval }, 'fetchKlines: all candidates/hosts failed');
  return [];
}

/** Helper: fetch symbols from CoinGecko markets as a fallback */
async function fetchSymbolsFromCoinGecko(perPage = 250) {
  try {
    const qUrl = `https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=${perPage}&page=1&sparkline=false`;
    const res = await fetchWithTimeout(qUrl, { method: 'GET' }, 7000);
    if (!res.ok) {
      logger.warn({ status: res.status }, 'CoinGecko fallback: non-ok response');
      return [];
    }
    const arr = await res.json().catch(() => null);
    if (!Array.isArray(arr)) return [];
    const mapped = [];
    const seen = new Set();
    for (const it of arr) {
      if (!it || !it.symbol) continue;
      const base = String(it.symbol).toUpperCase();
      const candidate = `${base}USDT`;
      if (!seen.has(candidate)) {
        seen.add(candidate);
        mapped.push({ symbol: candidate, base, quote: 'USDT', status: 'unknown' });
      }
    }
    logger.info({ count: mapped.length }, 'fetchSymbolsFromCoinGecko: fallback symbols prepared');
    return mapped;
  } catch (err) {
    logger.debug({ err }, 'fetchSymbolsFromCoinGecko: failed');
    return [];
  }
}

async function fetchAllSymbols() {
  logger.info('bybitRest.fetchAllSymbols: start');
  const configured = getConfiguredBase();
  const hostList = configured ? [configured, ...HOST_CANDIDATES.filter(h => h !== configured)] : HOST_CANDIDATES;
  if (chosenBase && !hostList.includes(chosenBase)) hostList.unshift(chosenBase);

  const candidates = [
    { path: '/v5/market/instruments', params: {} },
    { path: '/v5/market/instruments-info', params: {} },
    { path: '/v2/public/symbols', params: {} },
    { path: '/v2/public/tickers', params: {} }
  ];

  let lastOkResponseText = null;
  let lastOkHostPath = null;

  for (const host of hostList) {
    for (const c of candidates) {
      try {
        const url = new URL(`${host.replace(/\/$/, '')}${c.path}`);
        Object.entries(c.params || {}).forEach(([k, v]) => {
          if (v !== undefined && v !== null) url.searchParams.append(k, String(v));
        });

        const res = await fetchWithTimeout(url.toString(), { method: 'GET' }, 6000);
        // try to parse JSON but keep raw fallback for diagnostics
        let json = null;
        let bodyText = null;
        try {
          const text = await res.text();
          bodyText = text;
          try { json = JSON.parse(text); } catch (e) { json = null; }
        } catch (e) {
          bodyText = null;
        }

        if (!res.ok) {
          logger.debug({ host, path: c.path, status: res.status, bodyText: bodyText ? bodyText.slice(0, 400) : null }, 'fetchAllSymbols: HTTP error');
          continue;
        }

        lastOkHostPath = { host, path: c.path };
        lastOkResponseText = bodyText;

        let symbols = null;
        if (json && json.result && Array.isArray(json.result.list)) {
          symbols = json.result.list.map(it => ({
            symbol: it.symbol || it.name || null,
            base: it.baseCoin || it.base || null,
            quote: it.quoteCoin || it.quote || null,
            status: it.status || it.state || null
          })).filter(Boolean);
        } else if (json && json.result && Array.isArray(json.result)) {
          symbols = json.result.map(it => ({
            symbol: it.symbol || it.name || null,
            base: it.baseCoin || it.base || null,
            quote: it.quoteCoin || it.quote || null,
            status: it.status || it.state || null
          })).filter(Boolean);
        } else if (Array.isArray(json)) {
          symbols = json.map(it => ({
            symbol: it.symbol || it.name || null,
            base: it.base || it.baseCoin || null,
            quote: it.quote || it.quoteCoin || null,
            status: it.status || it.state || null
          })).filter(Boolean);
        } else {
          // If json is null but HTTP OK, we couldn't parse — continue to next candidate but keep lastOkResponseText for diagnostics
          logger.debug({ host, path: c.path, snippet: lastOkResponseText ? lastOkResponseText.slice(0, 400) : null }, 'fetchAllSymbols: parsed json null despite HTTP OK');
        }

        if (symbols && symbols.length) {
          logger.info({ host, path: c.path, count: symbols.length }, 'fetchAllSymbols: fetched symbols from host');
          return symbols;
        }

        logger.debug({ host, path: c.path, bodySnippet: lastOkResponseText ? lastOkResponseText.slice(0, 400) : null }, 'fetchAllSymbols: unexpected/empty shape; trying next');
      } catch (err) {
        logger.debug({ host, path: c.path, err: err && err.message ? err.message : String(err) }, 'fetchAllSymbols: candidate threw, trying next');
      }
    }
  }

  // If we reach here, no hosts returned usable symbols. Log diagnostic details.
  if (lastOkHostPath) {
    logger.warn({ lastOkHostPath, snippet: lastOkResponseText ? lastOkResponseText.slice(0, 800) : null }, 'fetchAllSymbols: all hosts/candidates failed to return symbols (last OK response snippet included)');
  } else {
    logger.warn('fetchAllSymbols: no host returned HTTP OK during attempts');
  }

  // Fallback: try CoinGecko to create a best-effort symbol list (BASEUSDT)
  try {
    const cgSymbols = await fetchSymbolsFromCoinGecko(250);
    if (cgSymbols && cgSymbols.length) {
      logger.info({ count: cgSymbols.length }, 'fetchAllSymbols: fallback to CoinGecko succeeded');
      return cgSymbols;
    }
    logger.warn('fetchAllSymbols: CoinGecko fallback returned no symbols');
  } catch (e) {
    logger.debug({ e }, 'fetchAllSymbols: CoinGecko fallback threw');
  }

  return [];
}

async function fetchTicker24h(symbol) {
  const configured = getConfiguredBase();
  const hostList = configured ? [configured, ...HOST_CANDIDATES.filter(h => h !== configured)] : HOST_CANDIDATES;
  if (chosenBase && !hostList.includes(chosenBase)) hostList.unshift(chosenBase);

  const candidates = [
    { path: '/v5/market/tickers', params: { symbol } },
    { path: '/v2/public/tickers', params: { symbol } }
  ];

  for (const host of hostList) {
    for (const c of candidates) {
      try {
        const url = new URL(`${host.replace(/\/$/, '')}${c.path}`);
        Object.entries(c.params || {}).forEach(([k, v]) => {
          if (v !== undefined && v !== null) url.searchParams.append(k, String(v));
        });
        const res = await fetchWithTimeout(url.toString(), { method: 'GET' }, 6000);
        const json = await res.json().catch(() => null);
        if (!res.ok) {
          logger.debug({ host, path: c.path, status: res.status, body: json }, 'fetchTicker24h: HTTP error');
          continue;
        }

        if (json && json.result && Array.isArray(json.result.list) && json.result.list.length) {
          const t = json.result.list[0];
          return {
            last_price: t.lastPrice || t.last || t.close || t.price || null,
            last: t.lastPrice || t.last || t.close || t.price || null,
            close: t.close || t.last || t.price || null,
            volume: t.turnover24h || t.volume || t.volume_24h || null,
            volume_24h: t.turnover24h || t.volume || null
          };
        }

        if (json && (Array.isArray(json) || (json.result && Array.isArray(json.result)))) {
          const arr = Array.isArray(json) ? json : json.result;
          if (arr.length && arr[0]) {
            const t = arr[0];
            return {
              last_price: t.last_price || t.last || t.close || t.price || null,
              last: t.last_price || t.last || t.close || t.price || null,
              close: t.close || t.last || t.price || null,
              volume: t.volume || t.volume_24h || null,
              volume_24h: t.volume || null
            };
          }
        }

        logger.debug({ host, path: c.path, body: json }, 'fetchTicker24h: unexpected shape, trying next');
      } catch (err) {
        logger.debug({ host, path: c.path, err: err && err.message ? err.message : String(err) }, 'fetchTicker24h: candidate threw');
      }
    }
  }

  return null;
}

async function getWalletBalance(coin = 'USDT') {
  const apiKey = process.env.BYBIT_API_KEY;
  const apiSecret = process.env.BYBIT_API_SECRET;
  if (!apiKey || !apiSecret) {
    throw new Error('getWalletBalance requires BYBIT_API_KEY and BYBIT_API_SECRET env vars');
  }

  const requestPath = `/v5/account/wallet-balance?coin=${encodeURIComponent(coin)}`;
  const base = getBase();
  const { signature, timestamp } = signV5Request('GET', requestPath, '', apiSecret);
  const url = `${base}${requestPath}`;
  const headers = {
    'Content-Type': 'application/json',
    'X-BAPI-API-KEY': apiKey,
    'X-BAPI-TIMESTAMP': timestamp,
    'X-BAPI-SIGN': signature,
    'X-BAPI-RECV-WINDOW': '5000'
  };

  const res = await fetchWithTimeout(url, { method: 'GET', headers }, 8000);
  const json = await res.json().catch(() => null);
  if (!res.ok) {
    logger.warn({ url, status: res.status, body: json }, 'getWalletBalance HTTP error');
    throw new Error(`getWalletBalance HTTP ${res.status}`);
  }

  if (json && json.result) {
    if (Array.isArray(json.result.list) && json.result.list.length) return json.result.list[0];
    if (json.result[coin]) return json.result[coin];
    return json.result;
  }
  return json;
}

async function placeMarketOrderV5({ category = 'linear', symbol, side = 'Buy', qty, reduceOnly = false, tp = null, sl = null } = {}) {
  const apiKey = process.env.BYBIT_API_KEY;
  const apiSecret = process.env.BYBIT_API_SECRET;
  if (!apiKey || !apiSecret) {
    throw new Error('placeMarketOrderV5 requires BYBIT_API_KEY and BYBIT_API_SECRET env vars');
  }

  const bodyObj = {
    category,
    symbol,
    side,
    orderType: 'Market',
    qty: String(qty),
    reduceOnly: Boolean(reduceOnly)
  };
  if (tp) bodyObj.takeProfit = String(tp);
  if (sl) bodyObj.stopLoss = String(sl);

  const bodyStr = JSON.stringify(bodyObj);
  const requestPath = `/v5/order/create`;
  const base = getBase();
  const { signature, timestamp } = signV5Request('POST', requestPath, bodyStr, apiSecret);
  const url = `${base}${requestPath}`;
  const headers = {
    'Content-Type': 'application/json',
    'X-BAPI-API-KEY': apiKey,
    'X-BAPI-TIMESTAMP': timestamp,
    'X-BAPI-SIGN': signature,
    'X-BAPI-RECV-WINDOW': '5000'
  };

  const res = await fetchWithTimeout(url, { method: 'POST', body: bodyStr, headers }, 8000);
  const json = await res.json().catch(() => null);
  if (!res.ok) {
    logger.warn({ url, status: res.status, body: json }, 'placeMarketOrderV5 HTTP error');
    throw new Error(`placeMarketOrderV5 HTTP ${res.status}`);
  }
  return json;
}

async function setPositionTradingStop({ category = 'linear', symbol, stopLoss }) {
  const apiKey = process.env.BYBIT_API_KEY;
  const apiSecret = process.env.BYBIT_API_SECRET;
  if (!apiKey || !apiSecret) {
    throw new Error('setPositionTradingStop requires BYBIT_API_KEY and BYBIT_API_SECRET env vars');
  }

  const bodyObj = { category, symbol };
  if (typeof stopLoss !== 'undefined' && stopLoss !== null) bodyObj.stopLoss = String(stopLoss);

  const bodyStr = JSON.stringify(bodyObj);
  const requestPath = `/v5/position/trading-stop`;
  const base = getBase();
  const { signature, timestamp } = signV5Request('POST', requestPath, bodyStr, apiSecret);
  const url = `${base}${requestPath}`;
  const headers = {
    'Content-Type': 'application/json',
    'X-BAPI-API-KEY': apiKey,
    'X-BAPI-TIMESTAMP': timestamp,
    'X-BAPI-SIGN': signature,
    'X-BAPI-RECV-WINDOW': '5000'
  };

  const res = await fetchWithTimeout(url, { method: 'POST', body: bodyStr, headers }, 8000);
  const json = await res.json().catch(() => null);
  if (!res.ok) {
    logger.warn({ url, status: res.status, body: json }, 'setPositionTradingStop HTTP error');
    throw new Error(`setPositionTradingStop HTTP ${res.status}`);
  }
  return json;
}

async function fetchOpenOrders({ category = 'linear', symbol = null } = {}) {
  const apiKey = process.env.BYBIT_API_KEY;
  const apiSecret = process.env.BYBIT_API_SECRET;
  if (!apiKey || !apiSecret) throw new Error('fetchOpenOrders requires BYBIT_API_KEY and BYBIT_API_SECRET env vars');

  const params = new URLSearchParams();
  if (category) params.append('category', category);
  if (symbol) params.append('symbol', symbol);
  const requestPath = `/v5/order/realtime${params.toString() ? `?${params.toString()}` : ''}`;

  const base = getBase();
  const { signature, timestamp } = signV5Request('GET', requestPath, '', apiSecret);

  const url = `${base}${requestPath}`;
  const headers = {
    'Content-Type': 'application/json',
    'X-BAPI-API-KEY': apiKey,
    'X-BAPI-TIMESTAMP': timestamp,
    'X-BAPI-SIGN': signature,
    'X-BAPI-RECV-WINDOW': '5000'
  };

  const res = await fetchWithTimeout(url, { method: 'GET', headers }, 8000);
  const json = await res.json().catch(() => null);
  if (!res.ok) {
    logger.warn({ url, status: res.status, body: json }, 'fetchOpenOrders HTTP error');
    throw new Error(`fetchOpenOrders HTTP ${res.status}`);
  }
  return json;
}

async function fetchOpenPositions({ category = 'linear', symbol = null } = {}) {
  const apiKey = process.env.BYBIT_API_KEY;
  const apiSecret = process.env.BYBIT_API_SECRET;
  if (!apiKey || !apiSecret) throw new Error('fetchOpenPositions requires BYBIT_API_KEY and BYBIT_API_SECRET env vars');

  const params = new URLSearchParams();
  if (category) params.append('category', category);
  if (symbol) params.append('symbol', symbol);
  const requestPath = `/v5/position/list${params.toString() ? `?${params.toString()}` : ''}`;

  const base = getBase();
  const { signature, timestamp } = signV5Request('GET', requestPath, '', apiSecret);

  const url = `${base}${requestPath}`;
  const headers = {
    'Content-Type': 'application/json',
    'X-BAPI-API-KEY': apiKey,
    'X-BAPI-TIMESTAMP': timestamp,
    'X-BAPI-SIGN': signature,
    'X-BAPI-RECV-WINDOW': '5000'
  };

  const res = await fetchWithTimeout(url, { method: 'GET', headers }, 8000);
  const json = await res.json().catch(() => null);
  if (!res.ok) {
    logger.warn({ url, status: res.status, body: json }, 'fetchOpenPositions HTTP error');
    throw new Error(`fetchOpenPositions HTTP ${res.status}`);
  }
  return json;
}

module.exports = {
  probeHosts,
  getBase,
  fetchKlines,
  fetchAllSymbols,
  fetchTicker24h,
  getWalletBalance,
  placeMarketOrderV5,
  setPositionTradingStop,
  fetchOpenOrders,
  fetchOpenPositions
};
