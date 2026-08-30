/**
 * src/services/bybitRest.js
 *
 * Complete Bybit REST helper:
 *  - probeHosts with resilient acceptance (accept HTTP OK even if JSON parse fails)
 *  - fetchKlines / fetchAllSymbols with robust shape normalization
 *  - fetchTicker24h helper
 *  - v5-signed helpers for wallet, order create, trading-stop, open orders/positions
 *
 * Set LOG_LEVEL=debug for more verbose logs.
 */

const fetch = require('node-fetch');
const crypto = require('crypto');
const { URL } = require('url');
const logger = require('pino')();
const config = require('../config');

// Candidate mainnet hostnames (ordered)
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

/** Fetch wrapper with timeout using AbortController */
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

/** Sign v5 request: timestamp + method + requestPath + body (body is '' for GET) */
function signV5Request(method, requestPath, body = '', apiSecret) {
  const timestamp = Date.now().toString();
  const payload = timestamp + method.toUpperCase() + requestPath + (body ? body : '');
  const signature = crypto.createHmac('sha256', apiSecret).update(payload).digest('hex');
  return { signature, timestamp };
}

/** Decide best base for calls:
 *  - priority: chosenBase (from probe), configured base env, then host candidate list first entry
 */
function getBase() {
  const configured = getConfiguredBase();
  if (chosenBase) return chosenBase;
  if (configured) return configured;
  return HOST_CANDIDATES[0];
}

/**
 * probeHosts(timeoutMs)
 * Try each host candidate with a lightweight v5 kline request for BTCUSDT.
 * Accept a host when it returns HTTP OK; prefer a host with valid JSON, but accept OK responses even if JSON parse fails.
 */
async function probeHosts(timeoutMs = 5000) {
  const configured = getConfiguredBase();
  const list = configured ? [configured, ...HOST_CANDIDATES.filter(h => h !== configured)] : HOST_CANDIDATES;

  logger.info({ candidates: list }, 'bybitRest: starting host probe');

  for (const host of list) {
    const testUrl = `${host.replace(/\/$/, '')}/v5/market/kline?category=linear&symbol=BTCUSDT&interval=60&limit=1`;
    try {
      const res = await fetchWithTimeout(testUrl, { method: 'GET' }, timeoutMs);
      // Log HTTP status so it's visible in logs
      logger.info({ host, status: res.status }, 'probeHosts: host responded');

      // prefer to parse JSON and validate, but accept OK responses even if JSON parse fails
      let json = null;
      try {
        json = await res.json();
      } catch (parseErr) {
        // parse failed — log it but accept host if HTTP was OK
        logger.debug({ host, err: parseErr && parseErr.message ? parseErr.message : String(parseErr) }, 'probeHosts: json parse failed for host');
      }

      if (res.ok) {
        // if we got usable JSON that's best — accept host
        if (json) {
          chosenBase = host.replace(/\/$/, '');
          logger.info({ chosenBase }, 'probeHosts: selected host (json ok)');
          return chosenBase;
        }
        // if no json but status OK, accept host (makes probe resilient to proxies/truncation)
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

  // if no hosts responded successfully, log and optionally fall back to configured base
  const configuredBase = getConfiguredBase();
  if (configuredBase) {
    logger.warn({ configuredBase }, 'probeHosts: no host probe succeeded — falling back to configured BYBIT_REST_BASE');
    chosenBase = configuredBase;
    return chosenBase;
  }

  logger.warn('probeHosts: no hosts responded successfully and no configured BYBIT_REST_BASE');
  return null;
}

/**
 * fetchKlines(symbol, interval, limit)
 * Tries host candidates (or configured/chosen base) and multiple endpoint paths, normalizes common shapes.
 * Returns array of { open_time, open, high, low, close, volume }
 */
async function fetchKlines(symbol, interval, limit = 200) {
  const configured = getConfiguredBase();
  const hostList = configured ? [configured, ...HOST_CANDIDATES.filter(h => h !== configured)] : HOST_CANDIDATES;
  // If probe chose a base, prioritize it.
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

        // v5: { result: { list: [ ... ] } }
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

        // legacy: json.result is array-of-arrays or array-of-objects
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

        // top-level array
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

/**
 * fetchAllSymbols()
 * Tries multiple hosts and v5/v2 instrument endpoints; returns array of normalized instrument objects:
 *  { symbol, base, quote, status }
 */
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
          logger.debug({ host, path: c.path, status: res.status, body: json }, 'fetchAllSymbols: HTTP error');
          continue;
        }

        // v5 shape: { result: { list: [ {symbol, baseCoin, quoteCoin, status}, ... ] } }
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
        }

        if (symbols && symbols.length) {
          logger.info({ host, path: c.path, count: symbols.length }, 'fetchAllSymbols: fetched symbols from host');
          return symbols;
        }

        // nothing useful from this candidate — log and continue
        logger.debug({ host, path: c.path, body: json }, 'fetchAllSymbols: unexpected/empty shape; trying next');
      } catch (err) {
        logger.debug({ host, path: c.path, err: err && err.message ? err.message : String(err) }, 'fetchAllSymbols: candidate threw, trying next');
      }
    }
  }

  logger.error('fetchAllSymbols: all hosts/candidates failed to return symbols');
  return [];
}

/**
 * fetchTicker24h(symbol)
 * Normalizes ticker shapes from various endpoints.
 */
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

        // v5: { result: { list: [ {...} ] } }
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

        // legacy / v2: top-level array or result array
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

/**
 * getWalletBalance(coin)
 * Signed v5 call to account wallet-balance.
 */
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

  // Normalise: result.list[0] or result[coin] or top-level result
  if (json && json.result) {
    if (Array.isArray(json.result.list) && json.result.list.length) return json.result.list[0];
    if (json.result[coin]) return json.result[coin];
    return json.result;
  }
  return json;
}

/**
 * placeMarketOrderV5(order)
 * Signed v5 POST to create an order. Returns the parsed JSON from Bybit.
 *
 * Note: Field names for takeProfit/stopLoss may differ depending on Bybit version — adjust if you see errors.
 */
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

/**
 * setPositionTradingStop({ category, symbol, stopLoss })
 * Signed v5 POST to set position trading stop (stop loss/take profit).
 */
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

/**
 * fetchOpenOrders / fetchOpenPositions
 * Signed GET calls to Bybit v5 private endpoints.
 */
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
