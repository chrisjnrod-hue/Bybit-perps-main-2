/**
 * src/services/bybitRest.js
 *
 * Bybit REST helper with:
 * - multi-host resilience (multiple mainnet hosts tried)
 * - startup host probing (optional explicit probeHosts() call)
 * - robust parsing/normalization of kline and symbols responses
 * - v5 signed request helpers for private endpoints (open orders / positions)
 *
 * Usage:
 *  - Option A (recommended): call `await require('./src/services/bybitRest').probeHosts()` during startup
 *    (e.g. in src/index.js after db.init()) to pick the fastest working host.
 *  - Option B: let the module operate without probing — it will try configured BYBIT_REST_BASE then fallbacks per-call.
 *
 * Exports:
 *  - probeHosts(timeoutMs)           -> Promise<string | null>  (resolves to chosen base or null)
 *  - getBase()                       -> string (best-effort base URL)
 *  - fetchKlines(symbol, interval, limit)
 *  - fetchAllSymbols()
 *  - fetchOpenOrders({category, symbol})
 *  - fetchOpenPositions({category, symbol})
 *
 * Requirements:
 *  - node-fetch (already present in project dependencies); Node 18+ has fetch builtin but we use node-fetch to be explicit.
 *  - If you call fetchOpenOrders / fetchOpenPositions you MUST set BYBIT_API_KEY and BYBIT_API_SECRET in Render environment.
 *
 * Notes:
 *  - probeHosts performs light GET requests to the v5 kline endpoint for a single sample symbol (BTCUSDT).
 *  - All functions log informative debug messages via pino; set LOG_LEVEL=debug in env for verbose logs.
 */

const fetch = require('node-fetch'); // keep explicit for older Node or consistent behavior
const crypto = require('crypto');
const { URL } = require('url');
const logger = require('pino')();
const config = require('../config'); // your repo's config module

// Candidate mainnet hostnames (ordered). Add/remove hosts as you prefer.
// Frankfurt/Europe often routes well to bybit.com or bybit.nl; we include common mirrors.
const HOST_CANDIDATES = [
  'https://api.bybit.com',
  'https://bybit.com',
  'https://bybits.com',
  'https://bybit.nl',
  'https://bybit.co.uk',
  'https://bybit.eu'
];

// Module-level chosen base (set by probeHosts or falls back per call)
let chosenBase = null;

/** Utility: get configured BYBIT_REST_BASE (from config or env) */
function getConfiguredBase() {
  const envBase = process.env.BYBIT_REST_BASE || (config && config.BYBIT_REST_BASE);
  if (!envBase) return null;
  return String(envBase).replace(/\/$/, '');
}

/** Fetch wrapper with timeout using AbortController */
async function fetchWithTimeout(url, opts = {}, timeoutMs = 5000) {
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
 * Try each host candidate with a lightweight v5 kline request for BTCUSDT to find a working host.
 * Returns the chosen base URL string or null if none succeeded.
 *
 * Usage: await probeHosts(3000);
 */
async function probeHosts(timeoutMs = 3000) {
  const configured = getConfiguredBase();
  const list = configured ? [configured, ...HOST_CANDIDATES.filter(h => h !== configured)] : HOST_CANDIDATES;

  logger.info({ candidates: list }, 'bybitRest: starting host probe');

  for (const host of list) {
    try {
      const testUrl = `${host.replace(/\/$/, '')}/v5/market/kline?category=linear&symbol=BTCUSDT&interval=60&limit=1`;
      const res = await fetchWithTimeout(testUrl, { method: 'GET' }, timeoutMs);
      if (!res.ok) {
        logger.debug({ host, status: res.status }, 'probeHosts: non-ok response');
        continue;
      }
      // validate JSON parse
      const json = await res.json().catch(() => null);
      if (!json) {
        logger.debug({ host }, 'probeHosts: json parse failed');
        continue;
      }
      // if we get any JSON, accept this host
      chosenBase = host.replace(/\/$/, '');
      logger.info({ chosenBase }, 'probeHosts: selected host');
      return chosenBase;
    } catch (err) {
      logger.debug({ host, err: err && err.message ? err.message : String(err) }, 'probeHosts: host error, trying next');
      continue;
    }
  }

  logger.warn('probeHosts: no hosts responded successfully');
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
        if (json && json.result && Array.isArray(json.result.list)) {
          return json.result.list.map(it => ({
            symbol: it.symbol || it.name || null,
            base: it.baseCoin || it.base || null,
            quote: it.quoteCoin || it.quote || null,
            status: it.status || it.state || null
          })).filter(Boolean);
        }

        // v5 sometimes returns result array directly
        if (json && json.result && Array.isArray(json.result)) {
          return json.result.map(it => ({
            symbol: it.symbol || it.name || null,
            base: it.baseCoin || it.base || null,
            quote: it.quoteCoin || it.quote || null,
            status: it.status || it.state || null
          })).filter(Boolean);
        }

        // v2 legacy: json.result array-of-objects
        if (json && json.result && Array.isArray(json.result)) {
          return json.result.map(it => ({
            symbol: it.name || it.symbol || null,
            base: it.base || it.baseCoin || null,
            quote: it.quote || it.quoteCoin || null,
            status: it.status || it.state || null
          })).filter(Boolean);
        }

        // top-level array fallback
        if (Array.isArray(json)) {
          return json.map(it => ({
            symbol: it.symbol || it.name || null,
            base: it.base || it.baseCoin || null,
            quote: it.quote || it.quoteCoin || null,
            status: it.status || it.state || null
          })).filter(Boolean);
        }

        logger.debug({ host, path: c.path, body: json }, 'fetchAllSymbols: unexpected shape; trying next');
      } catch (err) {
        logger.debug({ host, path: c.path, err: err && err.message ? err.message : String(err) }, 'fetchAllSymbols: candidate threw, trying next');
      }
    }
  }

  logger.error('fetchAllSymbols: all hosts/candidates failed');
  return [];
}

/**
 * fetchOpenOrders / fetchOpenPositions
 * These call Bybit v5 private endpoints and require BYBIT_API_KEY and BYBIT_API_SECRET in env (or config).
 * They return the raw JSON that Bybit returns (usually under result or retMsg/retCode). Caller can normalize further.
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

// Export public API
module.exports = {
  probeHosts,
  getBase,
  fetchKlines,
  fetchAllSymbols,
  fetchOpenOrders,
  fetchOpenPositions
};
