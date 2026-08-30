// src/services/bybitRest.js
// Bybit REST helper with multi-host resilience, robust parsing, and v5 signed requests support.

const fetch = require('node-fetch');
const crypto = require('crypto');
const logger = require('pino')();
const config = require('../config');

// Candidate mainnet hosts to try (ordered). Frankfurt-hosted Render instances often work best against nearby mirrors.
// Add/remove hosts as necessary for your region.
const HOST_CANDIDATES = [
  'https://api.bybit.com',
  'https://bybit.com',
  'https://bybits.com',
  'https://bybit.nl',
  'https://bybit.co.uk',
  'https://bybit.eu'
];

// Utility: choose base from config if set, otherwise try list (we still attempt candidates in fetch calls)
function getConfiguredBase() {
  if (config.BYBIT_REST_BASE) return config.BYBIT_REST_BASE.replace(/\/$/, '');
  return null;
}

// Helper: sign v5 request (Bybit Unified API v5 signature format).
// NOTE: This uses the common v5 pattern: signature = HMAC_SHA256(secret, timestamp + method + requestPath + body)
// requestPath should include the path + query string (e.g. "/v5/position/list?category=linear")
// Body for GET requests is an empty string.
// Headers used: X-BAPI-API-KEY, X-BAPI-SIGN, X-BAPI-TIMESTAMP, X-BAPI-RECV-WINDOW
function signV5Request(method, requestPath, body = '', apiSecret) {
  const timestamp = Date.now().toString();
  const payload = timestamp + method.toUpperCase() + requestPath + (body ? body : '');
  const signature = crypto.createHmac('sha256', apiSecret).update(payload).digest('hex');
  return { signature, timestamp };
}

module.exports = {
  // Multi-candidate kline fetch + normalization
  /**
   * Fetch klines with multiple endpoint fallbacks and normalization.
   * Returns array of { open_time, open, high, low, close, volume }
   */
  async fetchKlines(symbol, interval, limit = 200) {
    const configuredBase = getConfiguredBase();
    const candidates = [
      // typical Bybit v5 market kline
      { path: '/v5/market/kline', params: { category: 'linear', symbol, interval, limit: String(limit) } },
      // legacy endpoints (some hosts expose older path)
      { path: '/v2/public/kline', params: { symbol, interval, limit: String(limit) } },
      { path: '/v2/public/kline/list', params: { symbol, interval, limit: String(limit) } }
    ];

    // If the user has set BYBIT_REST_BASE in config, attempt that host first.
    const hostList = configuredBase ? [configuredBase, ...HOST_CANDIDATES.filter(h => h !== configuredBase)] : HOST_CANDIDATES;

    for (const host of hostList) {
      for (const c of candidates) {
        try {
          const url = new URL(`${host}${c.path}`);
          Object.entries(c.params || {}).forEach(([k, v]) => {
            if (v !== undefined && v !== null) url.searchParams.append(k, String(v));
          });

          const res = await fetch(url.toString(), { method: 'GET' });
          let json;
          try { json = await res.json(); } catch (e) { json = null; }

          if (!res.ok) {
            logger.debug({ host, path: c.path, status: res.status, body: json }, 'fetchKlines failed (HTTP status)');
            continue;
          }

          // v5 shape: { retCode:0, result: { list: [...] } }
          if (json && ((json.result && Array.isArray(json.result.list)) || Array.isArray(json.result))) {
            // normalize both v5 and some legacy shapes
            let arr = [];
            if (json.result && Array.isArray(json.result.list)) arr = json.result.list;
            else if (json.result && Array.isArray(json.result)) arr = json.result;
            // if top-level array
            else if (Array.isArray(json)) arr = json;

            // If arr is array-of-arrays (legacy) OR array-of-objects
            if (arr.length && Array.isArray(arr[0])) {
              // array of arrays: [timestamp, open, high, low, close, volume, ...]
              return arr.map(r => ({
                open_time: r[0],
                open: Number(r[1]),
                high: Number(r[2]),
                low: Number(r[3]),
                close: Number(r[4]),
                volume: Number(r[5])
              }));
            } else if (arr.length && typeof arr[0] === 'object') {
              // array of objects - various keys supported
              return arr.map(r => ({
                open_time: r.start || r.start_at || r.t || r.open_time || r[0],
                open: Number(r.open || r.o || r[1] || r.k?.o || 0),
                high: Number(r.high || r.h || r[2] || r.k?.h || 0),
                low: Number(r.low || r.l || r[3] || r.k?.l || 0),
                close: Number(r.close || r.c || r[4] || r.k?.c || 0),
                volume: Number(r.volume || r.v || r[5] || r.k?.v || 0)
              }));
            }
          }

          // Catch-all: sometimes v5 returns nested shapes; attempt to extract a list property
          if (json && json.result && Array.isArray(json.result.data)) {
            const arr = json.result.data;
            return arr.map(r => ({
              open_time: r.start || r.t || r[0],
              open: Number(r.open || r.o || r[1] || 0),
              high: Number(r.high || r.h || r[2] || 0),
              low: Number(r.low || r.l || r[3] || 0),
              close: Number(r.close || r.c || r[4] || 0),
              volume: Number(r.volume || r.v || r[5] || 0)
            }));
          }

          logger.debug({ host, path: c.path, body: json }, 'fetchKlines candidate returned unexpected shape; trying next');
        } catch (err) {
          logger.warn({ err, candidateHost: host, candidatePath: c.path, symbol, interval }, 'fetchKlines candidate threw, trying next');
        }
      } // end candidates loop
    } // end hostList loop

    logger.error({ symbol, interval }, 'fetchKlines: all endpoint candidates failed');
    return [];
  },

  /**
   * Fetch open (realtime) orders using Bybit v5 `/v5/order/realtime`
   * Requires API key + secret in env: BYBIT_API_KEY and BYBIT_API_SECRET
   *
   * Example query: ?category=linear&symbol=BTCUSDT
   */
  async fetchOpenOrders({ category = 'linear', symbol = null } = {}) {
    const apiKey = process.env.BYBIT_API_KEY;
    const apiSecret = process.env.BYBIT_API_SECRET;
    if (!apiKey || !apiSecret) {
      throw new Error('fetchOpenOrders requires BYBIT_API_KEY and BYBIT_API_SECRET environment variables');
    }

    // Build path + query
    const query = new URLSearchParams();
    if (category) query.append('category', category);
    if (symbol) query.append('symbol', symbol);
    const requestPath = `/v5/order/realtime${query.toString() ? `?${query.toString()}` : ''}`;

    // Choose base
    const base = getConfiguredBase() || HOST_CANDIDATES[0];
    // Sign
    const { signature, timestamp } = signV5Request('GET', requestPath, '', apiSecret);

    const url = `${base}${requestPath}`;
    const headers = {
      'Content-Type': 'application/json',
      'X-BAPI-API-KEY': apiKey,
      'X-BAPI-TIMESTAMP': timestamp,
      'X-BAPI-SIGN': signature,
      'X-BAPI-RECV-WINDOW': '5000'
    };

    const res = await fetch(url, { method: 'GET', headers });
    const json = await res.json().catch(() => null);
    if (!res.ok) {
      logger.warn({ url, status: res.status, body: json }, 'fetchOpenOrders HTTP error');
      throw new Error(`fetchOpenOrders HTTP ${res.status}`);
    }
    // Return raw response (application code can normalize as needed)
    return json;
  },

  /**
   * Fetch positions (open positions) using Bybit v5 `/v5/position/list`
   * Requires API key + secret in env.
   */
  async fetchOpenPositions({ category = 'linear', symbol = null } = {}) {
    const apiKey = process.env.BYBIT_API_KEY;
    const apiSecret = process.env.BYBIT_API_SECRET;
    if (!apiKey || !apiSecret) {
      throw new Error('fetchOpenPositions requires BYBIT_API_KEY and BYBIT_API_SECRET environment variables');
    }

    const query = new URLSearchParams();
    if (category) query.append('category', category);
    if (symbol) query.append('symbol', symbol);
    const requestPath = `/v5/position/list${query.toString() ? `?${query.toString()}` : ''}`;

    const base = getConfiguredBase() || HOST_CANDIDATES[0];
    const { signature, timestamp } = signV5Request('GET', requestPath, '', apiSecret);

    const url = `${base}${requestPath}`;
    const headers = {
      'Content-Type': 'application/json',
      'X-BAPI-API-KEY': apiKey,
      'X-BAPI-TIMESTAMP': timestamp,
      'X-BAPI-SIGN': signature,
      'X-BAPI-RECV-WINDOW': '5000'
    };

    const res = await fetch(url, { method: 'GET', headers });
    const json = await res.json().catch(() => null);
    if (!res.ok) {
      logger.warn({ url, status: res.status, body: json }, 'fetchOpenPositions HTTP error');
      throw new Error(`fetchOpenPositions HTTP ${res.status}`);
    }
    return json;
  }
};
