/**
 * src/services/bybitRest.js
 *
 * Bybit REST helper with:
 * - multi-host resilience (multiple mainnet hosts tried)
 * - startup host probing (optional explicit probeHosts() call)
 * - robust parsing/normalization of kline and symbols responses
 * - v5 signed request helpers for private endpoints (open orders / positions)
 *
 * Exports:
 *  - probeHosts(timeoutMs)
 *  - getBase()
 *  - fetchKlines(symbol, interval, limit)
 *  - fetchAllSymbols()
 *  - fetchTicker24h(symbol)
 *  - getWalletBalance(coin)
 *  - placeMarketOrderV5(order)
 *  - setPositionTradingStop(opts)
 *  - fetchOpenOrders({category, symbol})
 *  - fetchOpenPositions({category, symbol})
 */

const fetch = require('node-fetch'); // explicit
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
 * Normalizes kline responses across bybit endpoints; tries host candidates with chosenBase prioritized.
 */
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

        // legacy shapes...
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

/* The rest of the file (fetchTicker24h, getWalletBalance, placeMarketOrderV5, setPositionTradingStop,
   fetchOpenOrders, fetchOpenPositions) remains unchanged from previous iteration — omitted here for brevity.
   If you need the complete file including these helper functions I can paste the full file (they were included
   in previous messages). */

module.exports = {
  probeHosts,
  getBase,
  fetchKlines,
  fetchAllSymbols,
  // other helpers should be exported if present in your copy
};
