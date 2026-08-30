/**
 * src/services/bybitRest.js
 *
 * - Ensures chosenBase (from probeHosts or config) is tried first exclusively.
 * - Falls back to candidate hosts only if chosenBase fails.
 * - Persists chosenBase to disk and records probe diagnostics.
 * - Filters returned symbols to USDT/perp-like by default (configurable via SYMBOL_FILTER_REGEX).
 * - Full helper implementations (klines, ticker, signed v5 account/order calls).
 */

const fetch = require('node-fetch');
const crypto = require('crypto');
const { URL } = require('url');
const fs = require('fs');
const path = require('path');
const logger = require('pino')();
const config = require('../config');

// Preferred order: mainnet and testnet first
const HOST_CANDIDATES = [
  'https://api.bybit.com',
  'https://api-testnet.bybit.com',
  'https://bybit.com',
  'https://bybits.com',
  'https://bybit.nl',
  'https://bybit.co.uk',
  'https://bybit.eu'
];

let chosenBase = null;
const CHOSEN_BASE_FILE = path.join(__dirname, '..', 'data', 'chosen_bybit_base.txt');

// Diagnostics: last probe host/path and last OK response text
let lastProbeHostPath = null;
let lastProbeResponseText = null;

function getConfiguredBase() {
  const envBase = process.env.BYBIT_REST_BASE || (config && config.BYBIT_REST_BASE);
  if (!envBase) return null;
  return String(envBase).replace(/\/$/, '');
}

function loadChosenBaseFromDisk() {
  try {
    if (fs.existsSync(CHOSEN_BASE_FILE)) {
      const b = String(fs.readFileSync(CHOSEN_BASE_FILE, 'utf8') || '').trim();
      if (b) {
        chosenBase = b.replace(/\/$/, '');
        logger.info({ chosenBase }, 'bybitRest: loaded chosenBase from disk');
      }
    }
  } catch (e) {
    logger.debug({ e }, 'bybitRest: failed to read chosen base file');
  }
}

function saveChosenBaseToDisk(base) {
  try {
    fs.mkdirSync(path.dirname(CHOSEN_BASE_FILE), { recursive: true });
    fs.writeFileSync(CHOSEN_BASE_FILE, String(base || ''), 'utf8');
  } catch (e) {
    logger.debug({ e }, 'bybitRest: failed to persist chosen base');
  }
}
loadChosenBaseFromDisk();

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

/** Sign v5 request helper */
function signV5Request(method, requestPath, body = '', apiSecret) {
  const timestamp = Date.now().toString();
  const payload = timestamp + method.toUpperCase() + requestPath + (body ? body : '');
  const signature = crypto.createHmac('sha256', apiSecret).update(payload).digest('hex');
  return { signature, timestamp };
}

/** Get base host: chosenBase -> configured -> first candidate */
function getBase() {
  const configured = getConfiguredBase();
  if (chosenBase) return chosenBase;
  if (configured) return configured;
  return HOST_CANDIDATES[0];
}

/**
 * probeHosts(timeoutMs)
 * Try candidate hosts with a quick v5 kline request for BTCUSDT.
 * Accept a host on HTTP OK; persist chosen base and record diagnostics.
 */
async function probeHosts(timeoutMs = 5000) {
  const configured = getConfiguredBase();
  const list = configured ? [configured, ...HOST_CANDIDATES.filter(h => h !== configured)] : HOST_CANDIDATES.slice();

  logger.info({ candidates: list }, 'bybitRest: starting host probe');

  for (const host of list) {
    const testUrl = `${host.replace(/\/$/, '')}/v5/market/kline?category=linear&symbol=BTCUSDT&interval=60&limit=1`;
    try {
      const res = await fetchWithTimeout(testUrl, { method: 'GET' }, timeoutMs);
      logger.info({ host, status: res.status }, 'probeHosts: host responded');

      // capture text for diagnostics
      let text = null;
      let parsed = null;
      try {
        text = await res.text();
        try { parsed = JSON.parse(text); } catch (e) { parsed = null; }
      } catch (e) {
        text = null;
      }

      if (res.ok) {
        lastProbeHostPath = { host, path: testUrl };
        lastProbeResponseText = text;
        chosenBase = host.replace(/\/$/, '');
        saveChosenBaseToDisk(chosenBase);
        if (parsed) {
          logger.info({ chosenBase }, 'probeHosts: selected host (json ok)');
        } else {
          logger.info({ chosenBase, note: 'accepted despite json parse failure (HTTP OK)' }, 'probeHosts: selected host');
        }
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
    chosenBase = configuredBase;
    saveChosenBaseToDisk(chosenBase);
    logger.warn({ configuredBase }, 'probeHosts: falling back to configured BYBIT_REST_BASE');
    return chosenBase;
  }

  logger.warn('probeHosts: no hosts responded successfully and no configured BYBIT_REST_BASE');
  return null;
}

/** Helper: fetch symbols from CoinGecko markets as a fallback */
async function fetchSymbolsFromCoinGecko(perPage = 250) {
  try {
    const qUrl = `https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=${perPage}&page=1&sparkline=false`;
    const res = await fetchWithTimeout(qUrl, { method: 'GET' }, 8000);
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

/**
 * fetchAllSymbols()
 * Behavior: if chosenBase exists, try it exclusively first. If that fails or returns no usable symbols,
 * fall back to trying the full host list. Last-resort fallback to CoinGecko.
 */
async function fetchAllSymbols() {
  logger.info('bybitRest.fetchAllSymbols: start');

  const DEFAULT_SYMBOL_FILTER = process.env.SYMBOL_FILTER_REGEX ? new RegExp(process.env.SYMBOL_FILTER_REGEX) : /usdt(\.p|\.P|P)?$/i;
  const configured = getConfiguredBase();
  let hostList = configured ? [configured, ...HOST_CANDIDATES.filter(h => h !== configured)] : HOST_CANDIDATES.slice();

  if (chosenBase) {
    if (!hostList.includes(chosenBase)) hostList.unshift(chosenBase);
    else hostList = [chosenBase, ...hostList.filter(h => h !== chosenBase)];
  }

  const candidates = [
    { path: '/v5/market/instruments', params: {} },
    { path: '/v5/market/instruments-info', params: {} },
    { path: '/v2/public/symbols', params: {} },
    { path: '/v2/public/tickers', params: {} }
  ];

  let lastOkResponseText = null;
  let lastOkHostPath = null;

  async function attemptHostForSymbols(host, timeoutMs = 8000) {
    for (const c of candidates) {
      try {
        const url = new URL(`${host.replace(/\/$/, '')}${c.path}`);
        Object.entries(c.params || {}).forEach(([k, v]) => {
          if (v !== undefined && v !== null) url.searchParams.append(k, String(v));
        });

        const res = await fetchWithTimeout(url.toString(), { method: 'GET' }, timeoutMs);

        let json = null;
        let bodyText = null;
        try {
          bodyText = await res.text();
          try { json = JSON.parse(bodyText); } catch (e) { json = null; }
        } catch (e) {
          bodyText = null;
        }

        if (!res.ok) {
          logger.debug({ host, path: c.path, status: res.status }, 'attemptHostForSymbols: HTTP error');
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
          logger.debug({ host, path: c.path, snippet: lastOkResponseText ? lastOkResponseText.slice(0, 400) : null }, 'attemptHostForSymbols: parsed json null despite HTTP OK');
        }

        if (symbols && symbols.length) {
          const filtered = symbols.filter(s => s && s.symbol && DEFAULT_SYMBOL_FILTER.test(String(s.symbol)));
          if (filtered.length) {
            logger.info({ host, path: c.path, count: filtered.length }, 'attemptHostForSymbols: fetched and filtered symbols from host');
            return { symbols: filtered };
          } else {
            logger.info({ host, path: c.path, count: symbols.length, note: 'no symbols matched filter' });
            return { symbols: [], rawCount: symbols.length };
          }
        }
      } catch (err) {
        logger.debug({ host, path: c.path, err: err && err.message ? err.message : String(err) }, 'attemptHostForSymbols: candidate threw');
      }
    }
    return { symbols: null };
  }

  // 1) If chosenBase exists, try exclusively first
  if (chosenBase) {
    logger.info({ chosenBase }, 'fetchAllSymbols: attempting chosenBase exclusively first');
    try {
      const result = await attemptHostForSymbols(chosenBase, 8000);
      if (Array.isArray(result.symbols) && result.symbols.length) {
        lastProbeHostPath = lastProbeHostPath || lastOkHostPath;
        lastProbeResponseText = lastProbeResponseText || lastOkResponseText;
        return result.symbols;
      } else if (Array.isArray(result.symbols) && result.symbols.length === 0) {
        logger.warn({ chosenBase, rawCount: result.rawCount }, 'fetchAllSymbols: chosenBase returned symbols but none matched filter; falling back');
      } else {
        logger.warn({ chosenBase }, 'fetchAllSymbols: chosenBase attempt failed to produce symbols; falling back');
      }
    } catch (e) {
      logger.debug({ e }, 'fetchAllSymbols: chosenBase exclusive attempt threw, falling back');
    }
  }

  // 2) Fallback to full list (skip chosenBase if already tried)
  for (const host of hostList) {
    if (chosenBase && host.replace(/\/$/, '') === chosenBase) continue;
    try {
      const result = await attemptHostForSymbols(host, 8000);
      if (Array.isArray(result.symbols) && result.symbols.length) {
        logger.info({ host }, 'fetchAllSymbols: succeeded using fallback host');
        if (!chosenBase) {
          chosenBase = host.replace(/\/$/, '');
          saveChosenBaseToDisk(chosenBase);
          logger.info({ chosenBase }, 'fetchAllSymbols: setting chosenBase from fallback host');
        }
        return result.symbols;
      } else {
        logger.debug({ host }, 'fetchAllSymbols: this host returned no usable filtered symbols, trying next host');
        continue;
      }
    } catch (err) {
      logger.debug({ host, err: err && err.message ? err.message : String(err) }, 'fetchAllSymbols: host attempt threw');
      continue;
    }
  }

  if (lastOkHostPath) {
    logger.warn({ lastOkHostPath, snippet: lastOkResponseText ? lastOkResponseText.slice(0, 800) : null }, 'fetchAllSymbols: all hosts/candidates failed to return usable symbols (last OK snippet included)');
  } else {
    logger.warn('fetchAllSymbols: no host returned HTTP OK during attempts');
  }

  // Last-resort CoinGecko fallback (filtered)
  try {
    const cgSymbols = await fetchSymbolsFromCoinGecko(250);
    if (cgSymbols && cgSymbols.length) {
      const filtered = cgSymbols.filter(s => s.symbol && DEFAULT_SYMBOL_FILTER.test(String(s.symbol)));
      logger.info({ count: filtered.length }, 'fetchAllSymbols: fallback to CoinGecko (filtered) succeeded');
      return filtered;
    }
    logger.warn('fetchAllSymbols: CoinGecko fallback returned no symbols');
  } catch (e) {
    logger.debug({ e }, 'fetchAllSymbols: CoinGecko fallback threw');
  }

  return [];
}

/**
 * fetchKlines(symbol, interval, limit)
 * Tries chosenBase first then fallback hosts.
 */
async function fetchKlines(symbol, interval, limit = 200) {
  const configured = getConfiguredBase();
  let hostList = configured ? [configured, ...HOST_CANDIDATES.filter(h => h !== configured)] : HOST_CANDIDATES.slice();
  if (chosenBase) {
    if (!hostList.includes(chosenBase)) hostList.unshift(chosenBase);
    else hostList = [chosenBase, ...hostList.filter(h => h !== chosenBase)];
  }

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

        const res = await fetchWithTimeout(url.toString(), { method: 'GET' }, 8000);
        let json = null;
        try { json = await res.json(); } catch (e) { json = null; }

        if (!res.ok) {
          logger.debug({ host, path: c.path, status: res.status, body: json }, 'fetchKlines: HTTP error');
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

        logger.debug({ host, path: c.path, body: json }, 'fetchKlines: unexpected shape, trying next');
      } catch (err) {
        logger.debug({ host, path: c.path, err: err && err.message ? err.message : String(err) }, 'fetchKlines: candidate threw');
      }
    }
  }

  logger.error({ symbol, interval }, 'fetchKlines: all candidates/hosts failed');
  return [];
}

/**
 * fetchTicker24h(symbol)
 */
async function fetchTicker24h(symbol) {
  const configured = getConfiguredBase();
  let hostList = configured ? [configured, ...HOST_CANDIDATES.filter(h => h !== configured)] : HOST_CANDIDATES.slice();
  if (chosenBase) {
    if (!hostList.includes(chosenBase)) hostList.unshift(chosenBase);
    else hostList = [chosenBase, ...hostList.filter(h => h !== chosenBase)];
  }

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
        const res = await fetchWithTimeout(url.toString(), { method: 'GET' }, 8000);
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

/**
 * Signed/private endpoints: getWalletBalance, placeMarketOrderV5, setPositionTradingStop, fetchOpenOrders, fetchOpenPositions
 * Uses v5 signing (timestamp + method + requestPath + body) HMAC-SHA256
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

  const res = await fetchWithTimeout(url, { method: 'GET', headers }, 9000);
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

  const res = await fetchWithTimeout(url, { method: 'POST', body: bodyStr, headers }, 9000);
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

  const res = await fetchWithTimeout(url, { method: 'POST', body: bodyStr, headers }, 9000);
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

  const res = await fetchWithTimeout(url, { method: 'GET', headers }, 9000);
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

  const res = await fetchWithTimeout(url, { method: 'GET', headers }, 9000);
  const json = await res.json().catch(() => null);
  if (!res.ok) {
    logger.warn({ url, status: res.status, body: json }, 'fetchOpenPositions HTTP error');
    throw new Error(`fetchOpenPositions HTTP ${res.status}`);
  }
  return json;
}

/** Diagnostics helpers */
function getLastProbeInfo() {
  return {
    chosenBase,
    lastProbeHostPath,
    lastProbeResponseText: lastProbeResponseText ? String(lastProbeResponseText).slice(0, 2000) : null
  };
}

async function reprobe(timeoutMs = 5000) {
  lastProbeHostPath = null;
  lastProbeResponseText = null;
  const base = await probeHosts(timeoutMs);
  return base;
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
  fetchOpenPositions,
  // diagnostics
  getLastProbeInfo,
  reprobe
};
