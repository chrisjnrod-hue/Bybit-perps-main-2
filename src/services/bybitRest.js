// src/services/bybitRest.js
/**
 * Bybit REST helpers (fetchAllSymbols focused on PERPETUAL USDT contracts)
 *
 * - Cursor-based pagination to fetch instruments-info pages
 * - Default filter: instrumentType === 'PERPETUAL', symbol ends with USDT or USDT.P
 * - Exclude symbols that contain '-' (expiry/dated contracts like XRPUSDT-25SEP26)
 * - Respect SYMBOL_FILTER_REGEX env to override filter if desired
 *
 * Keep the rest of the file (fetchKlines, fetchTicker24h, signed endpoints) unchanged
 * if you already had working implementations — this file includes them in full.
 */

const fetch = require('node-fetch');
const crypto = require('crypto');
const { URL } = require('url');
const fs = require('fs');
const path = require('path');
const logger = require('pino')();
const config = require('../config');

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

let lastProbeHostPath = null;
let lastProbeResponseText = null;

function envBool(name, defaultVal = false) {
  if (typeof process.env[name] === 'undefined') return defaultVal;
  const v = String(process.env[name]).toLowerCase().trim();
  return v === '1' || v === 'true' || v === 'yes';
}

const OPENTRADES = envBool('OPENTRADES', false);
const MAINNET = envBool('MAINNET', true);

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
        const configured = getConfiguredBase();
        const hasMainnetEnv = typeof process.env.MAINNET !== 'undefined';
        if (!configured && !hasMainnetEnv) {
          chosenBase = b.replace(/\/$/, '');
          logger.info({ chosenBase }, 'bybitRest: loaded chosenBase from disk');
        } else {
          logger.info({ diskChosen: b, configured, MAINNET: process.env.MAINNET }, 'bybitRest: ignoring chosen_bybit_base.txt because environment/config is present');
        }
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

function getOrderBase() {
  const explicit = process.env.BYBIT_ORDER_BASE;
  if (explicit) return String(explicit).replace(/\/$/, '');

  if (typeof process.env.MAINNET !== 'undefined') {
    return MAINNET ? 'https://api.bybit.com' : 'https://api-testnet.bybit.com';
  }

  const useTestnetRaw = typeof process.env.BYBIT_USE_TESTNET !== 'undefined' ? String(process.env.BYBIT_USE_TESTNET) : null;
  if (useTestnetRaw !== null) {
    const val = useTestnetRaw.toLowerCase();
    if (val === '1' || val === 'true' || val === 'yes') return 'https://api-testnet.bybit.com';
    if (val === '0' || val === 'false' || val === 'no') return 'https://api.bybit.com';
    return 'https://api-testnet.bybit.com';
  }

  return getBase();
}

async function probeHosts(timeoutMs = 5000) {
  const configured = getConfiguredBase();
  const list = configured ? [configured] : HOST_CANDIDATES.slice();

  logger.info({ candidates: list }, 'bybitRest: starting host probe');

  for (const host of list) {
    const testUrl = `${host.replace(/\/$/, '')}/v5/market/kline?category=linear&symbol=BTCUSDT&interval=60&limit=1`;
    try {
      const res = await fetchWithTimeout(testUrl, { method: 'GET' }, timeoutMs);
      logger.info({ host, status: res.status }, 'probeHosts: host responded');

      let text = null;
      let parsed = null;
      try {
        text = await res.text();
        try { parsed = JSON.parse(text); } catch (e) { parsed = null; }
      } catch (e) {
        text = null;
      }

      if (res.ok) {
        const ct = (res.headers && typeof res.headers.get === 'function') ? (res.headers.get('content-type') || '') : '';
        const looksLikeJson = /application\/json/i.test(ct) || (text && text.trim().startsWith('{'));
        const parsedOkApi = parsed && (parsed.result || typeof parsed.ret_code !== 'undefined' || typeof parsed.retCode !== 'undefined');

        if (!looksLikeJson && !parsedOkApi) {
          logger.debug({ host, path: testUrl, contentType: ct, snippet: text ? text.slice(0, 200) : null }, 'probeHosts: host returned OK but not JSON/API-shaped; skipping');
          continue;
        }

        if (parsed && (typeof parsed.retCode !== 'undefined' || typeof parsed.ret_code !== 'undefined')) {
          const rc = typeof parsed.retCode !== 'undefined' ? parsed.retCode : parsed.ret_code;
          if (rc !== 0) {
            logger.debug({ host, path: testUrl, retCode: rc, snippet: text ? text.slice(0, 200) : null }, 'probeHosts: host returned API error retCode; skipping');
            continue;
          }
        }

        lastProbeHostPath = { host, path: testUrl };
        lastProbeResponseText = text;

        if (configured) {
          logger.info({ configured }, 'probeHosts: configured BYBIT_REST_BASE responded OK; not persisting probe selection');
          return configured;
        }

        chosenBase = host.replace(/\/$/, '');
        saveChosenBaseToDisk(chosenBase);
        if (parsed) {
          logger.info({ chosenBase }, 'probeHosts: selected host (json ok)');
        } else {
          logger.info({ chosenBase, note: 'accepted despite json parse detection (HTTP OK)' }, 'probeHosts: selected host');
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

async function fetchSymbolsFromCoinGecko(perPage = 500) {
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
 * - Cursor-based pagination over /v5/market/instruments-info
 * - Default: instrumentType === 'PERPETUAL' and symbol ends with USDT or USDT.P
 * - Excludes symbols that contain '-' (expiry/dated)
 * - Can be overridden by SYMBOL_FILTER_REGEX env var (full regex)
 */
async function fetchAllSymbols() {
  logger.info('bybitRest.fetchAllSymbols: starting cursor-based pagination for all symbols');

  const DEFAULT_SYMBOL_FILTER = process.env.SYMBOL_FILTER_REGEX
    ? new RegExp(process.env.SYMBOL_FILTER_REGEX)
    : /usdt(\.p)?$/i;

  const base = getBase();
  if (!base) {
    logger.warn('fetchAllSymbols: no base available (getBase returned null)');
    return [];
  }

  const allSymbols = [];
  let cursor = null;
  const limit = config.BYBIT_PAGINATION_LIMIT || 1000;
  let pageNum = 0;
  let totalRawInstruments = 0;
  let totalFiltered = 0;

  try {
    while (true) {
      pageNum++;

      const params = {
        category: 'linear',
        instrumentType: 'PERPETUAL',
        limit: String(limit)
      };
      if (cursor) params.cursor = cursor;

      const url = new URL(`${base.replace(/\/$/, '')}/v5/market/instruments-info`);
      Object.entries(params).forEach(([k, v]) => url.searchParams.append(k, String(v)));

      logger.info({ page: pageNum, cursor: cursor || 'initial', limit, url: url.toString() }, 'fetchAllSymbols: fetching page');

      const res = await fetchWithTimeout(url.toString(), { method: 'GET' }, 10000);
      let json = null;
      let bodyText = null;
      try {
        bodyText = await res.text();
        try { json = JSON.parse(bodyText); } catch (e) { json = null; }
      } catch (e) {
        bodyText = null;
      }

      if (!res.ok) {
        logger.warn({ status: res.status, page: pageNum, url: url.toString() }, 'fetchAllSymbols: HTTP error, stopping pagination');
        break;
      }
      if (!json) {
        logger.warn({ page: pageNum, snippet: bodyText ? bodyText.slice(0, 200) : null }, 'fetchAllSymbols: invalid JSON response, stopping pagination');
        break;
      }

      if (typeof json.ret_code !== 'undefined' || typeof json.retCode !== 'undefined') {
        const rc = typeof json.ret_code !== 'undefined' ? json.ret_code : json.retCode;
        const rm = json.ret_msg || json.retMsg || null;
        if (rc !== 0) {
          logger.warn({ retCode: rc, retMsg: rm, page: pageNum }, 'fetchAllSymbols: API returned non-zero retCode, stopping pagination');
          break;
        }
      }

      const result = json.result || {};
      const instruments = result.list || [];

      if (instruments.length > 0) {
        totalRawInstruments += instruments.length;
        logger.info({ page: pageNum, pageSize: instruments.length, totalRawSoFar: totalRawInstruments }, 'fetchAllSymbols: page fetched, applying filter');

        const filtered = instruments
          .filter(it => {
            if (!it || !it.symbol) return false;
            try {
              const sym = String(it.symbol);
              // Exclude expiry/dated contracts that include a hyphen
              if (/-/.test(sym)) return false;

              // Prefer instrumentType field if present; accept only PERPETUAL
              const itype = (it.instrumentType || it.instrument_type || it.instrument_type_string || '').toString().toUpperCase();
              if (itype && itype !== 'PERPETUAL') return false;

              // Apply symbol regex (default accepts USDT or USDT.P)
              if (!DEFAULT_SYMBOL_FILTER.test(sym)) return false;

              return true;
            } catch (e) {
              return false;
            }
          })
          .map(it => ({
            symbol: it.symbol,
            base: it.baseCoin || it.base || null,
            quote: it.quoteCoin || it.quote || 'USDT',
            status: it.status || null
          }));

        allSymbols.push(...filtered);
        totalFiltered += filtered.length;

        logger.info({ page: pageNum, pageSize: instruments.length, filtered: filtered.length, totalAccumulated: allSymbols.length }, 'fetchAllSymbols: page filtered and accumulated');
      } else {
        logger.info({ page: pageNum }, 'fetchAllSymbols: empty page received');
      }

      cursor = result.nextPageCursor;
      if (!cursor) {
        logger.info({ totalFetched: allSymbols.length, totalRaw: totalRawInstruments }, 'fetchAllSymbols: no nextPageCursor found, pagination complete');
        break;
      }

      await new Promise(resolve => setTimeout(resolve, 100));
    }

    if (allSymbols.length === 0) {
      logger.warn({ totalRawInstruments, totalFiltered }, 'fetchAllSymbols: no symbols matched configured filter after full pagination');
      return [];
    }

    const uniqueMap = new Map();
    for (const s of allSymbols) {
      const key = String(s.symbol).toUpperCase();
      if (!uniqueMap.has(key)) {
        uniqueMap.set(key, {
          symbol: String(s.symbol),
          base: s.base || null,
          quote: s.quote || 'USDT',
          status: s.status || null
        });
      }
    }

    const unique = Array.from(uniqueMap.values());
    unique.sort((a, b) => {
      const A = String(a.symbol || '').toUpperCase();
      const B = String(b.symbol || '').toUpperCase();
      if (A < B) return -1;
      if (A > B) return 1;
      return 0;
    });

    logger.info({ totalUnique: unique.length, sampleFirst: unique.slice(0, 5).map(s => s.symbol), sampleLast: unique.slice(-5).map(s => s.symbol) }, 'fetchAllSymbols: returning all unique symbols (sorted A-Z)');

    return unique;
  } catch (e) {
    logger.error({ e }, 'fetchAllSymbols: exception during pagination');
    return [];
  }
}

/**
 * fetchKlines(symbol, interval, limit)
 * (unchanged robust implementation — uses V5 and legacy V2 endpoints as fallback)
 */
async function fetchKlines(symbol, interval, limit = 200) {
  const base = getBase();
  if (!base) {
    logger.error({ symbol, interval }, 'fetchKlines: no base available (getBase returned null)');
    return [];
  }

  const candidates = [
    { path: '/v5/market/kline', params: { category: 'linear', symbol, interval, limit: String(limit) } },
    { path: '/v2/public/kline', params: { symbol, interval, limit: String(limit) } },
    { path: '/v2/public/kline/list', params: { symbol, interval, limit: String(limit) } }
  ];

  for (const c of candidates) {
    try {
      const url = new URL(`${base.replace(/\/$/, '')}${c.path}`);
      Object.entries(c.params || {}).forEach(([k, v]) => {
        if (v !== undefined && v !== null) url.searchParams.append(k, String(v));
      });

      const res = await fetchWithTimeout(url.toString(), { method: 'GET' }, 8000);
      let json = null;
      try { json = await res.json(); } catch (e) { json = null; }

      if (!res.ok) {
        logger.debug({ base, path: c.path, status: res.status, body: json }, 'fetchKlines: HTTP error on base');
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

      logger.debug({ base, path: c.path, body: json }, 'fetchKlines: unexpected shape, trying next candidate endpoint');
    } catch (err) {
      logger.debug({ base, path: c.path, err: err && err.message ? err.message : String(err) }, 'fetchKlines: candidate threw');
    }
  }

  logger.error({ symbol, interval }, 'fetchKlines: all candidates on base failed');
  return [];
}

/**
 * fetchTicker24h(symbol)
 */
async function fetchTicker24h(symbol) {
  const base = getBase();
  if (!base) {
    logger.warn('fetchTicker24h: no base available (getBase returned null)');
    return null;
  }

  const candidates = [
    { path: '/v5/market/tickers', params: { symbol } },
    { path: '/v2/public/tickers', params: { symbol } }
  ];

  for (const c of candidates) {
    try {
      const url = new URL(`${base.replace(/\/$/, '')}${c.path}`);
      Object.entries(c.params || {}).forEach(([k, v]) => {
        if (v !== undefined && v !== null) url.searchParams.append(k, String(v));
      });
      const res = await fetchWithTimeout(url.toString(), { method: 'GET' }, 8000);
      const json = await res.json().catch(() => null);
      if (!res.ok) {
        logger.debug({ base, path: c.path, status: res.status, body: json }, 'fetchTicker24h: HTTP error on base');
        continue;
      }

      if (json && json.result && Array.isArray(json.result.list) && json.result.list.length) {
        const t = json.result.list[0];
        return {
          last_price: t.lastPrice || t.last || t.close || t.price || null,
          last: t.lastPrice || t.last || t.close || t.price || null,
          close: t.close || t.last || t.price || null,
          volume: t.turnover24h || t.volume || t.volume_24_h || null,
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

      logger.debug({ base, path: c.path, body: json }, 'fetchTicker24h: unexpected shape, trying next candidate endpoint');
    } catch (err) {
      logger.debug({ base, path: c.path, err: err && err.message ? err.message : String(err) }, 'fetchTicker24h: candidate threw');
    }
  }
  return null;
}

/**
 * Private / signed endpoints (wallet, orders, positions)
 * - getWalletBalance, fetchOpenOrders, fetchOpenPositions etc.
 * - placeMarketOrderV5, setPositionTradingStop (honor OPENTRADES)
 */

async function getWalletBalance(coin = 'USDT') {
  const apiKey = process.env.BYBIT_API_KEY;
  const apiSecret = process.env.BYBIT_API_SECRET;
  if (!apiKey || !apiSecret) {
    throw new Error('getWalletBalance requires BYBIT_API_KEY and BYBIT_API_SECRET env vars');
  }

  const requestPath = `/v5/account/wallet-balance?coin=${encodeURIComponent(coin)}`;
  const base = getOrderBase();
  logger.debug({ base, coin }, 'getWalletBalance: using order base for wallet balance');
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

  if (!OPENTRADES) {
    logger.info({ op: 'DRY_RUN_ORDER', body: bodyObj }, 'placeMarketOrderV5: OPENTRADES disabled — not sending real order');
    return {
      ret_code: 0,
      ret_msg: 'DRY_RUN: order not placed (OPENTRADES=false)',
      result: { order: bodyObj, simulated: true }
    };
  }

  const bodyStr = JSON.stringify(bodyObj);
  const requestPath = `/v5/order/create`;
  const base = getOrderBase();
  logger.info({ base, symbol, qty, side }, 'placeMarketOrderV5: placing order (real)');
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

  if (!OPENTRADES) {
    logger.info({ op: 'DRY_RUN_TRADING_STOP', body: bodyObj }, 'setPositionTradingStop: OPENTRADES disabled — not sending real request');
    return {
      ret_code: 0,
      ret_msg: 'DRY_RUN: trading stop not applied (OPENTRADES=false)',
      result: { params: bodyObj, simulated: true }
    };
  }

  const bodyStr = JSON.stringify(bodyObj);
  const requestPath = `/v5/position/trading-stop`;
  const base = getOrderBase();
  logger.info({ base, symbol }, 'setPositionTradingStop: sending real trading stop');
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

  const base = getOrderBase();
  logger.debug({ base }, 'fetchOpenOrders: using order base');
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

  const base = getOrderBase();
  logger.debug({ base }, 'fetchOpenPositions: using order base');
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

function getLastProbeInfo() {
  return {
    chosenBase,
    lastProbeHostPath,
    lastProbeResponseText: lastProbeResponseText ? String(lastProbeResponseText).slice(0, 2000) : null,
    OPENTRADES,
    MAINNET
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
  getOrderBase,
  fetchKlines,
  fetchAllSymbols,
  fetchTicker24h,
  getWalletBalance,
  placeMarketOrderV5,
  setPositionTradingStop,
  fetchOpenOrders,
  fetchOpenPositions,
  getSeedSymbols: (symbols) => {
    if (!Array.isArray(symbols)) return [];
    const seedAll = envBool('SYMBOL_SEED_ALL', false);
    if (seedAll) {
      logger.info({ totalSymbols: symbols.length }, 'getSeedSymbols: SYMBOL_SEED_ALL=true, seeding ALL symbols');
      return symbols.slice();
    }
    logger.warn({ totalSymbols: symbols.length }, 'getSeedSymbols: SYMBOL_SEED_ALL=false, no symbols seeded (set SYMBOL_SEED_ALL=true to enable seeding)');
    return [];
  },
  getLastProbeInfo,
  reprobe,
  envBool
};
