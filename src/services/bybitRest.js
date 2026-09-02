/**
 * src/services/bybitRest.js
 *
 * - MAINNET / OPENTRADES aware order routing
 * - getOrderBase() chooses BYBIT_ORDER_BASE -> MAINNET -> fallback
 * - OPENTRADES=false causes order functions to dry-run (no real order HTTP calls)
 * - probeHosts is stricter: if BYBIT_REST_BASE is configured, only probe that host and don't persist
 *   a different host; require JSON/API-shaped responses before accepting a host (avoids HTML pages).
 * - fetchAllSymbols: request v5 instrument endpoints with category=linear, treat API retCode/ret_code != 0 as failures,
 *   and use more robust symbol filtering (by symbol suffix OR quote field). Now accepts USDT and USDT.P suffixes,
 *   merges unique symbols across endpoints, sorts A->Z, and provides a seeding helper to choose top-N or all.
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

/* Env boolean helper */
function envBool(name, defaultVal = false) {
  if (typeof process.env[name] === 'undefined') return defaultVal;
  const v = String(process.env[name]).toLowerCase().trim();
  return v === '1' || v === 'true' || v === 'yes';
}

const OPENTRADES = envBool('OPENTRADES', false);
const MAINNET = envBool('MAINNET', true);

/* Configured base helper */
function getConfiguredBase() {
  const envBase = process.env.BYBIT_REST_BASE || (config && config.BYBIT_REST_BASE);
  if (!envBase) return null;
  return String(envBase).replace(/\/$/, '');
}

/**
 * Load chosen base from disk but do not let it override an explicit BYBIT_REST_BASE or MAINNET env var.
 */
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
 * getOrderBase()
 * Prefer BYBIT_ORDER_BASE; if MAINNET env is present, obey it (MAINNET=true -> mainnet, false -> testnet).
 * Otherwise fallback to getBase().
 */
function getOrderBase() {
  const explicit = process.env.BYBIT_ORDER_BASE;
  if (explicit) return String(explicit).replace(/\/$/, '');

  // If MAINNET env is explicitly set, obey it
  if (typeof process.env.MAINNET !== 'undefined') {
    return MAINNET ? 'https://api.bybit.com' : 'https://api-testnet.bybit.com';
  }

  // Backwards compatibility with BYBIT_USE_TESTNET
  const useTestnetRaw = typeof process.env.BYBIT_USE_TESTNET !== 'undefined' ? String(process.env.BYBIT_USE_TESTNET) : null;
  if (useTestnetRaw !== null) {
    const val = useTestnetRaw.toLowerCase();
    if (val === '1' || val === 'true' || val === 'yes') return 'https://api-testnet.bybit.com';
    if (val === '0' || val === 'false' || val === 'no') return 'https://api.bybit.com';
    return 'https://api-testnet.bybit.com';
  }

  return getBase();
}

/**
 * probeHosts(timeoutMs)
 * - If BYBIT_REST_BASE is set, only probe that host and DO NOT persist a different host.
 * - Accept a host only if response looks like JSON or API-shaped JSON.
 * - Require retCode===0 when retCode present.
 */
async function probeHosts(timeoutMs = 5000) {
  const configured = getConfiguredBase();
  const list = configured ? [configured] : HOST_CANDIDATES.slice();

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
        // Basic content-type guard:
        const ct = (res.headers && typeof res.headers.get === 'function') ? (res.headers.get('content-type') || '') : '';
        const looksLikeJson = /application\/json/i.test(ct) || (text && text.trim().startsWith('{'));

        // Try to detect API-shaped JSON: has `result` or `ret_code`/`retCode` fields commonly in Bybit responses
        const parsedOkApi = parsed && (parsed.result || typeof parsed.ret_code !== 'undefined' || typeof parsed.retCode !== 'undefined');

        if (!looksLikeJson && !parsedOkApi) {
          logger.debug({ host, path: testUrl, contentType: ct, snippet: text ? text.slice(0, 200) : null }, 'probeHosts: host returned OK but not JSON/API-shaped; skipping');
          continue;
        }

        // If API returned retCode/ret_code, ensure it's zero before accepting
        if (parsed && (typeof parsed.retCode !== 'undefined' || typeof parsed.ret_code !== 'undefined')) {
          const rc = typeof parsed.retCode !== 'undefined' ? parsed.retCode : parsed.ret_code;
          if (rc !== 0) {
            logger.debug({ host, path: testUrl, retCode: rc, snippet: text ? text.slice(0, 200) : null }, 'probeHosts: host returned API error retCode; skipping');
            continue;
          }
        }

        lastProbeHostPath = { host, path: testUrl };
        lastProbeResponseText = text;

        // If user explicitly configured BYBIT_REST_BASE, respect that and DO NOT overwrite it.
        if (configured) {
          logger.info({ configured }, 'probeHosts: configured BYBIT_REST_BASE responded OK; not persisting probe selection');
          return configured;
        }

        // Otherwise accept and persist the host we probed
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

/** Helper: fetch symbols from CoinGecko markets as a fallback */
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
 * Behavior: use only the single base from getBase() for attempts.
 * Last-resort fallback to CoinGecko.
 */
async function fetchAllSymbols() {
  logger.info('bybitRest.fetchAllSymbols: start');

  // Accept USDT or USDT.P suffix (case-insensitive). Also allow override via SYMBOL_FILTER_REGEX env.
  const DEFAULT_SYMBOL_FILTER = process.env.SYMBOL_FILTER_REGEX ? new RegExp(process.env.SYMBOL_FILTER_REGEX) : /usdt(\.p)?$/i;

  // TARGET_SYMBOLS: if initial harvest < target, attempt extended harvest
  const TARGET_SYMBOLS = process.env.SYMBOL_TARGET ? Number(process.env.SYMBOL_TARGET) : 700;
  const base = getBase();
  if (!base) {
    logger.warn('fetchAllSymbols: no base available (getBase returned null)');
    return [];
  }

  // Conservative candidate endpoints (first pass)
  const candidates = [
    { path: '/v5/market/instruments', params: { category: 'linear' } },
    { path: '/v5/market/instruments-info', params: { category: 'linear' } },
    { path: '/v2/public/symbols', params: {} },
    { path: '/v2/public/tickers', params: {} }
  ];

  // Extended candidate endpoints (used when initial harvest is below TARGET_SYMBOLS)
  const extendedCandidates = [
    { path: '/v5/market/instruments', params: {} },            // broader - no category filter
    { path: '/v5/market/instruments-info', params: {} },       // broader
    { path: '/v5/market/tickers', params: {} },
    { path: '/v2/public/symbols', params: {} },
    { path: '/v2/public/tickers', params: {} }
  ];

  let lastOkResponseText = null;
  let lastOkHostPath = null;

  async function attemptHostForSymbols(host, candidateList, timeoutMs = 8000) {
    for (const c of candidateList) {
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

        // If API uses retCode/ret_code semantics, treat non-zero as failure and skip this endpoint.
        if (json && (typeof json.retCode !== 'undefined' || typeof json.ret_code !== 'undefined')) {
          const rc = (typeof json.retCode !== 'undefined') ? json.retCode : json.ret_code;
          const rm = json.retMsg || json.ret_msg || null;
          if (rc !== 0) {
            logger.debug({ host, path: c.path, retCode: rc, retMsg: rm, snippet: bodyText ? bodyText.slice(0,200) : null }, 'attemptHostForSymbols: API returned non-zero retCode; skipping endpoint');
            continue;
          }
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
          // More robust filtering: accept if symbol matches regex OR quote === 'USDT' OR symbol endsWith 'USDT' / 'USDT.P'
          const filtered = symbols.filter(s => {
            if (!s || !s.symbol) return false;
            try {
              const sym = String(s.symbol);
              const quote = String(s.quote || '').toUpperCase();
              // regex match (handles USDT and USDT.P)
              if (DEFAULT_SYMBOL_FILTER.test(sym)) return true;
              // explicit quote field equals USDT
              if (quote === 'USDT') return true;
              // symbol string ends with USDT or USDT.P
              const su = sym.toUpperCase();
              if (su.endsWith('USDT') || su.endsWith('USDT.P')) return true;
            } catch (e) {
              return false;
            }
            return false;
          });

          logger.info({ host, path: c.path, fetched: symbols.length, filtered: filtered.length }, 'attemptHostForSymbols: fetched symbols from host endpoint');

          // Return both filtered list and raw count so caller can decide whether to expand
          return { symbols: filtered, rawCount: symbols.length };
        }
      } catch (err) {
        logger.debug({ host, path: c.path, err: err && err.message ? err.message : String(err) }, 'attemptHostForSymbols: candidate threw');
      }
    }
    return { symbols: null };
  }

  function mergeUniqueSymbols(arrays) {
    const map = new Map();
    for (const arr of arrays) {
      if (!Array.isArray(arr)) continue;
      for (const s of arr) {
        if (!s || !s.symbol) continue;
        const key = String(s.symbol).toUpperCase();
        if (!map.has(key)) {
          map.set(key, {
            symbol: String(s.symbol),
            base: s.base || null,
            quote: s.quote || null,
            status: s.status || null
          });
        }
      }
    }
    // Return alphabetically sorted A->Z (case-insensitive)
    const out = Array.from(map.values()).sort((a, b) => {
      const A = String(a.symbol || '').toUpperCase();
      const B = String(b.symbol || '').toUpperCase();
      if (A < B) return -1;
      if (A > B) return 1;
      return 0;
    });
    return out;
  }

  try {
    // First, try the standard candidate list
    const firstAttempt = await attemptHostForSymbols(base, candidates, 8000);
    if (Array.isArray(firstAttempt.symbols) && firstAttempt.symbols.length) {
      lastProbeHostPath = lastProbeHostPath || lastOkHostPath;
      lastProbeResponseText = lastProbeResponseText || lastOkResponseText;

      // If we already have enough symbols, return them sorted
      if (firstAttempt.symbols.length >= TARGET_SYMBOLS) {
        const merged = mergeUniqueSymbols([firstAttempt.symbols]);
        logger.info({ base, count: merged.length }, 'fetchAllSymbols: base returned sufficient symbols (initial pass)');
        return merged;
      }

      // Otherwise, attempt extended harvest on the same base to gather more unique symbols
      logger.info({ base, initialCount: firstAttempt.symbols.length, target: TARGET_SYMBOLS }, 'fetchAllSymbols: initial harvest below target, attempting extended harvest on same base');
      const secondAttempt = await attemptHostForSymbols(base, extendedCandidates, 10000);

      const combined = mergeUniqueSymbols([firstAttempt.symbols, secondAttempt.symbols || []]);
      logger.info({ base, combinedCount: combined.length, initial: firstAttempt.symbols.length, second: (secondAttempt.symbols ? secondAttempt.symbols.length : 0) }, 'fetchAllSymbols: extended harvest complete, merged unique symbols');

      if (combined.length) {
        logger.info({ sampleFirst: combined.slice(0, 5).map(s => s.symbol), sampleLast: combined.slice(-5).map(s => s.symbol) }, 'fetchAllSymbols: returning combined symbol list (sorted A->Z)');
        return combined;
      } else {
        // If merge failed but initial had something, return initial (sorted & unique)
        const onlyInitial = mergeUniqueSymbols([firstAttempt.symbols]);
        logger.warn({ base, initialCount: firstAttempt.symbols.length }, 'fetchAllSymbols: extended harvest yielded no new symbols; returning initial filtered set');
        return onlyInitial;
      }
    } else if (Array.isArray(firstAttempt.symbols) && firstAttempt.symbols.length === 0) {
      logger.warn({ base, rawCount: firstAttempt.rawCount }, 'fetchAllSymbols: base returned symbols but none matched filter; returning empty (no fallback hosts used)');
    } else {
      logger.warn({ base }, 'fetchAllSymbols: base attempt failed to produce symbols; returning empty (no fallback hosts used)');
    }
  } catch (e) {
    logger.debug({ e }, 'fetchAllSymbols: base exclusive attempt threw, returning empty (no fallback hosts used)');
  }

  if (lastOkHostPath) {
    logger.warn({ lastOkHostPath, snippet: lastOkResponseText ? lastOkResponseText.slice(0, 800) : null }, 'fetchAllSymbols: base failed but there was an OK response snippet (included)');
  } else {
    logger.warn('fetchAllSymbols: base returned no HTTP OK during attempts');
  }

  // Last-resort CoinGecko fallback (filtered)
  try {
    const cgSymbols = await fetchSymbolsFromCoinGecko(500);
    if (cgSymbols && cgSymbols.length) {
      const filtered = cgSymbols.filter(s => s.symbol && DEFAULT_SYMBOL_FILTER.test(String(s.symbol)));
      const uniqueSorted = mergeUniqueSymbols([filtered]);
      logger.info({ count: uniqueSorted.length }, 'fetchAllSymbols: fallback to CoinGecko (filtered) succeeded');
      return uniqueSorted;
    }
    logger.warn('fetchAllSymbols: CoinGecko fallback returned no symbols');
  } catch (e) {
    logger.debug({ e }, 'fetchAllSymbols: CoinGecko fallback threw');
  }

  return [];
}

/**
 * Helper to derive seed list (top N vs all) for initial kline seeding / subscriptions.
 * - If SYMBOL_SEED_ALL=true, return the full symbol list (sorted A->Z).
 * - Otherwise return the first SYMBOL_SEED_TOPN symbols (default 100).
 *
 * Other modules should call getSeedSymbols(fetchAllSymbolsResult) when deciding
 * which symbols to seed initially.
 */
function getSeedSymbols(symbols) {
  if (!Array.isArray(symbols)) return [];
  const seedAll = envBool('SYMBOL_SEED_ALL', false);
  const topN = process.env.SYMBOL_SEED_TOPN ? Number(process.env.SYMBOL_SEED_TOPN) : 100;
  if (seedAll) {
    logger.info({ totalSymbols: symbols.length }, 'getSeedSymbols: SYMBOL_SEED_ALL enabled — seeding all symbols');
    return symbols.slice(); // copy
  }
  logger.info({ totalSymbols: symbols.length, seedTopN: topN }, 'getSeedSymbols: returning top N symbols for seeding');
  return symbols.slice(0, topN);
}

/**
 * fetchKlines(symbol, interval, limit)
 * Uses single base (getBase()) only.
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
 * Uses single base (getBase()) only.
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
 * Signed/private endpoints:
 * - getWalletBalance, fetchOpenOrders, fetchOpenPositions: read-only but use getOrderBase()
 * - placeMarketOrderV5, setPositionTradingStop: order-mutating endpoints; if OPENTRADES=false they simulate/dry-run
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

  // Dry-run when OPENTRADES is disabled
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

  // Dry-run when OPENTRADES is disabled
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

/** Diagnostics helpers */
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
  // new helper for seeding
  getSeedSymbols,
  // diagnostics
  getLastProbeInfo,
  reprobe
};
