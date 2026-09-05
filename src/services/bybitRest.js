// src/services/bybitRest.js
/* full file (updated): same structure as before with a more permissive default symbol filter
   that accepts USDT or USDT.P but rejects dated/expiry symbols containing '-' */
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
 * fetchAllSymbols() - CURSOR PAGINATION
 * Default filter: accept USDT or USDT.P perpetuals, but reject dated/expiry symbols (contain '-')
 * Use SYMBOL_FILTER_REGEX env to override behavior.
 */
async function fetchAllSymbols() {
  logger.info('bybitRest.fetchAllSymbols: starting cursor-based pagination for all symbols');

  // Default: accept USDT or USDT.P, case-insensitive. Override with SYMBOL_FILTER_REGEX env var if needed.
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
              // Exclude dated/expiry style symbols which include a hyphen (e.g., "XRPUSDT-25SEP26")
              if (/-/.test(sym)) return false;

              const quote = String(it.quoteCoin || it.quote || '').toUpperCase();

              // Accept if matches configured regex
              if (DEFAULT_SYMBOL_FILTER.test(sym)) return true;

              // Some responses provide explicit quote coin; accept USDT (but still exclude hyphen)
              if (quote === 'USDT') {
                // ensure symbol ends with USDT or USDT.P
                const su = sym.toUpperCase();
                if (su.endsWith('USDT') || su.endsWith('USDT.P')) return true;
              }
            } catch (e) {
              return false;
            }
            return false;
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
    // fallback to CoinGecko if useful
    return [];
  }
}

/**
 * getSeedSymbols(symbols)
 */
function getSeedSymbols(symbols) {
  if (!Array.isArray(symbols)) return [];

  const seedAll = envBool('SYMBOL_SEED_ALL', false);

  if (seedAll) {
    logger.info({ totalSymbols: symbols.length }, 'getSeedSymbols: SYMBOL_SEED_ALL=true, seeding ALL symbols');
    return symbols.slice();
  }

  logger.warn({ totalSymbols: symbols.length }, 'getSeedSymbols: SYMBOL_SEED_ALL=false, no symbols seeded (set SYMBOL_SEED_ALL=true to enable seeding)');
  return [];
}

/**
 * fetchKlines, fetchTicker24h, signed endpoints...
 * (unchanged from your prior implementation; omitted here for brevity — copy your existing functions)
 *
 * Note: in your repo keep the rest of the functions (fetchKlines, fetchTicker24h,
 * placeMarketOrderV5, getWalletBalance, etc.) exactly as before.
 */

// For brevity in this message I won't duplicate the entire rest of the file; keep all previous functions unchanged
// (fetchKlines, fetchTicker24h, getWalletBalance, placeMarketOrderV5, setPositionTradingStop, fetchOpenOrders, fetchOpenPositions)
// and exports as in your current file. The only change needed is the fetchAllSymbols filtering logic above.

module.exports = {
  probeHosts,
  getBase,
  getOrderBase,
  fetchAllSymbols,
  // keep the rest of your exports as before:
  fetchKlines: require('./bybitRest').fetchKlines, // if copying into your file, keep original function bodies instead
  fetchTicker24h: require('./bybitRest').fetchTicker24h,
  getSeedSymbols,
  getWalletBalance: require('./bybitRest').getWalletBalance,
  placeMarketOrderV5: require('./bybitRest').placeMarketOrderV5,
  setPositionTradingStop: require('./bybitRest').setPositionTradingStop,
  fetchOpenOrders: require('./bybitRest').fetchOpenOrders,
  fetchOpenPositions: require('./bybitRest').fetchOpenPositions,
  getLastProbeInfo,
  reprobe,
  envBool
};
