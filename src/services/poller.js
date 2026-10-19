// poller.js
const { performance } = require('perf_hooks');
const dbModule = require('../db');
const bybit = require('./bybitRest');
const bybitWs = require('./bybitWs');
const config = require('../config');
const logger = require('pino')();
const Bottleneck = require('bottleneck');
const macdUtil = require('./macd');
const signalManager = require('./signalManager');
const notificationQueue = require('./notificationQueue');
const marketData = require('./marketData'); // NEW: used by composite gate

const limiter = new Bottleneck({
  minTime: 50
});

const SEED_CONCURRENCY = Number(
  config.SEED_CONCURRENCY || 6
);

const FIVE_MINUTES_MS = 5 * 60 * 1000;

let isRunning = false;
let startupComplete = false;
let boundaryScanInProgress = false;

// Global de-dupe registry used across root-TF and MTF alignment flows
// Key format: symbol|root_tf|candle_open_time|signal_family
const processedEventKeys = new Map();

// Tracks startup-batch signals (REDUCED TTL: 5 minutes instead of 12 hours)
// Map<signalKey, { firedAt }>
const startupBatchSignalKeys = new Map();

// Cooldown cache for REST fallbacks during WS gaps
const restFallbackCooldowns = new Map();

// Track root TF boundary state - Key: poller.loop2.processedCandle.${symbol}.${tf}
// UNIFIED: Only one key format for all state tracking
const rootTfBoundaryStates = new Map();

// Track MTF alignment state for each symbol/TF/candleOpenTime to prevent duplicate alerts
// Key: ${symbol}|${tf}\vert{}${candleOpenTime}, Value: { lastAlignmentState, lastFlipTime, candleOpenTime }
const mtfAlignmentStates = new Map();

// In-memory fallback state cache when DB is temporarily unavailable
// Key: same as DB state keys
// Value: { value, cachedAt }
const inMemoryStateCache = new Map();

// MACD flip dedupe tracker
// Key: ${symbol}|${tf}\vert{}${candleOpenTime}
const macdFlipTracker = new Map();

// DB connectivity tracker for health monitoring
let lastDbConnectivityCheck = Date.now();
let dbIsHealthy = true;

// NEW: 1D root signal count tracking
const consecutive1dTradeCounts = new Map(); // symbol -> { count, lastAcceptedAt }

// NEW: keepalive ping handle
let keepaliveIntervalHandle = null;

// ============================================================================
// DATABASE SAFETY GUARDS & RECOVERY
// ============================================================================

function isDbReady() {
  if (!dbModule || typeof dbModule.get !== 'function') {
    return false;
  }

  try {
    const db = dbModule.get();
    if (!db || typeof db.prepare !== 'function') {
      return false;
    }
    return true;
  } catch (err) {
    logger.debug({ err }, 'poller: db readiness check failed');
    return false;
  }
}

function safeGetDb() {
  if (!isDbReady()) {
    logger.debug('poller: db is not ready');
    return null;
  }

  try {
    const db = dbModule.get();
    if (!db) {
      logger.debug('poller: dbModule.get() returned null');
      return null;
    }
    return db;
  } catch (err) {
    logger.debug({ err }, 'poller: failed to acquire database connection');
    return null;
  }
}

/**
 * ENHANCED: Attempt to reconnect to DB with exponential backoff
 * Waits for DB to become available before proceeding
 */
async function ensureDbReady(maxRetries = 3, backoffMs = 500) {
  let db = safeGetDb();

  if (db) {
    dbIsHealthy = true;
    return db;
  }

  logger.info(
    { maxRetries, initialBackoffMs: backoffMs },
    'poller: database not ready; initiating reconnection sequence'
  );

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    const waitMs = backoffMs * attempt;

    logger.warn(
      { attempt, maxRetries, waitMs },
      'poller: waiting before DB reconnection attempt'
    );

    await sleep(waitMs);

    db = safeGetDb();

    if (db) {
      logger.info(
        { attempt },
        'poller: database reconnected successfully'
      );
      dbIsHealthy = true;
      return db;
    }

    if (attempt < maxRetries) {
      logger.warn(
        { attempt, nextAttempt: attempt + 1 },
        'poller: DB reconnection attempt failed; will retry'
      );
    }
  }

  logger.error(
    { maxRetries },
    'poller: database connection failed after all reconnection attempts'
  );
  dbIsHealthy = false;
  return null;
}

/**
 * CRITICAL FIX: Enhanced state getter with in-memory fallback, age validation, and retry logic
 */
function safeGetState(key, maxAgeMs = 5 * 60 * 1000) {
  if (!key) {
    logger.debug('poller: getState called with empty key');
    return null;
  }

  if (dbModule && typeof dbModule.getState === 'function') {
    try {
      const value = dbModule.getState(key);
      if (value !== undefined && value !== null) {
        inMemoryStateCache.set(key, {
          value,
          cachedAt: Date.now()
        });
        return value;
      }
    } catch (err) {
      logger.debug({ err, key }, 'poller: getState DB attempt failed; checking in-memory cache');
    }
  }

  const cached = inMemoryStateCache.get(key);
  if (cached && cached.value !== undefined && cached.value !== null) {
    const age = Date.now() - cached.cachedAt;

    if (age > maxAgeMs) {
      logger.warn(
        { key, ageMs: age, maxAgeMs },
        'poller: in-memory cache entry too stale; discarding and returning null'
      );
      inMemoryStateCache.delete(key);
      return null;
    }

    logger.debug({ key, ageMs: age }, 'poller: retrieved state from in-memory cache');
    return cached.value;
  }

  return null;
}

/**
 * CRITICAL FIX: Enhanced state setter with in-memory cache + improved retry logic
 */
function safeSetState(key, value) {
  if (!key || value === undefined) {
    logger.debug({ key, value }, 'poller: setState called with invalid parameters');
    return false;
  }

  let dbSuccess = false;

  if (dbModule && typeof dbModule.setState === 'function') {
    let retries = 3;
    let retryDelayMs = 50;

    while (retries > 0) {
      try {
        dbModule.setState(key, value);
        dbSuccess = true;
        logger.debug({ key, value }, 'poller: setState persisted to database');
        break;
      } catch (err) {
        retries--;
        if (retries > 0) {
          logger.debug(
            { err, key, retryCount: 3 - retries, nextDelayMs: retryDelayMs * 2 },
            'poller: setState DB attempt failed; retrying with backoff'
          );
          const start = Date.now();
          while (Date.now() - start < retryDelayMs) {}
          retryDelayMs *= 2;
        } else {
          logger.warn(
            { err, key, value },
            'poller: setState DB failed after retries; relying on in-memory cache only'
          );
        }
      }
    }
  }

  inMemoryStateCache.set(key, {
    value,
    cachedAt: Date.now()
  });

  return dbSuccess || true;
}

function safeDeleteState(key) {
  if (!dbModule || typeof dbModule.deleteState !== 'function') {
    logger.debug('poller: dbModule.deleteState not available');
    inMemoryStateCache.delete(key);
    return false;
  }

  try {
    dbModule.deleteState(key);
    inMemoryStateCache.delete(key);
    return true;
  } catch (err) {
    logger.debug({ err, key }, 'poller: deleteState failed');
    inMemoryStateCache.delete(key);
    return false;
  }
}

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms || 0);
  });
}

function normalizeRootTf(tf) {
  if (tf === null || tf === undefined) {
    return null;
  }

  const value = String(tf)
    .trim()
    .toUpperCase();

  if (value === '1D' || value === 'D') {
    return 'D';
  }

  if (value === '1H' || value === 'H') {
    return '60';
  }

  if (value === '4H') {
    return '240';
  }

  return value;
}

function buildRootTfs() {
  const raw = Array.isArray(config.ROOT_TFS)
    ? config.ROOT_TFS
    : ['60', '240', 'D'];

  const seen = new Set();
  const output = [];

  for (const timeframe of raw) {
    const normalized = normalizeRootTf(timeframe);

    if (!normalized || seen.has(normalized)) {
      continue;
    }

    seen.add(normalized);
    output.push(normalized);
  }

  return output.length > 0
    ? output
    : ['60', '240', 'D'];
}

// NEW: root TF trade order priority
function buildRootTfTradePriorityOrder() {
  const raw = Array.isArray(config.ROOT_TF_TRADE_PRIORITY)
    ? config.ROOT_TF_TRADE_PRIORITY
    : ['240', 'D', '60'];

  const seen = new Set();
  const out = [];

  for (const tf of raw) {
    const normalized = normalizeRootTf(tf);
    if (!normalized || seen.has(normalized)) {
      continue;
    }
    seen.add(normalized);
    out.push(normalized);
  }

  // ensure all root TFs exist
  for (const tf of buildRootTfs()) {
    if (!seen.has(tf)) {
      out.push(tf);
    }
  }

  return out;
}

function sortByRootTfPriority(signals) {
  const priority = buildRootTfTradePriorityOrder();

  return [...signals].sort((a, b) => {
    const aTf = normalizeRootTf(a?.root_tf);
    const bTf = normalizeRootTf(b?.root_tf);

    const aIndex = priority.indexOf(aTf);
    const bIndex = priority.indexOf(bTf);

    const ai = aIndex >= 0 ? aIndex : Number.MAX_SAFE_INTEGER;
    const bi = bIndex >= 0 ? bIndex : Number.MAX_SAFE_INTEGER;

    if (ai !== bi) return ai - bi;
    return String(a?.symbol || '').localeCompare(String(b?.symbol || ''));
  });
}

function isUsdtSymbol(symbol) {
  const value = String(symbol || '')
    .toUpperCase();

  if (!value) {
    return false;
  }

  if (/USDT[QHUZ0-9]/.test(value.slice(-6))) {
    return false;
  }

  return /USDT(\.P)?$/.test(value);
}

function getLatestOpenTime(db, symbol, timeframe) {
  if (!db) {
    return null;
  }

  try {
    const row = db
      .prepare(
        `
          SELECT open_time
          FROM klines
          WHERE symbol = ?
            AND timeframe = ?
          ORDER BY open_time DESC
          LIMIT 1
        `
      )
      .get(symbol, timeframe);

    if (!row) {
      return null;
    }

    const openTime = Number(row.open_time);

    return Number.isFinite(openTime)
      ? openTime
      : null;
  } catch (err) {
    logger.debug({ err, symbol, timeframe }, 'poller: getLatestOpenTime query failed');
    return null;
  }
}

function getRootTfMs(tf) {
  const normalized = normalizeRootTf(tf);

  if (normalized === 'D') {
    return 24 * 60 * 60 * 1000;
  }

  const minutes = Number(normalized);

  if (Number.isFinite(minutes) && minutes > 0) {
    return minutes * 60 * 1000;
  }

  return FIVE_MINUTES_MS;
}

function getNextTimeframeBoundaryMs(tf, nowMs = Date.now()) {
  const tfMs = getRootTfMs(tf);

  const epochStart = Date.UTC(1970, 0, 1);
  const alignedFloor = Math.floor(
    (nowMs - epochStart) / tfMs
  ) * tfMs;

  return alignedFloor + tfMs + epochStart;
}

function buildEventId(type, symbol, timeframe, candleOpenTime) {
  return [
    type,
    String(symbol || ''),
    String(timeframe || ''),
    Number(candleOpenTime) || 0
  ].join(':');
}

function startupBatchKeyFromSignal(signal) {
  if (!signal || !signal.symbol || !signal.root_tf) {
    return null;
  }

  const symbol = String(signal.symbol).toUpperCase();
  const rootTf = normalizeRootTf(signal.root_tf);
  const candleOpenTime = Number(
    signal.candle_open_time ??
    signal.candleOpenTime ??
    signal.detected_at ??
    0
  );

  const effectiveCandle = Number.isFinite(candleOpenTime)
    ? candleOpenTime
    : 0;

  return `${symbol}|${rootTf}|${effectiveCandle}`;
}

function dedupKeyFromSignal(signal, family = 'generic') {
  if (!signal || !signal.symbol || !signal.root_tf) {
    return null;
  }

  const symbol = String(signal.symbol).toUpperCase();
  const rootTf = normalizeRootTf(signal.root_tf);
  const candleOpenTime = Number(
    signal.candle_open_time ??
    signal.candleOpenTime ??
    signal.detected_at ??
    0
  );

  const effectiveCandle = Number.isFinite(candleOpenTime)
    ? candleOpenTime
    : 0;

  return `${symbol}|${rootTf}|${effectiveCandle}|${String(family).toLowerCase()}`;
}

function alreadyProcessedSignal(signal, family = 'generic') {
  if (isStartupBatchSignal(signal, family)) {
    return true;
  }

  const key = dedupKeyFromSignal(signal, family);
  if (!key) {
    return true;
  }

  if (processedEventKeys.has(key)) {
    return true;
  }

  processedEventKeys.set(key, Date.now());
  return false;
}

function trackRootTfBoundaryState(symbol, tf, candleOpenTime) {
  const stateKey = `poller.loop2.processedCandle.${symbol}.${tf}`;
  const prevState = rootTfBoundaryStates.get(stateKey);

  if (prevState && prevState.candleOpenTime !== candleOpenTime) {
    const keysToDelete = [];
    for (const [key] of processedEventKeys.entries()) {
      if (key.includes(`${symbol}|${tf}|${prevState.candleOpenTime}`)) {
        keysToDelete.push(key);
      }
    }
    for (const key of keysToDelete) {
      processedEventKeys.delete(key);
    }

    logger.debug(
      {
        symbol,
        tf,
        oldCandleOpenTime: prevState.candleOpenTime,
        newCandleOpenTime: candleOpenTime,
        purgedKeys: keysToDelete.length
      },
      'poller: invalidated cache for root TF boundary transition'
    );
  }

  rootTfBoundaryStates.set(stateKey, {
    candleOpenTime,
    lastProcessedTime: Date.now()
  });
}

function cleanupExpiredMtfAlignmentStates(ttlMs = 24 * 60 * 60 * 1000) {
  const now = Date.now();
  const keysToDelete = [];

  for (const [key, value] of mtfAlignmentStates.entries()) {
    if (now - value.lastFlipTime > ttlMs) {
      keysToDelete.push(key);
    }
  }

  for (const key of keysToDelete) {
    mtfAlignmentStates.delete(key);
  }

  return keysToDelete.length;
}

function cleanupExpiredRootTfBoundaryStates(ttlMs = 7 * 24 * 60 * 60 * 1000) {
  const now = Date.now();
  const keysToDelete = [];

  for (const [key, value] of rootTfBoundaryStates.entries()) {
    if (now - value.lastProcessedTime > ttlMs) {
      keysToDelete.push(key);
    }
  }

  for (const key of keysToDelete) {
    rootTfBoundaryStates.delete(key);
  }

  return keysToDelete.length;
}

/**
 * FIXED: Reduced TTL from 12 hours to 5 minutes
 * Allows legitimate root candle signals to pass through after startup window
 */
function cleanupExpiredStartupBatchSignals(ttlMs = 5 * 60 * 1000) {
  const now = Date.now();
  const keysToDelete = [];

  for (const [key, info] of startupBatchSignalKeys.entries()) {
    if (now - info.firedAt > ttlMs) {
      keysToDelete.push(key);
    }
  }

  for (const key of keysToDelete) {
    startupBatchSignalKeys.delete(key);
  }

  return keysToDelete.length;
}

function cleanupRestFallbackCooldowns(maxAgeMs = 10 * 60 * 1000) {
  const now = Date.now();
  const keysToDelete = [];

  for (const [key, ts] of restFallbackCooldowns.entries()) {
    if (now - ts > maxAgeMs) {
      keysToDelete.push(key);
    }
  }

  for (const key of keysToDelete) {
    restFallbackCooldowns.delete(key);
  }

  return keysToDelete.length;
}

function cleanupInMemoryStateCache(ttlMs = 2 * 60 * 60 * 1000) {
  const now = Date.now();
  const keysToDelete = [];

  for (const [key, value] of inMemoryStateCache.entries()) {
