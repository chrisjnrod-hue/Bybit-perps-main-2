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

// Track MTF alignment state for each symbol/TF to prevent duplicate alerts
// Key: ${symbol}\vert{}${tf}, Value: { lastAlignmentState, lastFlipTime }
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
    if (value && typeof value === 'object' && value.cachedAt) {
      if (now - value.cachedAt > ttlMs) {
        keysToDelete.push(key);
      }
    }
  }

  for (const key of keysToDelete) {
    inMemoryStateCache.delete(key);
  }

  return keysToDelete.length;
}

function cleanupMacdFlipTracker(ttlMs = 60 * 60 * 1000) {
  const now = Date.now();
  const keysToDelete = [];

  for (const [key, value] of macdFlipTracker.entries()) {
    if (value && value.firedAt && now - value.firedAt > ttlMs) {
      keysToDelete.push(key);
    }
  }

  for (const key of keysToDelete) {
    macdFlipTracker.delete(key);
  }

  return keysToDelete.length;
}

function clearExpiredProcessedSignalKeys(ttlMs = 60 * 60 * 1000) {
  const now = Date.now();

  for (const [key, ts] of processedEventKeys.entries()) {
    if (now - ts > ttlMs) {
      processedEventKeys.delete(key);
    }
  }

  const mtfPurged = cleanupExpiredMtfAlignmentStates(24 * 60 * 60 * 1000);
  const rootTfPurged = cleanupExpiredRootTfBoundaryStates(7 * 24 * 60 * 60 * 1000);
  const startupPurged = cleanupExpiredStartupBatchSignals(5 * 60 * 1000);
  const cooldownPurged = cleanupRestFallbackCooldowns(10 * 60 * 1000);
  const stateCachePurged = cleanupInMemoryStateCache(2 * 60 * 60 * 1000);
  const macdPurged = cleanupMacdFlipTracker(60 * 60 * 1000);

  if (
    mtfPurged > 0 ||
    rootTfPurged > 0 ||
    startupPurged > 0 ||
    cooldownPurged > 0 ||
    stateCachePurged > 0 ||
    macdPurged > 0
  ) {
    logger.debug(
      {
        processedEventKeysSize: processedEventKeys.size,
        mtfAlignmentStatesSize: mtfAlignmentStates.size,
        rootTfBoundaryStatesSize: rootTfBoundaryStates.size,
        startupBatchSignalKeysSize: startupBatchSignalKeys.size,
        restFallbackCooldownsSize: restFallbackCooldowns.size,
        inMemoryStateCacheSize: inMemoryStateCache.size,
        macdFlipTrackerSize: macdFlipTracker.size,
        mtfPurged,
        rootTfPurged,
        startupPurged,
        cooldownPurged,
        stateCachePurged,
        macdPurged
      },
      'poller: cache cleanup cycle completed'
    );
  }
}

function registerStartupBatchSignal(signal, family = 'generic') {
  const baseKey = startupBatchKeyFromSignal(signal);
  if (baseKey) {
    startupBatchSignalKeys.set(baseKey, { firedAt: Date.now() });
  }

  const key = dedupKeyFromSignal(signal, family);
  if (key) {
    startupBatchSignalKeys.set(key, { firedAt: Date.now() });
    processedEventKeys.set(key, Date.now());
  }

  const symbol = String(signal?.symbol || '').toUpperCase();
  const rootTf = normalizeRootTf(signal?.root_tf);
  const candleOpenTime = Number(
    signal?.candle_open_time ??
    signal?.candleOpenTime ??
    signal?.detected_at ??
    0
  );
  const effectiveCandle = Number.isFinite(candleOpenTime) ? candleOpenTime : 0;

  if (symbol && rootTf) {
    const families = [
      'generic',
      'rootcandle',
      'alignment',
      'ws_rootcandle',
      'rootcandle_update',
      'mtf_alignment'
    ];
    for (const fam of families) {
      startupBatchSignalKeys.set(
        `${symbol}|${rootTf}|${effectiveCandle}|${fam}`,
        { firedAt: Date.now() }
      );
      processedEventKeys.set(`${symbol}|${rootTf}|${effectiveCandle}|${fam}`, Date.now());
    }
  }
}

function isStartupBatchSignal(signal, family = 'generic') {
  const baseKey = startupBatchKeyFromSignal(signal);
  if (baseKey && startupBatchSignalKeys.has(baseKey)) {
    return true;
  }

  const key = dedupKeyFromSignal(signal, family);
  if (key && startupBatchSignalKeys.has(key)) {
    return true;
  }

  return false;
}

// NEW: consecutive 1D count utilities
function resetStaleConsecutive1dCounts(maxAgeMs = 48 * 60 * 60 * 1000) {
  const now = Date.now();

  for (const [symbol, info] of consecutive1dTradeCounts.entries()) {
    if (now - (info.lastAcceptedAt || 0) > maxAgeMs) {
      consecutive1dTradeCounts.delete(symbol);
    }
  }
}

function incrementConsecutive1dCount(symbol) {
  const now = Date.now();
  const info = consecutive1dTradeCounts.get(symbol) || { count: 0, lastAcceptedAt: 0 };

  info.count = (info.count || 0) + 1;
  info.lastAcceptedAt = now;
  consecutive1dTradeCounts.set(symbol, info);
}

function getConsecutive1dCount(symbol) {
  const info = consecutive1dTradeCounts.get(symbol);
  return info ? info.count || 0 : 0;
}

// NEW: keepalive ping loop
function startKeepAliveLoop() {
  if (!config.ENABLE_KEEPALIVE_PING || !config.KEEPALIVE_URL) {
    return;
  }

  const url = String(config.KEEPALIVE_URL).trim();
  if (!url) {
    return;
  }

  const intervalMs = Number(config.KEEPALIVE_INTERVAL_MS || 5 * 60 * 1000);
  const http = url.startsWith('https') ? require('https') : require('http');

  if (keepaliveIntervalHandle) {
    clearInterval(keepaliveIntervalHandle);
  }

  keepaliveIntervalHandle = setInterval(() => {
    try {
      const req = http.get(url, (res) => {
        res.on('data', () => {});
        res.on('end', () => {});
      });

      req.on('error', (err) => {
        logger.debug({ err, url }, 'poller.keepalive: ping failed');
      });

      req.setTimeout(5000, () => {
        req.abort();
      });
    } catch (err) {
      logger.debug({ err, url }, 'poller.keepalive: unexpected ping error');
    }
  }, Math.max(1000, intervalMs));

  logger.info({ url, intervalMs }, 'poller.keepalive: started');
}

// NEW: realtime liquidity gate (EXTENDED)
async function checkRealtimeLiquidity(symbol, options = {}) {
  if (!config.LIQUIDITY_CHECK_ENABLED) {
    return { ok: true, depthUsdt: Infinity, computedMinDepth: 0 };
  }

  if (bybit && typeof bybit.fetchOrderBook === 'function') {
    try {
      const topLevels = Number(config.LIQUIDITY_CHECK_TOP_LEVELS || 10);
      const book = await bybit.fetchOrderBook(symbol, Math.max(10, topLevels));
      const bids = Array.isArray(book?.bids) ? book.bids : [];
      let depthUsdt = 0;

      for (const bid of bids.slice(0, topLevels)) {
        const price = Number(bid?.[0] ?? 0);
        const qty = Number(bid?.[1] ?? 0);
        if (Number.isFinite(price) && Number.isFinite(qty)) {
          depthUsdt += price * qty;
        }
      }

      const accountEquityUsd = Number(options.accountEquityUsd || 0);
      const targetPositionFrac = Number(config.TARGET_POSITION_FRAC || 0.01);
      const baseTargetPositionUsd = Math.max(1, accountEquityUsd * targetPositionFrac);
      const depthMultiplier = Number(options.depthMultiplierOverride || config.LIQUIDITY_DEPTH_MULTIPLIER || 3);
      const minDepthAbsolute = Number(config.LIQUIDITY_MIN_USDT_DEPTH || 1000);
      const computedMinDepth = Math.max(minDepthAbsolute, Math.round(baseTargetPositionUsd * depthMultiplier));

      const ok = depthUsdt >= computedMinDepth;

      return { ok, depthUsdt, computedMinDepth };
    } catch (err) {
      logger.debug({ err, symbol }, 'poller: realtime liquidity check failed; allowing by default');
      return { ok: true, depthUsdt: 0, computedMinDepth: 0 };
    }
  }

  return { ok: true, depthUsdt: 0, computedMinDepth: 0 };
}

function computeLiquidityAndVolumeThresholds({
  accountEquityUsd = 0,
  targetPositionFrac = Number(config.TARGET_POSITION_FRAC || 0.01),
  depthMultiplier = Number(config.LIQUIDITY_DEPTH_MULTIPLIER || 3),
  minDepthAbsolute = Number(config.LIQUIDITY_MIN_USDT_DEPTH || 1000),
  volumeMultiplier = Number(config.MIN_24H_VOLUME_MULTIPLIER || 5),
  min24hVolumeAbsolute = Number(config.MIN_24H_VOLUME_ABSOLUTE || 5000)
} = {}) {
  const targetPositionUsd = Math.max(1, accountEquityUsd * targetPositionFrac);
  const minDepth = Math.max(minDepthAbsolute, Math.round(targetPositionUsd * depthMultiplier));
  const min24hVolume = Math.max(min24hVolumeAbsolute, Math.round(targetPositionUsd * volumeMultiplier));
  return { targetPositionUsd, minDepth, min24hVolume };
}

async function passesCompositeLiquidityVolumeGate(symbol, { accountEquityUsd = 0 } = {}) {
  try {
    let mdata = null;
    try {
      if (marketData && typeof marketData.updateSymbolMarketData === 'function') {
        mdata = await marketData.updateSymbolMarketData(symbol);
      }
    } catch (err) {
      logger.debug({ err, symbol }, 'poller: marketData.updateSymbolMarketData failed for composite gate');
      mdata = null;
    }

    const { targetPositionUsd, minDepth, min24hVolume } = computeLiquidityAndVolumeThresholds({
      accountEquityUsd
    });

    const liqRes = await checkRealtimeLiquidity(symbol, { accountEquityUsd, depthMultiplierOverride: undefined });
    const passDepth = typeof liqRes.ok === 'boolean' ? liqRes.ok : false;

    const vol24h = Number(mdata?.volume_24h_usdt || 0);
    const pass24h = vol24h >= min24hVolume;

    const volChangePct = (mdata && typeof mdata.volume_change_pct !== 'undefined') ? Number(mdata.volume_change_pct) : null;
    const minVolChangePct = Number(config.MIN_24H_VOLUME_CHANGE_PCT ?? -1000);
    const passVolChange = volChangePct === null ? true : (volChangePct >= minVolChangePct);

    const ok = passDepth && pass24h && passVolChange;

    return {
      ok,
      details: {
        liq: liqRes,
        depthUsdt: liqRes.depthUsdt,
        computedMinDepth: liqRes.computedMinDepth || minDepth,
        passDepth,
        vol24h,
        min24hVolume,
        pass24h,
        volChangePct,
        minVolChangePct,
        passVolChange,
        targetPositionUsd
      }
    };
  } catch (err) {
    logger.debug({ err, symbol }, 'poller: composite liquidity/volume gate threw, defaulting to allow');
    return { ok: true, details: { error: err && err.message } };
  }
}

async function validateMtfAlignmentConsensus(symbol) {
  const alignment = await signalManager.evaluateMtfAlignment(symbol);

  const alignmentValues = Object.values(alignment || {});
  const totalCount = alignmentValues.length;

  if (totalCount === 0) {
    return { isAligned: false, mtfScore: 0, reason: 'no_data', positiveCount: 0, totalCount: 0 };
  }

  const positiveCount = alignmentValues.filter((value) => {
    if (typeof value === 'boolean') {
      return value === true;
    }

    if (value && typeof value === 'object') {
      if (typeof value.positive !== 'undefined') {
        return value.positive === true;
      }
      return !!value;
    }

    return !!value;
  }).length;

  const mtfScore = totalCount > 0 ? positiveCount / totalCount : 0;
  const threshold = Number(config.MTF_ALIGNMENT_RATING || 0.6);
  const isAligned = mtfScore > threshold;

  return {
    isAligned,
    mtfScore,
    positiveCount,
    totalCount,
    alignment,
    reason: isAligned ? 'threshold_exceeded' : `only_${positiveCount}_of_${totalCount}`
  };
}

/**
 * Check if MTF alignment state has changed and should trigger an alert
 * CRITICAL: Only fires when MTF reaches 100% AND last TF candle just opened
 */
function shouldFireMtfAlignmentAlert(symbol, tf, mtfValidation) {
  const stateKey = `${symbol}|${tf}`;
  const prevState = mtfAlignmentStates.get(stateKey);

  const isNow100Percent = mtfValidation.positiveCount === mtfValidation.totalCount && mtfValidation.totalCount > 0;

  if (!prevState) {
    mtfAlignmentStates.set(stateKey, {
      lastAlignmentState: isNow100Percent,
      lastFlipTime: Date.now()
    });
    return isNow100Percent;
  }

  const wasAlignedBefore = prevState.lastAlignmentState;

  if (!wasAlignedBefore && isNow100Percent) {
    mtfAlignmentStates.set(stateKey, {
      lastAlignmentState: isNow100Percent,
      lastFlipTime: Date.now()
    });
    return true;
  }

  if (wasAlignedBefore && isNow100Percent) {
    return false;
  }

  if (wasAlignedBefore && !isNow100Percent) {
    mtfAlignmentStates.set(stateKey, {
      lastAlignmentState: isNow100Percent,
      lastFlipTime: Date.now()
    });
    return false;
  }

  return false;
}

function registerBybitWsListeners() {
  if (!bybitWs || typeof bybitWs.on !== 'function') {
    return;
  }

  bybitWs.removeAllListeners('kline');
  bybitWs.on('kline', async (kline) => {
    try {
      if (!kline || !kline.symbol || !kline.timeframe) {
        return;
      }

      const symbol = String(kline.symbol).toUpperCase();
      const tf = normalizeRootTf(kline.timeframe);

      if (!isUsdtSymbol(symbol)) {
        return;
      }

      if (!tf) {
        return;
      }

      const rootTfs = buildRootTfs();

      if (!rootTfs.includes(tf)) {
        return;
      }

      const openTime = Number(kline.open_time);

      if (!Number.isFinite(openTime)) {
        return;
      }

      const wsSignal = {
        symbol,
        root_tf: tf,
        candle_open_time: openTime,
        detected_at: Date.now()
      };

      const processedStateKey = `poller.loop2.processedCandle.${symbol}.${tf}`;
      const processedOpen = Number(safeGetState(processedStateKey) || 0);

      if (openTime > processedOpen) {
        const flip = await macdUtil.isMacdFlip(symbol, tf);

        if (flip) {
          logger.info(
            { symbol, tf, openTime },
            'poller.ws: MACD flip detected'
          );

          const mtfValidation = await validateMtfAlignmentConsensus(symbol);

          if (!alreadyProcessedSignal(wsSignal, 'ws_rootcandle')) {
            const rootEventId = buildEventId('rootcandle', symbol, tf, openTime);

            const signal = await signalManager.handleRootSignal({
              symbol,
              root_tf: tf,
              detected_at: Date.now(),
              candle_open_time: openTime,
              eventId: rootEventId,
              signalType: 'rootcandle_update',
              notifyImmediately: false
            });

            if (signal) {
              const rootSignal = {
                ...signal,
                eventId: rootEventId,
                notificationType: 'new_root_candle',
                signalType: 'rootcandle_update',
                meta: {
                  ...(signal.meta || {}),
                  mtfScore: mtfValidation.mtfScore,
                  mtfAligned: mtfValidation.isAligned,
                  alignment: mtfValidation.alignment || {},
                  mtfAlignment: mtfValidation.alignment || {},
                  suppressDetailedTelegram: !config.TELEGRAM_PUSH_FULL
                }
              };

              notificationQueue.enqueueSignal(rootSignal, 'new_root_candle');
            }
          }
        }
        safeSetState(processedStateKey, openTime);
      }
    } catch (err) {
      logger.debug({ err }, 'poller.ws: ws kline event failed');
    }
  });
}

function startDbHealthCheckLoop() {
  setImmediate(() => {
    this.runDbHealthCheckLoop()
      .catch((err) => {
        logger.error({ err }, 'poller: DB health check loop crashed');
      });
  });
}

async function runDbHealthCheckLoop() {
  const HEALTH_CHECK_INTERVAL_MS = 10 * 1000;

  while (isRunning) {
    await sleep(HEALTH_CHECK_INTERVAL_MS);

    if (!isRunning) {
      break;
    }

    const wasHealthy = dbIsHealthy;
    const isCurrentlyHealthy = isDbReady();

    if (!isCurrentlyHealthy && wasHealthy) {
      logger.error('poller: ⚠️ DATABASE CONNECTION LOST - switching to in-memory cache');
      dbIsHealthy = false;
    } else if (isCurrentlyHealthy && !wasHealthy) {
      logger.info('poller: ✅ DATABASE CONNECTION RESTORED');
      dbIsHealthy = true;
    }

    lastDbConnectivityCheck = Date.now();
  }
}

module.exports = {
  start() {
    if (isRunning) {
      return;
    }

    isRunning = true;
    startupComplete = true;

    try {
      signalManager.setOpenTradesAllowed(true);
    } catch (err) {
      logger.debug({ err }, 'poller.start: failed to enable open trades');
    }

    registerBybitWsListeners();

    if (bybitWs && typeof bybitWs.start === 'function' && bybitWs.isEnabled()) {
      try {
        bybitWs.start();
        const db = safeGetDb();
        if (db) {
          try {
            const rows = db.prepare('SELECT symbol FROM symbols').all();
            if (rows && rows.length > 0) {
              const symbols = rows.map((r) => r.symbol).filter(isUsdtSymbol);
              bybitWs.subscribeSymbols(symbols);
            }
          } catch (err) {
            logger.debug({ err }, 'poller.start: failed to query symbols');
          }
        }
      } catch (err) {
        logger.warn({ err }, 'poller.start: failed to initialize WS stream');
      }
    }

    this.startCacheCleanupLoop();
    this.startDbHealthCheckLoop();
    this.startBoundaryScanLoop();
    this.startRootTfCandleOpenLoop();

    try {
      startKeepAliveLoop();
    } catch (err) {
      logger.debug({ err }, 'poller.start: failed to start keepalive loop');
    }
  },

  async initialScan(options = {}) {
    const { seed = true } = options;
    let allSymbols = [];
    const useWs = !!config.USE_WS;

    if (useWs) {
      try {
        const wsTimeoutMs = config.WS_INITIAL_SCAN_TIMEOUT || 10000;
        allSymbols = await Promise.race([
          this.performWsInitialScan(),
          new Promise((_, reject) => setTimeout(() => reject(new Error('WS scan timeout')), wsTimeoutMs))
        ]);
      } catch (err) {
        allSymbols = [];
      }
    }

    if (!Array.isArray(allSymbols) || allSymbols.length === 0) {
      allSymbols = await bybit.fetchAllSymbols();
    }

    if (!Array.isArray(allSymbols) || allSymbols.length === 0) {
      return [];
    }

    const db = await ensureDbReady(3, 500);
    if (!db) {
      return [];
    }

    try {
      const insert = db.prepare(`
        INSERT OR REPLACE INTO symbols
          (symbol, base, quote, fetched_at)
        VALUES (?, ?, ?, ?)
      `);

      const now = Date.now();
      const insertMany = db.transaction((rows) => {
        for (const symbolInfo of rows) {
          if (!symbolInfo || !symbolInfo.symbol) continue;
          insert.run(
            symbolInfo.symbol,
            symbolInfo.base || symbolInfo.symbol.replace(/USDT(\.P)?$/i, ''),
            symbolInfo.quote || 'USDT',
            now
          );
        }
      });

      const validSymbols = allSymbols.filter((symbolInfo) => symbolInfo && symbolInfo.symbol && isUsdtSymbol(symbolInfo.symbol));
      insertMany(validSymbols);

      if (bybitWs && typeof bybitWs.start === 'function' && bybitWs.isEnabled()) {
        try {
          bybitWs.start();
          bybitWs.subscribeSymbols(validSymbols);
        } catch (err) {
          logger.warn({ err }, 'poller.initialScan: WS subscription error');
        }
      }

      const seedSymbols = bybit.getSeedSymbols(validSymbols);
      if (seedSymbols && seedSymbols.length > 0 && seed) {
        setImmediate(() => {
          this.backgroundSeedKlines(seedSymbols).catch(() => {});
        });
      }

      return validSymbols;
    } catch (err) {
      logger.error({ err }, 'poller.initialScan: transaction failed');
      return [];
    }
  },

  async performWsInitialScan() {
    try {
      if (bybitWs && typeof bybitWs.performInitialScan === 'function') {
        const result = await bybitWs.performInitialScan();
        return Array.isArray(result) ? result : [];
      }
    } catch (err) {
      logger.debug({ err }, 'poller.performWsInitialScan failed');
    }
    return [];
  },

  async backgroundSeedKlines(symbols = []) {
    if (!Array.isArray(symbols) || symbols.length === 0) return;

    for (let i = 0; i < symbols.length; i += SEED_CONCURRENCY) {
      const batch = symbols.slice(i, i + SEED_CONCURRENCY);
      const jobs = batch.map((item) => limiter.schedule(() => this.seedKlinesForSymbol(item.symbol)));
      try {
        await Promise.all(jobs);
      } catch (err) {
        logger.debug({ err }, 'backgroundSeedKlines: batch failed');
      }
    }
  },

  async seedKlinesForSymbol(symbol, timeframe = null) {
    try {
      const symbolUpper = String(symbol || '').toUpperCase();
      if (!isUsdtSymbol(symbol) || /USDT[QHUZ0-9]/.test(symbolUpper.slice(-6))) return;

      const rootTfs = timeframe ? [String(timeframe)] : (Array.isArray(config.ROOT_TFS) ? config.ROOT_TFS : buildRootTfs());
      const mtfTfs = Array.isArray(config.MTF_TFS) ? config.MTF_TFS.map(String) : [];
      const timeframes = Array.from(new Set([...rootTfs, ...mtfTfs]));

      for (const tf of timeframes) {
        const interval = normalizeRootTf(tf) === 'D' ? 'D' : String(tf);
        try {
          const klines = await limiter.schedule(() => bybit.fetchKlines(symbol, interval, config.SEED_KLINES_LIMIT));
          if (!klines || klines.length === 0) continue;

          const db = safeGetDb();
          if (!db) continue;

          const insert = db.prepare(`
            INSERT OR IGNORE INTO klines
              (symbol, timeframe, open_time, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
          `);

          const insertMany = db.transaction((rows) => {
            for (const kline of rows) {
              insert.run(symbol, tf, kline.open_time, kline.open, kline.high, kline.low, kline.close, kline.volume);
            }
          });

          insertMany(klines);

          try {
            if (typeof macdUtil.computeAndStoreMacd === 'function') {
              await macdUtil.computeAndStoreMacd(symbol, tf);
            } else if (typeof macdUtil.computeMacdHistogram === 'function') {
              await macdUtil.computeMacdHistogram(symbol, tf);
            }
          } catch (err) {
            logger.debug({ err, symbol, tf }, 'seedKlinesForSymbol: MACD warm-up failed');
          }
        } catch (err) {
          logger.debug({ err, symbol, tf }, 'seedKlinesForSymbol: timeframe fetch failed');
        }
      }
    } catch (err) {
      logger.debug({ err, symbol, timeframe }, 'seedKlinesForSymbol: unexpected error');
    }
  },

  async quickFetchLatestKline(symbol, tf) {
    try {
      const interval = normalizeRootTf(tf) === 'D' ? 'D' : String(tf);
      const klines = await limiter.schedule(() => bybit.fetchKlines(symbol, interval, 2));
      if (!klines || klines.length === 0) return;

      const db = safeGetDb();
      if (!db) return;

      const insert = db.prepare(`
        INSERT OR IGNORE INTO klines
          (symbol, timeframe, open_time, open, high, low, close, volume)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      `);

      const insertMany = db.transaction((rows) => {
        for (const kline of rows) {
          insert.run(symbol, tf, kline.open_time, kline.open, kline.high, kline.low, kline.close, kline.volume);
        }
      });

      insertMany(klines);

      if (typeof macdUtil.computeAndStoreMacd === 'function') {
        await macdUtil.computeAndStoreMacd(symbol, tf);
      } else if (typeof macdUtil.computeMacdHistogram === 'function') {
        await macdUtil.computeMacdHistogram(symbol, tf);
      }
    } catch (err) {
      logger.debug({ err, symbol, tf }, 'quickFetchLatestKline: fetch failed');
    }
  },

  async processMissingDataQueueSafe(missingDataQueue = []) {
    if (!Array.isArray(missingDataQueue) || missingDataQueue.length === 0) return;

    const COOLDOWN_MS = 5 * 60 * 1000;
    const MAX_REST_FALLBACKS_PER_PASS = 30;
    const now = Date.now();

    const eligibleItems = missingDataQueue.filter((item) => {
      const key = `${item.symbol}|${item.tf}`;
      const lastAttempt = restFallbackCooldowns.get(key) || 0;
      return now - lastAttempt > COOLDOWN_MS;
    });

    const itemsToProcess = eligibleItems.slice(0, MAX_REST_FALLBACKS_PER_PASS);
    if (itemsToProcess.length === 0) return;

    for (const item of itemsToProcess) {
      const key = `${item.symbol}|${item.tf}`;
      restFallbackCooldowns.set(key, now);
      try {
        await this.quickFetchLatestKline(item.symbol, item.tf);
      } catch (err) {
        logger.debug({ err, symbol: item.symbol, tf: item.tf }, 'poller.bg: lightweight fetch error');
      }
    }
    cleanupRestFallbackCooldowns(10 * 60 * 1000);
  },

  async scanAllForStartup() {
    if (startupComplete) return [];

    try {
      const db = await ensureDbReady(3, 500);
      if (!db) return [];

      const rows = db.prepare(`SELECT symbol FROM symbols ORDER BY symbol COLLATE NOCASE ASC`).all();
      const validRows = rows.filter((row) => isUsdtSymbol(row.symbol));

      const newSignals = [];
      const pageSize = Number(config.PAGE_SIZE || 25);

      for (let i = 0; i < validRows.length; i += pageSize) {
        const page = validRows.slice(i, i + pageSize);
        const results = await Promise.all(page.map((row) => this.scanSymbolRoots(row.symbol)));
        newSignals.push(...results.flat());
      }

      newSignals.sort((a, b) => String(a.symbol || '').toUpperCase().localeCompare(String(b.symbol || '').toUpperCase()));

      if (newSignals.length > 0) {
        for (const signal of newSignals) {
          registerStartupBatchSignal(signal, signal.signalType || 'rootcandle_update');
        }
        notificationQueue.enqueueStartupBatch(newSignals);
      }

      this.initializeBoundaryCandleState(validRows, db);
      startupComplete = true;
      return newSignals;
    } catch (err) {
      logger.error({ err }, 'scanAllForStartup: unexpected error');
      return [];
    }
  },

  initializeBoundaryCandleState(rows, db) {
    if (!db) return;
    const rootTfs = buildRootTfs();

    for (const row of rows) {
      const symbol = row.symbol;
      for (const tf of rootTfs) {
        const latestOpen = getLatestOpenTime(db, symbol, tf);
        if (latestOpen === null) continue;
        const stateKey = this.getLoop2ProcessedCandleKey(symbol, tf);
        safeSetState(stateKey, latestOpen);
        trackRootTfBoundaryState(symbol, tf, latestOpen);
      }
    }
  },

  async scanSymbolRoots(symbol) {
    const rootTfs = buildRootTfs();
    const results = [];

    if (!isUsdtSymbol(symbol)) return results;

    for (const tf of rootTfs) {
      try {
        const db = safeGetDb();
        if (!db) continue;

        const selectStatement = db.prepare(`
          SELECT open_time, close, open
          FROM klines
          WHERE symbol = ?
            AND timeframe = ?
          ORDER BY open_time DESC
          LIMIT 2
        `);

        let rows = selectStatement.all(symbol, tf);

        if (!rows || rows.length < 2) {
          await this.seedKlinesForSymbol(symbol, tf);
          rows = selectStatement.all(symbol, tf);
          if (!rows || rows.length < 2) continue;
        }

        const flip = await macdUtil.isMacdFlip(symbol, tf);
        if (!flip) continue;

        const signal = await signalManager.handleRootSignal({
          symbol,
          root_tf: tf,
          detected_at: Date.now(),
          candle_open_time: Number(rows[0].open_time),
          notifyImmediately: false
        });

        if (signal) results.push(signal);
      } catch (err) {
        logger.debug({ err, symbol, tf }, 'scanSymbolRoots: error checking flip');
      }
    }

    return results;
  },

  getNextFiveMinuteBoundaryMs(nowMs = Date.now()) {
    const remainder = nowMs % FIVE_MINUTES_MS;
    return nowMs + (remainder === 0 ? FIVE_MINUTES_MS : FIVE_MINUTES_MS - remainder);
  },

  startBoundaryScanLoop() {
    setImmediate(() => {
      this.runBoundaryScanLoop().catch((err) => {
        logger.error({ err }, 'poller: boundary scan loop crashed');
      });
    });
  },

  async runBoundaryScanLoop() {
    while (isRunning) {
      const nextBoundary = this.getNextFiveMinuteBoundaryMs();
      const delay = Math.max(0, nextBoundary - Date.now());

      await sleep(delay);
      if (!isRunning) break;

      if (boundaryScanInProgress) continue;
      boundaryScanInProgress = true;

      try {
        await this.runBoundaryScanOnce();
      } catch (err) {
        logger.error({ err }, 'poller.loop2: boundary scan failed');
      } finally {
        boundaryScanInProgress = false;
      }
    }
  },

  async runBoundaryScanOnce() {
    clearExpiredProcessedSignalKeys();

    const db = safeGetDb();
    if (!db) return;

    try {
      const rows = db.prepare(`SELECT symbol FROM symbols ORDER BY symbol COLLATE NOCASE ASC`).all();
      const validRows = rows.filter((row) => isUsdtSymbol(row.symbol));

      const rootTfs = buildRootTfs();
      const alignmentAlerts = [];
      const latestSignals = dbModule.getLatestSignalsSnapshot() || [];

      for (const row of validRows) {
        const symbol = row.symbol;

        for (const tf of rootTfs) {
          try {
            let latestOpen = getLatestOpenTime(db, symbol, tf);
            if (latestOpen === null) {
              await this.seedKlinesForSymbol(symbol, tf);
              latestOpen = getLatestOpenTime(db, symbol, tf);
            }

            if (latestOpen === null) continue;

            const processedStateKey = this.getLoop2ProcessedCandleKey(symbol, tf);
            const processedOpen = Number(safeGetState(processedStateKey) || 0);

            if (latestOpen > processedOpen) {
              const activeSignals = latestSignals.filter((signal) => {
                return (
                  String(signal.symbol || '').toUpperCase() === String(symbol || '').toUpperCase() &&
                  normalizeRootTf(signal.root_tf) === tf
                );
              });

              const mtfValidation = await validateMtfAlignmentConsensus(symbol);

              // FIXED: If activeSignals is empty, fallback to creating a default activeSignal so MTF alignment alerts are never skipped
              if (shouldFireMtfAlignmentAlert(symbol, tf, mtfValidation)) {
                const activeSignal = activeSignals.length > 0 ? activeSignals[0] : {
                  symbol,
                  root_tf: tf,
                  candle_open_time: latestOpen,
                  meta: { tvScore: 0, tvSource: 'tradingview' }
                };
                const eventId = buildEventId('mtf_align', symbol, tf, latestOpen);

                alignmentAlerts.push({
                  ...activeSignal,
                  symbol,
                  root_tf: tf,
                  detected_at: Date.now(),
                  candle_open_time: latestOpen,
                  eventId,
                  signalType: 'mtf_alignment',
                  notificationType: 'mtf_alignment',
                  state: 'accept',
                  meta: {
                    ...(activeSignal.meta || {}),
                    alignment: mtfValidation.alignment || {},
                    decision: 'accept',
                    acceptReason: 'mtf_alignment_alert_100_percent',
                    tvScore: Number(activeSignal?.meta?.tvScore ?? activeSignal?.tvScore ?? 0),
                    tvSource: activeSignal?.meta?.tvSource || 'tradingview',
                    mtfScore: mtfValidation.mtfScore,
                    positiveCount: mtfValidation.positiveCount,
                    totalCount: mtfValidation.totalCount,
                    suppressDetailedTelegram: !config.TELEGRAM_PUSH_FULL
                  }
                });

                logger.info(
                  {
                    symbol,
                    tf,
                    mtfScore: (mtfValidation.mtfScore * 100).toFixed(0) + '%',
                    positiveCount: mtfValidation.positiveCount,
                    totalCount: mtfValidation.totalCount
                  },
                  'poller.loop2: MTF alignment reached 100% - alert generated'
                );
              }
            }
          } catch (err) {
            logger.debug({ err, symbol, tf }, 'poller.loop2: symbol/timeframe scan failed');
          }
        }
      }

      for (const alert of alignmentAlerts) {
        try {
          notificationQueue.enqueueSignal(alert, 'mtf_alignment');
        } catch (err) {
          logger.error({ err, alert }, 'poller.loop2: FAILED to enqueue alignment alert');
        }
      }
    } catch (err) {
      logger.error({ err }, 'poller.loop2: scan iteration failed');
    }
  },

  startRootTfCandleOpenLoop() {
    setImmediate(() => {
      this.runRootTfCandleOpenLoop().catch((err) => {
        logger.error({ err }, 'poller: root TF candle open loop crashed');
      });
    });
  },

  async runRootTfCandleOpenLoop() {
    while (isRunning) {
      const rootTfs = buildRootTfs();
      const nowMs = Date.now();
      let nextBoundaryMs = Infinity;

      for (const tf of rootTfs) {
        const next = getNextTimeframeBoundaryMs(tf, nowMs);
        if (next < nextBoundaryMs) nextBoundaryMs = next;
      }

      if (!isFinite(nextBoundaryMs)) nextBoundaryMs = nowMs + 60 * 60 * 1000;

      const dueTfs = [];
      for (const tf of rootTfs) {
        if (getNextTimeframeBoundaryMs(tf, nowMs) === nextBoundaryMs) dueTfs.push(tf);
      }

      const delay = Math.max(0, nextBoundaryMs - Date.now());
      await sleep(delay);

      if (!isRunning) break;
      await sleep(1500);

      try {
        const tfsToRun = dueTfs.length > 0 ? dueTfs : buildRootTfs();
        let scannedSuccess = false;
        for (let attempt = 1; attempt <= 3; attempt++) {
          scannedSuccess = await this.runRootTfCandleOpenOnce(tfsToRun, Date.now());
          if (scannedSuccess || attempt === 3) break;
          await sleep(2000);
        }
      } catch (err) {
        logger.error({ err }, 'poller: root TF candle open scan failed');
      }
    }
  },

  async runRootTfCandleOpenOnce(tfsInput, nowMs = Date.now()) {
    const scanStartTime = performance.now();
    let tfsToProcess = Array.isArray(tfsInput) ? tfsInput : [];

    if (tfsToProcess.length === 0) {
      const rootTfs = buildRootTfs();
      const epochStart = Date.UTC(1970, 0, 1);
      for (const tf of rootTfs) {
        if (Math.abs((nowMs - epochStart) % getRootTfMs(tf)) < 5 * 60 * 1000) tfsToProcess.push(tf);
      }
    }

    if (tfsToProcess.length === 0) return false;

    const db = await ensureDbReady(3, 500);
    if (!db) return false;

    try {
      const rows = db.prepare(`SELECT symbol FROM symbols ORDER BY symbol COLLATE NOCASE ASC`).all();
      const validRows = rows.filter((row) => isUsdtSymbol(row.symbol));

      const readyCandidates = [];
      const missingDataQueue = [];
      let detectedNewCandles = false;

      for (const row of validRows) {
        const symbol = row.symbol;
        for (const tf of tfsToProcess) {
          const latestOpen = getLatestOpenTime(db, symbol, tf);
          const processedStateKey = this.getLoop2ProcessedCandleKey(symbol, tf);
          const processedOpen = Number(safeGetState(processedStateKey) || 0);

          if (latestOpen !== null && latestOpen > processedOpen) {
            detectedNewCandles = true;
            readyCandidates.push({ symbol, tf, latestOpen, processedStateKey });
            trackRootTfBoundaryState(symbol, tf, latestOpen);
          } else {
            missingDataQueue.push({ symbol, tf });
          }
        }
      }

      if (missingDataQueue.length > 0) {
        setImmediate(async () => {
          await this.processMissingDataQueueSafe(missingDataQueue);
        });
      }

      if (readyCandidates.length === 0) return false;

      const allBoundarySignals = [];
      const candidatePromises = readyCandidates.map(async (candidate) => {
        const { symbol, tf, latestOpen, processedStateKey } = candidate;

        try {
          const flipKey = `${symbol}|${tf}|${latestOpen}`;
          if (macdFlipTracker.has(flipKey)) return null;

          const flip = await macdUtil.isMacdFlip(symbol, tf);
          if (flip) {
            macdFlipTracker.set(flipKey, { firedAt: Date.now() });

            const mtfValidation = await validateMtfAlignmentConsensus(symbol);
            const eventId = buildEventId('rootcandle', symbol, tf, latestOpen);

            const rootCandidate = {
              symbol,
              root_tf: tf,
              detected_at: Date.now(),
              candle_open_time: latestOpen,
              eventId,
              signalType: 'rootcandle_update',
              notifyImmediately: true
            };

            if (isStartupBatchSignal(rootCandidate, 'rootcandle')) {
              safeSetState(processedStateKey, latestOpen);
              return null;
            }

            if (!alreadyProcessedSignal(rootCandidate, 'rootcandle')) {
              const acceptThreshold = 0.7;
              let isHighConfidence = mtfValidation.mtfScore >= acceptThreshold;
              let decision = isHighConfidence ? 'accept' : 'monitor';

              let passedDailyFlip = false;
              let passedMaStructure = false;
              let passedDailyHistogramPositive = false;
              let passedDailyHistNegativeRising = false;

              try {
                try { passedDailyFlip = await macdUtil.isMacdFlip(symbol, 'D'); } catch (err) {}
                if (typeof macdUtil.is1dMaStructure === 'function') {
                  try { passedMaStructure = await macdUtil.is1dMaStructure(symbol); } catch (err) {}
                }
                if (typeof macdUtil.isDailyHistogramPositive === 'function') {
                  try { passedDailyHistogramPositive = await macdUtil.isDailyHistogramPositive(symbol); } catch (err) {}
                }
                if (typeof macdUtil.isDailyHistNegativeButRising === 'function') {
                  try { passedDailyHistNegativeRising = await macdUtil.isDailyHistNegativeButRising(symbol); } catch (err) {}
                }

                const maStructureWithPositiveOrRising = passedMaStructure && (passedDailyHistogramPositive || passedDailyHistNegativeRising);
                if (!(passedDailyFlip || maStructureWithPositiveOrRising)) {
                  decision = 'monitor';
                  isHighConfidence = false;
                }
              } catch (err) {
                decision = 'monitor';
                isHighConfidence = false;
              }

              let compositeDetails = null;
              if (decision === 'accept') {
                try {
                  let accountEquityUsd = 0;
                  if (typeof signalManager.getAccountEquityUsd === 'function') {
                    accountEquityUsd = Number(await signalManager.getAccountEquityUsd()) || 0;
                  }
                  const composite = await passesCompositeLiquidityVolumeGate(symbol, { accountEquityUsd });
                  compositeDetails = composite.details || composite;
                  if (!composite.ok) {
                    decision = 'monitor';
                    isHighConfidence = false;
                  }
                } catch (err) {}
              }

              if (decision === 'accept') {
                try {
                  if (normalizeRootTf(tf) === 'D') {
                    resetStaleConsecutive1dCounts();
                    const current = getConsecutive1dCount(symbol);
                    const maxAllowed = Number(config.MAX_CONSECUTIVE_ROOT_SIGNALS_1D || 2);
                    if (current >= maxAllowed) {
                      decision = 'monitor';
                      isHighConfidence = false;
                    } else {
                      incrementConsecutive1dCount(symbol);
                    }
                  }
                } catch (err) {}
              }

              let technicalRating = null;
              try {
                if (config.USE_INTERNAL_TECHNICAL_RATING && typeof macdUtil.computeTechnicalRating === 'function') {
                  technicalRating = await macdUtil.computeTechnicalRating(symbol, tf);
                }
              } catch (err) {}

              try {
                const persisted = await signalManager.handleRootSignal({
                  symbol,
                  root_tf: tf,
                  detected_at: Date.now(),
                  candle_open_time: latestOpen,
                  eventId,
                  signalType: 'rootcandle_update',
                  notifyImmediately: decision === 'accept'
                });

                if (!persisted) {
                  safeSetState(processedStateKey, latestOpen);
                  return null;
                }

                if (!persisted.meta) persisted.meta = {};
                const accept1dCriteria = [];
                if (passedDailyFlip) accept1dCriteria.push('daily_flip');
                if (passedMaStructure) accept1dCriteria.push('ma_7_14_28');
                if (passedDailyHistogramPositive) accept1dCriteria.push('daily_hist_positive');
                if (passedDailyHistNegativeRising) accept1dCriteria.push('daily_hist_negative_rising');

                const confidenceTag = isHighConfidence ? 'mtf_high_confidence' : 'mtf_monitor_required';
                persisted.meta.acceptReasons = [confidenceTag, ...accept1dCriteria];
                persisted.meta.acceptReason = persisted.meta.acceptReasons.join('+');

                if (compositeDetails) persisted.meta.liquidityGate = compositeDetails;
                if (technicalRating !== null) persisted.meta.technicalRating = technicalRating;

                const finalSignal = {
                  ...persisted,
                  eventId,
                  notificationType: 'new_root_candle',
                  signalType: 'rootcandle_update',
                  state: decision,
                  meta: {
                    ...(persisted.meta || {}),
                    mtfScore: mtfValidation.mtfScore,
                    mtfAligned: mtfValidation.isAligned,
                    alignment: mtfValidation.alignment || {},
                    decision,
                    acceptReason: persisted.meta.acceptReason,
                    suppressDetailedTelegram: !config.TELEGRAM_PUSH_FULL
                  }
                };

                safeSetState(processedStateKey, latestOpen);
                return finalSignal;
              } catch (err) {
                safeSetState(processedStateKey, latestOpen);
                return null;
              }
            }
          }
        } catch (err) {
          logger.debug({ err, symbol, tf }, 'poller: error evaluating fast-path candidate');
        }
        return null;
      });

      const evaluatedResults = await Promise.all(candidatePromises);
      for (const res of evaluatedResults) {
        if (res) allBoundarySignals.push(res);
      }

      if (allBoundarySignals.length > 0) {
        for (const tf of tfsToProcess) {
          const tfSignals = allBoundarySignals.filter((s) => normalizeRootTf(s.root_tf) === tf);
          if (tfSignals.length > 0) {
            for (const s of tfSignals) {
              if (!s.meta) s.meta = {};
              s.meta.suppressDetailedTelegram = !config.TELEGRAM_PUSH_FULL;
            }
            notificationQueue.enqueueRootCandleOpenBatch(tfSignals, tf, false);
          }
        }
      }

      return detectedNewCandles;
    } catch (err) {
      logger.error({ err }, 'poller.runRootTfCandleOpenOnce: fatal error');
      return false;
    }
  },

  getLoop2ProcessedCandleKey(symbol, tf) {
    return `poller.loop2.processedCandle.${symbol}.${tf}`;
  },

  startCacheCleanupLoop() {
    setImmediate(() => {
      this.runCacheCleanupLoop().catch((err) => {
        logger.error({ err }, 'poller: cache cleanup loop crashed');
      });
    });
  },

  async runCacheCleanupLoop() {
    const CLEANUP_INTERVAL_MS = 5 * 60 * 1000;

    while (isRunning) {
      await sleep(CLEANUP_INTERVAL_MS);
      if (!isRunning) break;

      try {
        clearExpiredProcessedSignalKeys(60 * 60 * 1000);
        resetStaleConsecutive1dCounts();
      } catch (err) {
        logger.error({ err }, 'poller: cache cleanup failed');
      }
    }
  },

  startDbHealthCheckLoop,
  runDbHealthCheckLoop,

  stop() {
    isRunning = false;
    if (keepaliveIntervalHandle) {
      clearInterval(keepaliveIntervalHandle);
      keepaliveIntervalHandle = null;
    }
    logger.info('poller: stopping loops');
  }
};
