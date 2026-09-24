const dbModule = require('../db');
const bybit = require('./bybitRest');
const config = require('../config');
const logger = require('pino')();
const Bottleneck = require('bottleneck');
const macdUtil = require('./macd');
const signalManager = require('./signalManager');
const notificationQueue = require('./notificationQueue');
const wsManager = require('./bybitWs');

const restLimiter = new Bottleneck({
  maxConcurrent: Number(
    config.REST_MAX_CONCURRENT ||
    process.env.REST_MAX_CONCURRENT ||
    6
  ),
  minTime: Number(
    config.REST_MIN_TIME_MS ||
    process.env.REST_MIN_TIME_MS ||
    100
  )
});

const FIVE_MINUTES_MS = 5 * 60 * 1000;
const DEFAULT_REST_UPDATE_INTERVAL_MS = 60 * 1000;
const DEFAULT_WS_STALE_MS = 10 * 60 * 1000;

let isRunning = false;
let startupComplete = false;
let boundaryScanInProgress = false;
let restUpdateInProgress = false;
let wsListenerAttached = false;
let restUpdaterTimer = null;
let startupPromise = null;
let mtfLoopTimer = null;
let midCandleLoopTimer = null;

function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms || 0);
  });
}

function configBool(value, defaultValue = false) {
  if (value === undefined || value === null) {
    return defaultValue;
  }

  if (typeof value === 'boolean') {
    return value;
  }

  const normalized = String(value)
    .trim()
    .toLowerCase();

  if (['1', 'true', 'yes', 'on'].includes(normalized)) {
    return true;
  }

  if (['0', 'false', 'no', 'off'].includes(normalized)) {
    return false;
  }

  return defaultValue;
}

function normalizeTimeframe(timeframe) {
  if (timeframe === null || timeframe === undefined) {
    return null;
  }

  const value = String(timeframe)
    .trim()
    .toUpperCase();

  if (value === '1H' || value === 'H') {
    return '60';
  }

  if (value === '1D') {
    return 'D';
  }

  return value;
}

function buildRootTfs() {
  const configured = Array.isArray(config.ROOT_TFS)
    ? config.ROOT_TFS
    : ['60', '240', 'D'];

  const output = [];
  const seen = new Set();

  for (const timeframe of configured) {
    const normalized = normalizeTimeframe(timeframe);

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

function buildAllTfs() {
  const configured = [
    ...buildRootTfs(),
    ...(Array.isArray(config.MTF_TFS)
      ? config.MTF_TFS
      : [])
  ];

  const output = [];
  const seen = new Set();

  for (const timeframe of configured) {
    const normalized = normalizeTimeframe(timeframe);

    if (!normalized || seen.has(normalized)) {
      continue;
    }

    seen.add(normalized);
    output.push(normalized);
  }

  return output;
}

function isUsdtSymbol(symbol) {
  const value = String(symbol || '').toUpperCase();

  if (!value) {
    return false;
  }

  if (/USDT[QHUZ0-9]/.test(value.slice(-6))) {
    return false;
  }

  return /USDT(\.P)?$/.test(value);
}

function getLatestOpenTime(db, symbol, timeframe) {
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

  const value = Number(row.open_time);

  return Number.isFinite(value)
    ? value
    : null;
}

function getSymbolsFromDb() {
  const db = dbModule.get();

  return db
    .prepare(
      `
        SELECT symbol
        FROM symbols
        ORDER BY symbol COLLATE NOCASE ASC
      `
    )
    .all()
    .map((row) => row.symbol)
    .filter(isUsdtSymbol);
}

function upsertKlines(symbol, timeframe, klines) {
  if (!Array.isArray(klines) || klines.length === 0) {
    return 0;
  }

  const db = dbModule.get();

  const statement = db.prepare(
    `
      INSERT INTO klines
        (
          symbol,
          timeframe,
          open_time,
          open,
          high,
          low,
          close,
          volume
        )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(symbol, timeframe, open_time)
      DO UPDATE SET
        open = excluded.open,
        high = excluded.high,
        low = excluded.low,
        close = excluded.close,
        volume = excluded.volume
    `
  );

  let count = 0;

  const transaction = db.transaction((rows) => {
    for (const kline of rows) {
      const openTime = Number(kline.open_time);

      if (!Number.isFinite(openTime)) {
        continue;
      }

      statement.run(
        symbol,
        timeframe,
        openTime,
        Number(kline.open || 0),
        Number(kline.high || 0),
        Number(kline.low || 0),
        Number(kline.close || 0),
        Number(kline.volume || 0)
      );

      count++;
    }
  });

  transaction(klines);

  return count;
}

function getRestIntervalMs() {
  return Number(
    config.REST_UPDATE_INTERVAL_MS ||
    process.env.REST_UPDATE_INTERVAL_MS ||
    DEFAULT_REST_UPDATE_INTERVAL_MS
  );
}

function getWsStaleMs() {
  return Number(
    config.WS_STALE_MS ||
    process.env.WS_STALE_MS ||
    DEFAULT_WS_STALE_MS
  );
}

function isWsEnabled() {
  return configBool(
    config.USE_WS ??
    process.env.USE_WS,
    true
  );
}

async function persistWsKline(kline) {
  if (
    !kline ||
    !kline.symbol ||
    !kline.timeframe
  ) {
    return;
  }

  const symbol = kline.symbol;
  const timeframe = normalizeTimeframe(kline.timeframe);

  if (
    !isUsdtSymbol(symbol) ||
    !timeframe
  ) {
    return;
  }

  upsertKlines(symbol, timeframe, [kline]);

  logger.debug(
    {
      symbol,
      timeframe,
      openTime: kline.open_time,
      confirm: kline.confirm
    },
    'poller: WebSocket kline persisted'
  );
}

async function fetchAndPersistRestKlines(symbol, timeframe, limit) {
  try {
    const klines = await restLimiter.schedule(() => {
      return bybit.fetchKlines(symbol, timeframe, limit);
    });

    if (!Array.isArray(klines) || klines.length === 0) {
      return 0;
    }

    return upsertKlines(symbol, timeframe, klines);
  } catch (err) {
    logger.debug(
      {
        err,
        symbol,
        timeframe
      },
      'poller: REST kline fetch failed'
    );

    return 0;
  }
}

async function updateAllKlinesFromRest() {
  if (restUpdateInProgress) {
    return;
  }

  restUpdateInProgress = true;

  const startedAt = Date.now();
  const symbols = getSymbolsFromDb();

  const wsHealthy =
    isWsEnabled() &&
    wsManager.isHealthy(getWsStaleMs());

  const timeframes = wsHealthy
    ? buildRootTfs()
    : buildAllTfs();

  const limit = Number(
    config.REST_UPDATE_LIMIT ||
    process.env.REST_UPDATE_LIMIT ||
    100
  );

  let completed = 0;

  try {
    logger.info(
      {
        symbols: symbols.length,
        timeframes,
        wsHealthy
      },
      'poller: REST batch update started'
    );

    for (
      let index = 0;
      index < symbols.length;
      index += Number(
        config.REST_BATCH_SIZE ||
        process.env.REST_BATCH_SIZE ||
        25
      )
    ) {
      const batchSize = Number(
        config.REST_BATCH_SIZE ||
        process.env.REST_BATCH_SIZE ||
        25
      );

      const batch = symbols.slice(index, index + batchSize);
      const jobs = [];

      for (const symbol of batch) {
        for (const timeframe of timeframes) {
          jobs.push(fetchAndPersistRestKlines(symbol, timeframe, limit));
        }
      }

      const results = await Promise.all(jobs);
      completed += results.reduce((total, value) => total + value, 0);
    }

    logger.info(
      {
        symbols: symbols.length,
        timeframes,
        completed,
        durationMs: Date.now() - startedAt
      },
      'poller: REST batch update completed'
    );
  } finally {
    restUpdateInProgress = false;
  }
}

function startRestUpdater() {
  if (restUpdaterTimer) {
    return;
  }

  const interval = getRestIntervalMs();

  const run = async () => {
    if (!isRunning) {
      restUpdaterTimer = null;
      return;
    }

    try {
      await updateAllKlinesFromRest();
    } catch (err) {
      logger.error(
        { err },
        'poller: REST updater failed'
      );
    }

    if (isRunning) {
      restUpdaterTimer = setTimeout(run, interval);
    } else {
      restUpdaterTimer = null;
    }
  };

  void run();

  logger.info(
    {
      intervalMs: interval
    },
    'poller: REST updater started'
  );
}

function attachWsListener() {
  if (wsListenerAttached) {
    return;
  }

  wsListenerAttached = true;

  wsManager.on('kline', (kline) => {
    void persistWsKline(kline)
      .catch((err) => {
        logger.debug(
          { err },
          'poller: WebSocket kline persistence failed'
        );
      });
  });
}

function subscribeAllSymbolsToWs() {
  if (!isWsEnabled()) {
    logger.info('poller: WebSocket disabled; REST fallback active');
    return 0;
  }

  if (!wsManager.start()) {
    return 0;
  }

  const symbols = getSymbolsFromDb();
  const timeframes = buildAllTfs();

  const subscribed = wsManager.subscribeSymbols(symbols, timeframes);

  logger.info(
    {
      symbols: symbols.length,
      subscribed,
      timeframes
    },
    'poller: WebSocket subscriptions requested'
  );

  return subscribed;
}

async function discoverAndPersistSymbols() {
  const symbols = await bybit.fetchAllSymbols();

  if (
    !Array.isArray(symbols) ||
    symbols.length === 0
  ) {
    logger.warn('poller: REST symbol discovery returned no symbols');
    return [];
  }

  const db = dbModule.get();

  const insert = db.prepare(
    `
      INSERT OR REPLACE INTO symbols
        (symbol, base, quote, fetched_at)
      VALUES (?, ?, ?, ?)
    `
  );

  const now = Date.now();

  const transaction = db.transaction((rows) => {
    for (const item of rows) {
      if (
        !item ||
        !item.symbol ||
        !isUsdtSymbol(item.symbol)
      ) {
        continue;
      }

      insert.run(
        item.symbol,
        item.base ||
          item.symbol.replace(/USDT(\.P)?$/i, ''),
        item.quote || 'USDT',
        now
      );
    }
  });

  transaction(symbols);

  logger.info(
    {
      count: symbols.length
    },
    'poller: symbols discovered and persisted'
  );

  return symbols;
}

async function seedSymbolTimeframes(symbol) {
  const timeframes = buildAllTfs();

  for (const timeframe of timeframes) {
    await fetchAndPersistRestKlines(
      symbol,
      timeframe,
      Number(
        config.SEED_KLINES_LIMIT ||
        process.env.SEED_KLINES_LIMIT ||
        500
      )
    );
  }
}

function getRootSignalStateKey(symbol, timeframe) {
  return [
    'poller.loop2.lastRootOpen',
    symbol,
    normalizeTimeframe(timeframe)
  ].join('.');
}

function getMidcandleKey(symbol, timeframe) {
  return [
    'poller.midcandle.lastOpen',
    symbol,
    normalizeTimeframe(timeframe)
  ].join('.');
}

module.exports = {
  start() {
    if (isRunning) {
      logger.debug('poller.start: poller already running');
      return;
    }

    isRunning = true;

    attachWsListener();
    startRestUpdater();

    logger.info('poller.start: continuous services started');
  },

  async initialScan(options = {}) {
    if (startupPromise) {
      return startupPromise;
    }

    startupPromise = (async () => {
      const seed = options.seed !== false;

      logger.info(
        {
          seed
        },
        'poller.initialScan: starting'
      );

      const symbols = await discoverAndPersistSymbols();

      attachWsListener();
      subscribeAllSymbolsToWs();

      const seedSymbols = bybit.getSeedSymbols(symbols);

      if (seed && seedSymbols.length > 0) {
        await this.backgroundSeedKlines(seedSymbols);
      }

      logger.info(
        {
          symbols: symbols.length,
          seeded: seed && seedSymbols.length > 0
        },
        'poller.initialScan: completed'
      );

      return symbols;
    })();

    try {
      return await startupPromise;
    } catch (err) {
      startupPromise = null;
      throw err;
    }
  },

  async performWsInitialScan() {
    try {
      const result = await wsManager.performInitialScan();

      return Array.isArray(result) ? result : [];
    } catch (err) {
      logger.debug(
        {
          err
        },
        'poller.performWsInitialScan failed'
      );

      return [];
    }
  },

  async backgroundSeedKlines(symbols = []) {
    if (
      !Array.isArray(symbols) ||
      symbols.length === 0
    ) {
      logger.info('poller.backgroundSeedKlines: no symbols');
      return;
    }

    const concurrency = Number(
      config.SEED_CONCURRENCY ||
      process.env.SEED_CONCURRENCY ||
      6
    );

    logger.info(
      {
        symbols: symbols.length,
        concurrency,
        timeframes: buildAllTfs()
      },
      'poller.backgroundSeedKlines: started'
    );

    for (
      let index = 0;
      index < symbols.length;
      index += concurrency
    ) {
      const batch = symbols.slice(index, index + concurrency);

      await Promise.all(
        batch.map((item) => {
          const symbol =
            typeof item === 'string'
              ? item
              : item && item.symbol;

          return seedSymbolTimeframes(symbol);
        })
      );
    }

    logger.info('poller.backgroundSeedKlines: completed');
  },

  async seedKlinesForSymbol(symbol, timeframe = null) {
    if (!isUsdtSymbol(symbol)) {
      logger.warn(
        {
          symbol
        },
        'poller.seedKlinesForSymbol: invalid symbol'
      );

      return;
    }

    const timeframes = timeframe
      ? [normalizeTimeframe(timeframe)]
      : buildAllTfs();

    for (const currentTimeframe of timeframes) {
      if (!currentTimeframe) {
        continue;
      }

      await fetchAndPersistRestKlines(
        symbol,
        currentTimeframe,
        Number(
          config.SEED_KLINES_LIMIT ||
          process.env.SEED_KLINES_LIMIT ||
          500
        )
      );
    }
  },

  async scanAllForStartup() {
    if (startupComplete) {
      logger.debug('poller.scanAllForStartup: already completed');
      return [];
    }

    const db = dbModule.get();

    const rows = db
      .prepare(
        `
          SELECT symbol
          FROM symbols
          ORDER BY symbol COLLATE NOCASE ASC
        `
      )
      .all()
      .filter((row) => isUsdtSymbol(row.symbol));

    const signals = [];
    const pageSize = Number(
      config.PAGE_SIZE ||
      process.env.PAGE_SIZE ||
      25
    );

    for (
      let index = 0;
      index < rows.length;
      index += pageSize
    ) {
      const page = rows.slice(index, index + pageSize);

      const results = await Promise.all(
        page.map((row) => this.scanSymbolRoots(row.symbol))
      );

      for (const result of results) {
        signals.push(...result);
      }
    }

    if (signals.length > 0) {
      notificationQueue.enqueueStartupBatch(signals);
    }

    this.initializeBoundaryCandleState(rows, db);
    startupComplete = true;

    logger.info(
      {
        signals: signals.length,
        symbols: rows.length
      },
      'poller.scanAllForStartup: completed'
    );

    return signals;
  },

  initializeBoundaryCandleState(rows, db) {
    const rootTfs = buildRootTfs();

    for (const row of rows) {
      for (const timeframe of rootTfs) {
        const latestOpen = getLatestOpenTime(db, row.symbol, timeframe);

        if (latestOpen === null) {
          continue;
        }

        dbModule.setState(
          getRootSignalStateKey(row.symbol, timeframe),
          latestOpen
        );
      }
    }

    logger.info(
      {
        symbols: rows.length,
        timeframes: rootTfs
      },
      'poller: boundary state initialized'
    );
  },

  async scanSymbolRoots(symbol) {
    const db = dbModule.get();
    const results = [];

    if (!isUsdtSymbol(symbol)) {
      return results;
    }

    for (const timeframe of buildRootTfs()) {
      const rows = db
        .prepare(
          `
            SELECT open_time
            FROM klines
            WHERE symbol = ?
              AND timeframe = ?
            ORDER BY open_time DESC
            LIMIT 2
          `
        )
        .all(symbol, timeframe);

      if (rows.length < 2) {
        continue;
      }

      const closedOpenTime = Number(rows[0].open_time);

      const flip =
        typeof macdUtil.isMacdFlipAtClosedCandle === 'function'
          ? await macdUtil.isMacdFlipAtClosedCandle(
              symbol,
              timeframe,
              closedOpenTime
            )
          : await macdUtil.isMacdFlip(symbol, timeframe);

      if (!flip) {
        continue;
      }

      const signal = await signalManager.handleRootSignal({
        symbol,
        root_tf: timeframe,
        detected_at: Date.now(),
        candle_open_time: closedOpenTime,
        notifyImmediately: false,
        notificationType: 'startup'
      });

      if (signal) {
        results.push(signal);
      }
    }

    return results;
  },

  getNextFiveMinuteBoundaryMs(nowMs = Date.now()) {
    const remainder = nowMs % FIVE_MINUTES_MS;

    return nowMs + (
      remainder === 0
        ? FIVE_MINUTES_MS
        : FIVE_MINUTES_MS - remainder
    );
  },

  startBoundaryScanLoop() {
    if (boundaryScanInProgress) {
      return;
    }

    setImmediate(() => {
      this.runBoundaryScanLoop()
        .catch((err) => {
          logger.error(
            { err },
            'poller: boundary loop crashed'
          );
        });
    });
  },

  async runBoundaryScanLoop() {
    while (isRunning) {
      if (!startupComplete) {
        logger.debug('poller.loop2: startup not complete; waiting');
        await sleep(5000);
        continue;
      }

      const nextBoundary = this.getNextFiveMinuteBoundaryMs();

      const delay = Math.max(0, nextBoundary - Date.now());

      logger.debug(
        {
          nextBoundary: new Date(nextBoundary).toISOString(),
          delayMs: delay
        },
        'poller.loop2: waiting for boundary'
      );

      await sleep(delay);

      if (!isRunning) {
        break;
      }

      if (boundaryScanInProgress) {
        logger.warn('poller.loop2: previous scan still running');
        continue;
      }

      boundaryScanInProgress = true;

      try {
        await this.runBoundaryScanOnce();
      } catch (err) {
        logger.error(
          { err },
          'poller.loop2: boundary scan failed'
        );
      } finally {
        boundaryScanInProgress = false;
      }
    }
  },

  async runBoundaryScanOnce() {
    const startedAt = Date.now();
    const db = dbModule.get();
    const newSignals = [];

    const rows = db
      .prepare(
        `
          SELECT symbol
          FROM symbols
          ORDER BY symbol COLLATE NOCASE ASC
        `
      )
      .all()
      .filter((row) => isUsdtSymbol(row.symbol));

    const rootTfs = buildRootTfs();

    logger.info(
      {
        symbols: rows.length,
        rootTfs
      },
      'poller.loop2: boundary scan started'
    );

    for (const row of rows) {
      const symbol = row.symbol;

      for (const timeframe of rootTfs) {
        const latestOpen = getLatestOpenTime(db, symbol, timeframe);

        if (latestOpen === null) {
          continue;
        }

        const stateKey = getRootSignalStateKey(symbol, timeframe);
        const processedOpen = Number(dbModule.getState(stateKey) || 0);

        if (latestOpen <= processedOpen) {
          continue;
        }

        const previousRow = db
          .prepare(
            `
              SELECT open_time
              FROM klines
              WHERE symbol = ?
                AND timeframe = ?
                AND open_time < ?
              ORDER BY open_time DESC
              LIMIT 1
            `
          )
          .get(symbol, timeframe, latestOpen);

        if (!previousRow) {
          dbModule.setState(stateKey, latestOpen);
          continue;
        }

        const closedOpenTime = Number(previousRow.open_time);

        const flip =
          typeof macdUtil.isMacdFlipAtClosedCandle === 'function'
            ? await macdUtil.isMacdFlipAtClosedCandle(
                symbol,
                timeframe,
                closedOpenTime
              )
            : await macdUtil.isMacdFlip(symbol, timeframe);

        logger.info(
          {
            symbol,
            timeframe,
            latestOpen,
            processedOpen,
            closedOpenTime,
            flip
          },
          'poller.loop2: new root candle evaluated'
        );

        if (flip) {
          const signal = await signalManager.handleRootSignal({
            symbol,
            root_tf: timeframe,
            detected_at: Date.now(),
            candle_open_time: closedOpenTime,
            notifyImmediately: true,
            notificationType: 'new_root_candle',
            skipTradeOpen: false
          });

          if (signal) {
            signal.notificationType = 'new_root_candle';
            newSignals.push(signal);
          }
        }

        dbModule.setState(stateKey, latestOpen);
      }
    }

    if (newSignals.length > 0) {
      notificationQueue.enqueueRootCandleBatch(newSignals);
      logger.info(
        {
          count: newSignals.length
        },
        'poller.loop2: root candle batch enqueued'
      );
    } else {
      logger.info('poller.loop2: no new root flips found');
    }

    logger.info(
      {
        newSignals: newSignals.length,
        durationMs: Date.now() - startedAt
      },
      'poller.loop2: boundary scan completed'
    );
  },

  startMidCandleLoop() {
    if (midCandleLoopTimer) {
      return;
    }

    const run = async () => {
      if (!isRunning || !startupComplete) {
        return;
      }

      try {
        await this.scanMidCandleSignals();
      } catch (err) {
        logger.error(
          { err },
          'poller.midcandle: loop failed'
        );
      }

      if (isRunning && startupComplete) {
        midCandleLoopTimer = setTimeout(run, 60 * 1000);
      } else {
        midCandleLoopTimer = null;
      }
    };

    void run();
  },

  async scanMidCandleSignals() {
    const db = dbModule.get();
    const symbols = getSymbolsFromDb();

    for (const symbol of symbols) {
      for (const timeframe of buildRootTfs()) {
        const latestOpen = getLatestOpenTime(db, symbol, timeframe);

        if (latestOpen === null) {
          continue;
        }

        const stateKey = getMidcandleKey(symbol, timeframe);
        const seenOpen = Number(dbModule.getState(stateKey) || 0);

        if (latestOpen <= seenOpen) {
          continue;
        }

        const previousRow = db
          .prepare(
            `
              SELECT open_time
              FROM klines
              WHERE symbol = ?
                AND timeframe = ?
                AND open_time < ?
              ORDER BY open_time DESC
              LIMIT 1
            `
          )
          .get(symbol, timeframe, latestOpen);

        if (!previousRow) {
          dbModule.setState(stateKey, latestOpen);
          continue;
        }

        const closedOpenTime = Number(previousRow.open_time);

        const flip =
          typeof macdUtil.isMacdFlipAtClosedCandle === 'function'
            ? await macdUtil.isMacdFlipAtClosedCandle(
                symbol,
                timeframe,
                closedOpenTime
              )
            : await macdUtil.isMacdFlip(symbol, timeframe);

        if (flip) {
          await signalManager.handleRootSignal({
            symbol,
            root_tf: timeframe,
            detected_at: Date.now(),
            candle_open_time: closedOpenTime,
            notifyImmediately: true,
            notificationType: 'midcandle_update',
            skipTradeOpen: true
          });
        }

        dbModule.setState(stateKey, latestOpen);
      }
    }
  },

  startMtfAlignmentLoop() {
    if (mtfLoopTimer) {
      return;
    }

    const run = async () => {
      if (!isRunning || !startupComplete) {
        return;
      }

      try {
        await this.scanMtfAlignmentAlerts();
      } catch (err) {
        logger.error(
          { err },
          'poller.mtf: loop failed'
        );
      }

      if (isRunning && startupComplete) {
        mtfLoopTimer = setTimeout(run, 75 * 1000);
      } else {
        mtfLoopTimer = null;
      }
    };

    void run();
  },

  async scanMtfAlignmentAlerts() {
    const symbols = getSymbolsFromDb();

    for (const symbol of symbols) {
      try {
        const alignment = await signalManager.evaluateMtfAlignment(symbol);
        const stateKey = `poller.mtf.lastAlignment.${symbol}`;

        const oldState = dbModule.getState(stateKey);
        const currentState = JSON.stringify(alignment);

        if (oldState === currentState) {
          continue;
        }

        dbModule.setState(stateKey, alignment);

        const signal = {
          symbol,
          root_tf: buildRootTfs()[0] || '60',
          detected_at: Date.now(),
          candle_open_time: null,
          notificationType: 'mtf_alignment',
          meta: {
            alignment,
            decision: 'monitor',
            acceptReason: 'mtf_alignment_alert',
            mtfScore: Object.keys(alignment || {}).length
              ? Object.values(alignment).filter((v) => v && v.positive).length /
                Object.keys(alignment).length
              : 0,
            tvScore: 0,
            tvSource: 'n/a',
            marketData: {
              price: 0,
              volume_24h_usdt: 0,
              volume_change_pct: null,
              market_cap: null
            }
          }
        };

        notificationQueue.enqueueSignal(signal, 'mtf_alignment');
      } catch (err) {
        logger.debug(
          {
            err,
            symbol
          },
          'poller.mtf: alignment alert failed'
        );
      }
    }
  },

  getLoop2ProcessedCandleKey(symbol, timeframe) {
    return getRootSignalStateKey(symbol, timeframe);
  }
};
