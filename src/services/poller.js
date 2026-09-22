const dbModule = require('../db');
const bybit = require('./bybitRest');
const config = require('../config');
const logger = require('pino')();
const Bottleneck = require('bottleneck');
const macdUtil = require('./macd');
const signalManager = require('./signalManager');
const notificationQueue = require('./notificationQueue');

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
}

module.exports = {
  start() {
    if (isRunning) {
      logger.debug(
        'poller.start: poller is already running'
      );

      return;
    }

    isRunning = true;
    startupComplete = true;

    try {
      signalManager.setOpenTradesAllowed(true);

      logger.info(
        'poller.start: open trades enabled'
      );
    } catch (err) {
      logger.debug(
        { err },
        'poller.start: failed to enable open trades'
      );
    }

    this.startBoundaryScanLoop();

    logger.info(
      'poller.start: boundary scan loop started'
    );
  },

  async initialScan(options = {}) {
    const {
      seed = true
    } = options;

    logger.info(
      { seed },
      'poller.initialScan: starting'
    );

    let allSymbols = [];
    const useWs = !!config.USE_WS;

    if (useWs) {
      try {
        const wsTimeoutMs = config.WS_INITIAL_SCAN_TIMEOUT || 10000;

        logger.info(
          { timeoutMs: wsTimeoutMs },
          'poller: attempting WS initial scan'
        );

        allSymbols = await Promise.race([
          this.performWsInitialScan(),
          new Promise((_, reject) => {
            setTimeout(() => {
              reject(new Error('WS scan timeout'));
            }, wsTimeoutMs);
          })
        ]);

        if (
          !Array.isArray(allSymbols) ||
          allSymbols.length === 0
        ) {
          logger.warn(
            'poller: WS initial scan returned no symbols; falling back to REST'
          );

          allSymbols = [];
        } else {
          logger.info(
            { count: allSymbols.length },
            'poller: WS initial scan provided symbols'
          );
        }
      } catch (err) {
        logger.debug(
          { err },
          'poller: WS initial scan failed or timed out; falling back to REST'
        );

        allSymbols = [];
      }
    }

    if (
      !Array.isArray(allSymbols) ||
      allSymbols.length === 0
    ) {
      logger.info(
        'poller: fetching symbols via REST'
      );

      allSymbols = await bybit.fetchAllSymbols();
    }

    if (
      !Array.isArray(allSymbols) ||
      allSymbols.length === 0
    ) {
      logger.warn(
        'poller.initialScan: no symbols discovered'
      );

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

    const insertMany = db.transaction((rows) => {
      for (const symbolInfo of rows) {
        if (!symbolInfo || !symbolInfo.symbol) {
          continue;
        }

        insert.run(
          symbolInfo.symbol,
          symbolInfo.base || symbolInfo.symbol.replace(/USDT(\.P)?$/i, ''),
          symbolInfo.quote || 'USDT',
          now
        );
      }
    });

    insertMany(
      allSymbols.filter((symbolInfo) =>
        symbolInfo && symbolInfo.symbol
      )
    );

    logger.info(
      {
        total: allSymbols.length
      },
      'poller.initialScan: symbols persisted'
    );

    const seedSymbols = bybit.getSeedSymbols(allSymbols);

    if (
      seedSymbols &&
      seedSymbols.length > 0
    ) {
      const invalidSymbols = seedSymbols.filter((seedSymbol) => {
        const symbol = String(seedSymbol.symbol || '').toUpperCase();

        if (/USDT[QHUZ0-9]/.test(symbol.slice(-6))) {
          return true;
        }

        return !/USDT(\.P)?$/.test(symbol);
      });

      if (invalidSymbols.length > 0) {
        logger.error(
          {
            count: invalidSymbols.length,
            samples: invalidSymbols.slice(0, 5).map((item) => item.symbol)
          },
          'poller.initialScan: invalid symbols in seed list'
        );
      }

      if (seed) {
        setImmediate(() => {
          this.backgroundSeedKlines(seedSymbols)
            .catch((err) => {
              logger.debug(
                { err },
                'poller.initialScan: background seeding failed'
              );
            });
        });
      } else {
        logger.debug(
          'poller.initialScan: background seeding disabled'
        );
      }
    } else {
      logger.info(
        'poller.initialScan: no seed symbols to process'
      );
    }

    return allSymbols;
  },

  async performWsInitialScan() {
    try {
      const wsManager = require('./bybitWs');

      if (
        wsManager &&
        typeof wsManager.performInitialScan === 'function'
      ) {
        const result = await wsManager.performInitialScan();
        return Array.isArray(result) ? result : [];
      }
    } catch (err) {
      logger.debug(
        { err },
        'poller.performWsInitialScan failed'
      );
    }

    return [];
  },

  async backgroundSeedKlines(symbols = []) {
    if (
      !Array.isArray(symbols) ||
      symbols.length === 0
    ) {
      logger.info(
        'backgroundSeedKlines: nothing to seed'
      );

      return;
    }

    logger.info(
      {
        count: symbols.length,
        concurrency: SEED_CONCURRENCY
      },
      'backgroundSeedKlines: starting'
    );

    for (
      let i = 0;
      i < symbols.length;
      i += SEED_CONCURRENCY
    ) {
      const batch = symbols.slice(i, i + SEED_CONCURRENCY);

      const jobs = batch.map((item) =>
        limiter.schedule(() =>
          this.seedKlinesForSymbol(item.symbol)
        )
      );

      try {
        await Promise.all(jobs);
      } catch (err) {
        logger.debug(
          { err },
          'backgroundSeedKlines: batch failed; continuing'
        );
      }
    }

    logger.info(
      'backgroundSeedKlines: completed'
    );
  },

  async seedKlinesForSymbol(symbol, timeframe = null) {
    try {
      const symbolUpper = String(symbol || '').toUpperCase();

      if (!isUsdtSymbol(symbol)) {
        logger.warn(
          { symbol },
          'seedKlinesForSymbol: invalid USDT symbol; skipping'
        );

        return;
      }

      if (
        /USDT[QHUZ0-9]/.test(
          symbolUpper.slice(-6)
        )
      ) {
        logger.warn(
          { symbol },
          'seedKlinesForSymbol: dated variant; skipping'
        );

        return;
      }

      const rootTfs = timeframe
        ? [String(timeframe)]
        : (
            Array.isArray(config.ROOT_TFS)
              ? config.ROOT_TFS
              : buildRootTfs()
          );

      const mtfTfs = Array.isArray(config.MTF_TFS)
        ? config.MTF_TFS.map(String)
        : [];

      const timeframes = Array.from(
        new Set([
          ...rootTfs,
          ...mtfTfs
        ])
      );

      for (const tf of timeframes) {
        const interval =
          normalizeRootTf(tf) === 'D'
            ? 'D'
            : String(tf);

        try {
          const klines = await limiter.schedule(() =>
            bybit.fetchKlines(
              symbol,
              interval,
              config.SEED_KLINES_LIMIT
            )
          );

          if (!klines || klines.length === 0) {
            logger.debug(
              { symbol, tf },
              'seedKlinesForSymbol: no klines returned'
            );

            continue;
          }

          const db = dbModule.get();

          const insert = db.prepare(
            `
              INSERT OR IGNORE INTO klines
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
            `
          );

          const insertMany = db.transaction((rows) => {
            for (const kline of rows) {
              insert.run(
                symbol,
                tf,
                kline.open_time,
                kline.open,
                kline.high,
                kline.low,
                kline.close,
                kline.volume
              );
            }
          });

          insertMany(klines);

          logger.debug(
            {
              symbol,
              tf,
              count: klines.length
            },
            'seedKlinesForSymbol: klines persisted'
          );

          try {
            if (
              typeof macdUtil.computeAndStoreMacd === 'function'
            ) {
              await macdUtil.computeAndStoreMacd(symbol, tf);
            } else if (
              typeof macdUtil.computeMacdHistogram === 'function'
            ) {
              await macdUtil.computeMacdHistogram(symbol, tf);
            }
          } catch (err) {
            logger.debug(
              {
                err,
                symbol,
                tf
              },
              'seedKlinesForSymbol: MACD warm-up failed'
            );
          }
        } catch (err) {
          logger.debug(
            {
              err,
              symbol,
              tf
            },
            'seedKlinesForSymbol: timeframe fetch failed'
          );
        }
      }
    } catch (err) {
      logger.debug(
        {
          err,
          symbol,
          timeframe
        },
        'seedKlinesForSymbol: unexpected error'
      );
    }
  },

  async scanAllForStartup() {
    if (startupComplete) {
      logger.debug(
        'scanAllForStartup: startup scan already completed'
      );

      return [];
    }

    logger.info(
      'scanAllForStartup: starting full startup pass'
    );

    try {
      const db = dbModule.get();

      const rows = db
        .prepare(
          `
            SELECT symbol
            FROM symbols
            ORDER BY symbol COLLATE NOCASE ASC
          `
        )
        .all();

      const validRows = rows.filter((row) =>
        isUsdtSymbol(row.symbol)
      );

      const invalidCount = rows.length - validRows.length;

      if (invalidCount > 0) {
        logger.warn(
          {
            invalidCount
          },
          'scanAllForStartup: invalid symbols skipped'
        );
      }

      const newSignals = [];
      const pageSize = Number(config.PAGE_SIZE || 25);

      for (
        let i = 0;
        i < validRows.length;
        i += pageSize
      ) {
        const page = validRows.slice(i, i + pageSize);

        const results = await Promise.all(
          page.map((row) =>
            this.scanSymbolRoots(row.symbol)
          )
        );

        newSignals.push(...results.flat());
      }

      newSignals.sort((a, b) => {
        const symbolA = String(a.symbol || '').toUpperCase();
        const symbolB = String(b.symbol || '').toUpperCase();

        return symbolA.localeCompare(symbolB);
      });

      if (newSignals.length > 0) {
        logger.info(
          {
            count: newSignals.length
          },
          'scanAllForStartup: enqueueing startup notification batch'
        );

        notificationQueue.enqueueStartupBatch(newSignals);
      } else {
        logger.info(
          'scanAllForStartup: no startup signals found'
        );
      }

      this.initializeBoundaryCandleState(validRows, db);

      startupComplete = true;

      logger.info(
        'scanAllForStartup: completed'
      );

      return newSignals;
    } catch (err) {
      logger.error(
        {
          err
        },
        'scanAllForStartup: unexpected error'
      );

      return [];
    }
  },

  initializeBoundaryCandleState(rows, db) {
    const rootTfs = buildRootTfs();

    for (const row of rows) {
      const symbol = row.symbol;

      for (const tf of rootTfs) {
        const latestOpen = getLatestOpenTime(db, symbol, tf);

        if (latestOpen === null) {
          continue;
        }

        dbModule.setState(
          this.getLoop2ProcessedCandleKey(symbol, tf),
          latestOpen
        );
      }
    }

    logger.info(
      {
        symbols: rows.length,
        timeframes: rootTfs.length
      },
      'poller: initialized boundary candle state'
    );
  },

  async scanSymbolRoots(symbol) {
    const rootTfs = buildRootTfs();
    const results = [];

    if (!isUsdtSymbol(symbol)) {
      logger.warn(
        {
          symbol
        },
        'scanSymbolRoots: invalid USDT symbol; skipping'
      );

      return results;
    }

    for (const tf of rootTfs) {
      try {
        const db = dbModule.get();

        const selectStatement = db.prepare(
          `
            SELECT open_time, close, open
            FROM klines
            WHERE symbol = ?
              AND timeframe = ?
            ORDER BY open_time DESC
            LIMIT 2
          `
        );

        let rows = selectStatement.all(symbol, tf);

        if (!rows || rows.length < 2) {
          logger.debug(
            {
              symbol,
              tf
            },
            'scanSymbolRoots: insufficient klines; seeding'
          );

          await this.seedKlinesForSymbol(symbol, tf);

          rows = selectStatement.all(symbol, tf);

          if (!rows || rows.length < 2) {
            logger.debug(
              {
                symbol,
                tf
              },
              'scanSymbolRoots: still insufficient klines'
            );

            continue;
          }
        }

        const flip = await macdUtil.isMacdFlip(symbol, tf);

        if (!flip) {
          continue;
        }

        const signal = await signalManager.handleRootSignal({
          symbol,
          root_tf: tf,
          detected_at: Date.now(),
          candle_open_time: Number(rows[0].open_time),
          notifyImmediately: false,
          notificationType: 'startup'
        });

        if (signal) {
          results.push(signal);
        }
      } catch (err) {
        logger.debug(
          {
            err,
            symbol,
            tf
          },
          'scanSymbolRoots: error checking flip'
        );
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
    setImmediate(() => {
      this.runBoundaryScanLoop()
        .catch((err) => {
          logger.error(
            {
              err
            },
            'poller: boundary scan loop crashed'
          );
        });
    });
  },

  async runBoundaryScanLoop() {
    while (isRunning) {
      const nextBoundary = this.getNextFiveMinuteBoundaryMs();

      const delay = Math.max(
        0,
        nextBoundary - Date.now()
      );

      logger.debug(
        {
          nextBoundary: new Date(nextBoundary).toISOString(),
          delayMs: delay
        },
        'poller.loop2: waiting for next five-minute boundary'
      );

      await sleep(delay);

      if (!isRunning) {
        break;
      }

      if (boundaryScanInProgress) {
        logger.warn(
          'poller.loop2: previous boundary scan is still running; skipping boundary'
        );

        continue;
      }

      boundaryScanInProgress = true;

      try {
        await this.runBoundaryScanOnce();
      } catch (err) {
        logger.error(
          {
            err
          },
          'poller.loop2: boundary scan failed'
        );
      } finally {
        boundaryScanInProgress = false;
      }
    }
  },

  async runBoundaryScanOnce() {
    const boundary = new Date();

    logger.info(
      {
        boundary: boundary.toISOString()
      },
      'poller.loop2: starting exact five-minute boundary scan'
    );

    await this.initialScan({ seed: false });

    const db = dbModule.get();

    const rows = db
      .prepare(
        `
          SELECT symbol
          FROM symbols
          ORDER BY symbol COLLATE NOCASE ASC
        `
      )
      .all();

    const validRows = rows.filter((row) =>
      isUsdtSymbol(row.symbol)
    );

    const rootTfs = buildRootTfs();
    const newSignals = [];
    const alignmentAlerts = [];

    for (const row of validRows) {
      const symbol = row.symbol;

      for (const tf of rootTfs) {
        try {
          await this.seedKlinesForSymbol(symbol, tf);

          const latestOpen = getLatestOpenTime(db, symbol, tf);

          if (latestOpen === null) {
            continue;
          }

          const processedStateKey = this.getLoop2ProcessedCandleKey(symbol, tf);

          const processedOpen = Number(
            dbModule.getState(processedStateKey) || 0
          );

          if (latestOpen > processedOpen) {
            const flip = await macdUtil.isMacdFlip(symbol, tf);

            if (flip) {
              const signal = await signalManager.handleRootSignal({
                symbol,
                root_tf: tf,
                detected_at: Date.now(),
                candle_open_time: latestOpen,
                notifyImmediately: false,
                notificationType: 'new_root_candle'
              });

              if (signal) {
                signal.notificationType = 'new_root_candle';
                newSignals.push(signal);
              }
            }

            dbModule.setState(processedStateKey, latestOpen);

            logger.debug(
              {
                symbol,
                tf,
                latestOpen
              },
              'poller.loop2: processed newly opened root candle'
            );
          }

          const latestSignals = dbModule.getLatestSignalsSnapshot();

          const activeSignals = latestSignals.filter((signal) => {
            return (
              signal.symbol === symbol &&
              signal.root_tf === tf
            );
          });

          if (activeSignals.length === 0) {
            continue;
          }

          const alignment = await signalManager.evaluateMtfAlignment(symbol);

          const alignmentStateKey = `poller.loop2.alignment.${symbol}.${tf}`;

          const previousAlignment = dbModule.getState(alignmentStateKey);

          const nextAlignmentJson = JSON.stringify(alignment || {});

          if (previousAlignment === nextAlignmentJson) {
            continue;
          }

          dbModule.setState(alignmentStateKey, alignment || {});

          const alignmentValues = Object.values(alignment || {});

          const positiveCount = alignmentValues.filter((value) => {
            return value && value.positive;
          }).length;

          const alignmentCount = alignmentValues.length;

          alignmentAlerts.push({
            symbol,
            root_tf: tf,
            detected_at: Date.now(),
            state: 'monitor',
            notificationType: 'mtf_alignment',
            meta: {
              alignment: alignment || {},
              decision: 'monitor',
              acceptReason: 'mtf_alignment_alert',
              tvScore: 0,
              tvSource: 'loop2',
              mtfScore: alignmentCount > 0 ? positiveCount / alignmentCount : 0
            }
          });
        } catch (err) {
          logger.debug(
            {
              err,
              symbol,
              tf
            },
            'poller.loop2: symbol/timeframe scan failed'
          );
        }
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
    }

    for (const alert of alignmentAlerts) {
      try {
        notificationQueue.enqueueSignal(alert, 'realtime');

        logger.info(
          {
            symbol: alert.symbol,
            root_tf: alert.root_tf
          },
          'poller.loop2: realtime alignment alert enqueued'
        );
      } catch (err) {
        logger.warn(
          {
            err,
            alert
          },
          'poller.loop2: failed to enqueue alignment alert'
        );
      }
    }

    logger.info(
      {
        newSignals: newSignals.length,
        alignmentAlerts: alignmentAlerts.length
      },
      'poller.loop2: exact five-minute boundary scan completed'
    );
  },

  getLoop2ProcessedCandleKey(symbol, tf) {
    return `poller.loop2.lastRootOpen.${symbol}.${tf}`;
  }
};
