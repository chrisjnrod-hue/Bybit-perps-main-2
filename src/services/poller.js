// src/services/poller.js
const dbModule = require('../db');
const bybit = require('./bybitRest');
const config = require('../config');
const logger = require('pino')();
const Bottleneck = require('bottleneck');
const macdUtil = require('./macd');
const signalManager = require('./signalManager');
const notificationQueue = require('./notificationQueue');

const limiter = new Bottleneck({ minTime: 50 });
const SEED_CONCURRENCY = Number(config.SEED_CONCURRENCY || 6);

let isRunning = false;
let startupComplete = false;

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms || 0));
}

function normalizeRootTf(tf) {
  if (tf === null || tf === undefined) return null;
  const value = String(tf).trim().toUpperCase();
  if (value === '1D' || value === 'D') return 'D';
  if (value === '1H' || value === 'H') return '60';
  return value;
}

function buildRootTfs() {
  const raw = Array.isArray(config.ROOT_TFS) ? config.ROOT_TFS : ['60', '240', 'D'];
  const seen = new Set();
  const out = [];

  for (const tf of raw) {
    const norm = normalizeRootTf(tf);
    if (!norm || seen.has(norm)) continue;
    seen.add(norm);
    out.push(norm);
  }

  return out.length ? out : ['60', '240', 'D'];
}

function isUsdtSymbol(symbol) {
  const s = String(symbol || '').toUpperCase();
  if (!s) return false;
  if (/USDT[QHUZ0-9]/.test(s.slice(-6))) return false;
  return /USDT(\.P)?$/.test(s);
}

module.exports = {
  start() {
    if (isRunning) return;
    isRunning = true;
    startupComplete = false;

    try { signalManager.setOpenTradesAllowed(false); } catch (e) { /* ignore */ }

    try {
      bybit.probeHosts(3000)
        .then((base) => {
          if (base) logger.info({ base }, 'probeHosts completed in background');
          else logger.warn('probeHosts completed in background with no selected base');
        })
        .catch((e) => logger.debug({ e }, 'probeHosts background failure'));
    } catch (e) {
      logger.debug({ e }, 'probeHosts startup call failed');
    }

    (async () => {
      try {
        logger.info('poller: starting initialScan');
        await this.initialScan();
        logger.info('poller: initialScan completed');

        try {
          await this.scanAllForStartup();
          logger.info('poller: startup full scan completed');
        } catch (err) {
          logger.error({ err }, 'poller: scanAllForStartup error');
        }

        try {
          signalManager.setOpenTradesAllowed(true);
          logger.info('poller: open trades enabled after initial scan');
        } catch (e) {
          logger.debug({ e }, 'poller: failed to enable open trades');
        }

        // LOOP 2: 5m boundary scan
        this.startBoundaryScanLoop();

        // LOOP 3: exact root candle open scan
        this.startRootCandleOpenLoop();
      } catch (err) {
        logger.error({ err }, 'poller: initialScan failed');
      }
    })();
  },

  async initialScan() {
    logger.info('poller.initialScan: starting');

    let allSymbols = [];
    const useWs = !!config.USE_WS;

    if (useWs) {
      try {
        const wsTimeoutMs = config.WS_INITIAL_SCAN_TIMEOUT || 10000;
        logger.info({ timeoutMs: wsTimeoutMs }, 'poller: attempting WS initial scan');
        allSymbols = await Promise.race([
          this.performWsInitialScan(),
          new Promise((_, reject) => setTimeout(() => reject(new Error('WS scan timeout')), wsTimeoutMs))
        ]);
        if (!Array.isArray(allSymbols) || allSymbols.length === 0) {
          logger.warn('poller: WS initial scan returned no symbols; will fallback to REST');
          allSymbols = [];
        } else {
          logger.info({ count: allSymbols.length }, 'poller: WS initial scan provided symbols');
        }
      } catch (e) {
        logger.debug({ e }, 'poller: WS initial scan failed or timed out; fallback to REST');
        allSymbols = [];
      }
    }

    if (!allSymbols || allSymbols.length === 0) {
      logger.info('poller: fetching symbols via REST (cursor pagination for USDT perpetuals)');
      allSymbols = await bybit.fetchAllSymbols();
    }

    if (!Array.isArray(allSymbols) || allSymbols.length === 0) {
      logger.warn('poller.initialScan: no symbols discovered');
      return;
    }

    const db = dbModule.get();
    const insert = db.prepare('INSERT OR REPLACE INTO symbols (symbol, base, quote, fetched_at) VALUES (?, ?, ?, ?)');
    const now = Date.now();
    const insertMany = db.transaction((rows) => {
      for (const s of rows) {
        insert.run(s.symbol, s.base || s.symbol.replace(/USDT(\.P)?$/i, ''), s.quote || 'USDT', now);
      }
    });
    insertMany(allSymbols.filter(s => s && s.symbol));
    logger.info({ total: allSymbols.length }, 'poller.initialScan: symbols persisted (USDT perpetuals only)');

    const seedSymbols = bybit.getSeedSymbols(allSymbols);
    if (seedSymbols && seedSymbols.length) {
      const invalidSymbols = seedSymbols.filter(s => {
        const sym = String(s.symbol || '').toUpperCase();
        if (/USDT[QHUZ0-9]/.test(sym.slice(-6))) return true;
        return !/USDT(\.P)?$/.test(sym);
      });

      if (invalidSymbols.length > 0) {
        logger.error(
          { count: invalidSymbols.length, samples: invalidSymbols.slice(0, 5).map(s => s.symbol) },
          'poller.initialScan: CRITICAL - invalid symbols in seed list! These should have been filtered.'
        );
      }

      setImmediate(() => this.backgroundSeedKlines(seedSymbols));
    } else {
      logger.info('poller.initialScan: no seed symbols to process (SYMBOL_SEED_ALL disabled or none)');
    }
  },

  async performWsInitialScan() {
    try {
      const wsManager = require('./bybitWs');
      if (wsManager && typeof wsManager.performInitialScan === 'function') {
        const res = await wsManager.performInitialScan();
        return Array.isArray(res) ? res : [];
      }
    } catch (e) {
      logger.debug({ e }, 'performWsInitialScan failed');
    }
    return [];
  },

  async backgroundSeedKlines(symbols = []) {
    if (!Array.isArray(symbols) || symbols.length === 0) {
      logger.info('backgroundSeedKlines: nothing to seed');
      return;
    }
    logger.info({ count: symbols.length, concurrency: SEED_CONCURRENCY }, 'backgroundSeedKlines: starting');

    for (let i = 0; i < symbols.length; i += SEED_CONCURRENCY) {
      const batch = symbols.slice(i, i + SEED_CONCURRENCY);
      const jobs = batch.map(s => limiter.schedule(() => this.seedKlinesForSymbol(s.symbol)));
      try {
        await Promise.all(jobs);
      } catch (e) {
        logger.debug({ e }, 'backgroundSeedKlines: batch failed (continuing)');
      }
    }
    logger.info('backgroundSeedKlines: completed');
  },

  async seedKlinesForSymbol(symbol, timeframe = null) {
    try {
      const symUpper = String(symbol || '').toUpperCase();
      if (!isUsdtSymbol(symbol)) {
        logger.warn({ symbol }, 'seedKlinesForSymbol: symbol is not valid USDT, skipping');
        return;
      }

      if (/USDT[QHUZ0-9]/.test(symUpper.slice(-6))) {
        logger.warn({ symbol }, 'seedKlinesForSymbol: symbol is a dated variant, skipping');
        return;
      }

      const rootTfs = timeframe ? [String(timeframe)] : (config.ROOT_TFS || []);
      const mtfTfs = Array.isArray(config.MTF_TFS) ? config.MTF_TFS.map(String) : [];
      const tfsSet = new Set([...(rootTfs || []), ...(mtfTfs || [])]);
      const tfs = Array.from(tfsSet);

      for (const tf of tfs) {
        const interval = normalizeRootTf(tf) === 'D' ? 'D' : String(tf);
        try {
          const klines = await limiter.schedule(() => bybit.fetchKlines(symbol, interval, config.SEED_KLINES_LIMIT));
          if (!klines || klines.length === 0) {
            logger.debug({ symbol, tf }, 'seedKlinesForSymbol: no klines returned from API');
            continue;
          }

          const db = dbModule.get();
          const insert = db.prepare('INSERT OR IGNORE INTO klines (symbol, timeframe, open_time, open, high, low, close, volume) VALUES (?, ?, ?, ?, ?, ?, ?, ?)');
          const insertMany = db.transaction((rows) => {
            for (const k of rows) {
              insert.run(symbol, tf, k.open_time, k.open, k.high, k.low, k.close, k.volume);
            }
          });
          insertMany(klines);
          logger.debug({ symbol, tf, count: klines.length }, 'seedKlinesForSymbol: klines persisted');

          try {
            if (typeof macdUtil.computeAndStoreMacd === 'function') {
              await macdUtil.computeAndStoreMacd(symbol, tf);
            } else if (typeof macdUtil.computeMacdHistogram === 'function') {
              await macdUtil.computeMacdHistogram(symbol, tf);
            }
          } catch (err) {
            logger.debug({ err, symbol, tf }, 'seedKlinesForSymbol: macd warm-up failed (continuing)');
          }
        } catch (err) {
          logger.debug({ err, symbol, tf }, 'seedKlinesForSymbol: fetch failed (skipping tf)');
        }
      }
    } catch (err) {
      logger.debug({ err, symbol, timeframe }, 'seedKlinesForSymbol: unexpected error');
    }
  },

  async scanAllForStartup() {
    if (startupComplete) {
      logger.info('scanAllForStartup: already completed, skipping');
      return;
    }

    try {
      logger.info('scanAllForStartup: starting full startup pass');
      const db = dbModule.get();
      const rows = db.prepare('SELECT symbol FROM symbols ORDER BY symbol COLLATE NOCASE ASC').all();

      const invalidSymbols = rows.filter(r => {
        const sym = String(r.symbol || '').toUpperCase();
        if (/USDT[QHUZ0-9]/.test(sym.slice(-6))) return true;
        return !isUsdtSymbol(r.symbol);
      });

      if (invalidSymbols.length > 0) {
        logger.warn(
          { count: invalidSymbols.length, samples: invalidSymbols.slice(0, 5).map(r => r.symbol) },
          'scanAllForStartup: WARNING - database contains invalid symbols! These will be skipped.'
        );
      }

      const newSignals = [];

      for (let i = 0; i < rows.length; i += config.PAGE_SIZE) {
        const page = rows.slice(i, i + config.PAGE_SIZE);
        const tasks = page
          .filter(r => isUsdtSymbol(r.symbol))
          .map(r => this.scanSymbolRoots(r.symbol));

        try {
          const results = await Promise.all(tasks);
          newSignals.push(...results.flat());
        } catch (e) {
          logger.debug({ e }, 'scanAllForStartup: page tasks error (continuing)');
        }
      }

      newSignals.sort((a, b) => {
        const symA = String(a.symbol || '').toUpperCase();
        const symB = String(b.symbol || '').toUpperCase();
        return symA.localeCompare(symB);
      });

      if (newSignals.length > 0) {
        logger.info({ newSignals: newSignals.length }, 'scanAllForStartup: enqueuing startup batch to notification queue');
        notificationQueue.enqueueStartupBatch(newSignals);
      } else {
        logger.info('scanAllForStartup: no new signals found');
      }

      startupComplete = true;
      logger.info('scanAllForStartup: completed full startup pass (USDT perpetuals only)');
    } catch (err) {
      logger.error({ err }, 'scanAllForStartup: unexpected error');
    }
  },

  async scanSymbolRoots(symbol) {
    const tfList = buildRootTfs();
    const results = [];

    if (!isUsdtSymbol(symbol)) {
      logger.warn({ symbol }, 'scanSymbolRoots: symbol is not valid USDT, skipping');
      return results;
    }

    for (const tf of tfList) {
      try {
        const db = dbModule.get();
        const selectStmt = db.prepare('SELECT open_time, close, open FROM klines WHERE symbol=? AND timeframe=? ORDER BY open_time DESC LIMIT 2');
        let rows = selectStmt.all(symbol, tf);

        if (!rows || rows.length < 2) {
          logger.debug({ symbol, tf }, 'scanSymbolRoots: insufficient klines, seeding now');
          await this.seedKlinesForSymbol(symbol, tf);

          rows = selectStmt.all(symbol, tf);
          if (!rows || rows.length < 2) {
            logger.debug({ symbol, tf }, 'scanSymbolRoots: still insufficient klines after seeding, skipping tf');
            continue;
          }
        }

        const flip = await require('./macd').isMacdFlip(symbol, tf);
        if (flip) {
          const sig = await require('./signalManager').handleRootSignal({
            symbol,
            root_tf: tf,
            detected_at: Date.now(),
            notifyImmediately: false
          });
          if (sig) results.push(sig);
        }
      } catch (err) {
        logger.debug({ err, symbol, tf }, 'scanSymbolRoots: error checking flip');
      }
    }

    return results;
  },

  // ===========================================================
  // LOOP 2: 5-minute boundary scan.
  // Behavior:
  // - refresh / seed same as loop 1
  // - check new root signals
  // - alert only on new root signals and MTF alignment changes
  // - send only one signal block per signal
  // - NO summary, NO recommended blocks
  // ===========================================================
  startBoundaryScanLoop() {
    setImmediate(() => {
      this.runBoundaryScanLoop().catch((err) => {
        logger.error({ err }, 'poller: boundary scan loop crashed');
      });
    });
  },

  async runBoundaryScanLoop() {
    while (isRunning) {
      try {
        // Refresh/seed data like initial deploy before scanning
        await this.initialScan();

        const db = dbModule.get();
        const rows = db.prepare('SELECT symbol FROM symbols ORDER BY symbol COLLATE NOCASE ASC').all();
        const validRows = rows.filter(r => isUsdtSymbol(r.symbol));
        const rootTfs = buildRootTfs();

        const seenState = dbModule.getState('poller.loop2.seen') || {};
        const seen = new Map(Object.entries(seenState));

        const newSignals = [];
        const alignmentAlerts = [];

        for (const row of validRows) {
          const symbol = row.symbol;

          for (const tf of rootTfs) {
            const key = `${symbol}:${tf}`;

            try {
              // Always seed fresh root data before scanning for this symbol/tf
              await this.seedKlinesForSymbol(symbol, tf);

              // Only process a new signal per symbol+tf once in this loop
              if (!seen.has(key)) {
                const flip = await require('./macd').isMacdFlip(symbol, tf);
                if (flip) {
                  const sig = await signalManager.handleRootSignal({
                    symbol,
                    root_tf: tf,
                    detected_at: Date.now(),
                    notifyImmediately: false
                  });

                  if (sig) {
                    newSignals.push(sig);
                    seen.set(key, Date.now());
                  } else {
                    seen.set(key, Date.now());
                  }
                } else {
                  seen.set(key, Date.now());
                }
              }

              // MTF alignment alerts for monitored/active signals
              const latestSignals = dbModule.getLatestSignalsSnapshot();
              const active = latestSignals.filter(s => s.symbol === symbol && s.root_tf === tf);

              if (active.length > 0) {
                const alignment = await signalManager.evaluateMtfAlignment(symbol);
                const alignmentStateKey = `poller.loop2.alignment.${symbol}.${tf}`;
                const prevAlignment = dbModule.getState(alignmentStateKey);

                const nextAlignmentJson = JSON.stringify(alignment || {});
                if (prevAlignment !== nextAlignmentJson) {
                  dbModule.setState(alignmentStateKey, alignment || {});
                  alignmentAlerts.push({
                    symbol,
                    root_tf: tf,
                    detected_at: Date.now(),
                    state: 'monitor',
                    meta: {
                      alignment: alignment || {},
                      decision: 'monitor',
                      acceptReason: 'mtf_alignment_alert',
                      tvScore: 0,
                      tvSource: 'loop2',
                      mtfScore: Object.keys(alignment || {}).length
                        ? (Object.values(alignment || {}).filter(v => v && v.positive).length / Object.keys(alignment || {}).length)
                        : 0
                    }
                  });
                }
              }
            } catch (e) {
              logger.debug({ e, symbol, tf }, 'poller.loop2: root check failed');
            }
          }
        }

        dbModule.setState('poller.loop2.seen', Object.fromEntries(seen));

        // IMPORTANT: loop 2 sends only single-signal telegram blocks
        for (const sig of newSignals) {
          try {
            notificationQueue.enqueueSignal(sig, 'realtime');
            logger.info({ symbol: sig.symbol, root_tf: sig.root_tf }, 'poller.loop2: enqueued new root signal block');
          } catch (e) {
            logger.warn({ e, sig }, 'poller.loop2: failed to enqueue new root signal');
          }
        }

        for (const al of alignmentAlerts) {
          try {
            notificationQueue.enqueueSignal(al, 'realtime');
            logger.info({ symbol: al.symbol, root_tf: al.root_tf }, 'poller.loop2: enqueued MTF alignment alert block');
          } catch (e) {
            logger.warn({ e, al }, 'poller.loop2: failed to enqueue alignment alert');
          }
        }
      } catch (err) {
        logger.error({ err }, 'poller.loop2: unexpected error');
      }

      await sleep(300000); // 5 minutes
    }
  },

  // ===========================================================
  // LOOP 3: precise root candle open scan.
  // Behavior:
  // - exact root candle open detection for 60 / 240 / D
  // - only scans on relevant open boundaries
  // - when a relevant signal appears, uses the existing full
  //   startup summary layout (summary + per-signal blocks + recommended blocks)
  // ===========================================================
  startRootCandleOpenLoop() {
    setImmediate(() => {
      this.runRootCandleOpenLoop().catch((err) => {
        logger.error({ err }, 'poller: root candle open loop crashed');
      });
    });
  },

  getNextRootCandleOpenMs(tf) {
    const now = new Date();
    const ts = now.getTime();

    if (normalizeRootTf(tf) === 'D') {
      const next = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() + 1, 0, 0, 0, 0));
      return next.getTime();
    }

    if (normalizeRootTf(tf) === '60') {
      const next = new Date(now);
      next.setUTCMinutes(0, 0, 0);
      next.setUTCHours(next.getUTCHours() + 1);
      return next.getTime();
    }

    if (normalizeRootTf(tf) === '240') {
      const next = new Date(now);
      const currentHour = next.getUTCHours();
      const alignedHour = Math.floor(currentHour / 4) * 4;
      const candidate = new Date(Date.UTC(next.getUTCFullYear(), next.getUTCMonth(), next.getUTCDate(), alignedHour + 4, 0, 0, 0));
      if (candidate.getTime() <= ts) {
        const shifted = new Date(Date.UTC(next.getUTCFullYear(), next.getUTCMonth(), next.getUTCDate() + 1, 0, 0, 0, 0));
        return shifted.getTime();
      }
      return candidate.getTime();
    }

    return ts + 60000;
  },

  getProcessedCandleKey(symbol, tf) {
    return `poller.loop3.lastRootOpen.${symbol}.${tf}`;
  },

  async runRootCandleOpenLoop() {
    while (isRunning) {
      try {
        const rootTfs = buildRootTfs();
        const db = dbModule.get();
        const symbols = db.prepare('SELECT symbol FROM symbols ORDER BY symbol COLLATE NOCASE ASC').all();

        const candidateSignals = [];

        for (const symbolRow of symbols) {
          const symbol = symbolRow.symbol;
          if (!isUsdtSymbol(symbol)) continue;

          for (const tf of rootTfs) {
            try {
              // Refresh seed data before checking exact open
              await this.seedKlinesForSymbol(symbol, tf);

              const latestRows = db.prepare(
                'SELECT open_time, close, open FROM klines WHERE symbol=? AND timeframe=? ORDER BY open_time DESC LIMIT 2'
              ).all(symbol, tf);

              if (!latestRows || latestRows.length < 2) continue;

              const latestOpen = Number(latestRows[0].open_time);
              const processedOpen = Number(dbModule.getState(this.getProcessedCandleKey(symbol, tf)) || 0);

              // Only scan when the newest root candle is newer than the last processed one
              if (latestOpen > processedOpen) {
                const flip = await require('./macd').isMacdFlip(symbol, tf);
                if (flip) {
                  const sig = await signalManager.handleRootSignal({
                    symbol,
                    root_tf: tf,
                    detected_at: Date.now(),
                    notifyImmediately: false
                  });

                  if (sig) {
                    candidateSignals.push(sig);
                  }
                }

                dbModule.setState(this.getProcessedCandleKey(symbol, tf), latestOpen);
              }
            } catch (e) {
              logger.debug({ e, symbol, tf }, 'poller.loop3: root candle check failed');
            }
          }
        }

        // Full startup-style telegram output, but only for relevant open tf(s)
        if (candidateSignals.length > 0) {
          notificationQueue.enqueueStartupBatch(candidateSignals);
          logger.info(
            { count: candidateSignals.length },
            'poller.loop3: enqueued full startup-style summary/recommended telegram flow'
          );
        }
      } catch (err) {
        logger.error({ err }, 'poller.loop3: unexpected error');
      }

      await sleep(60000);
    }
  }
};
