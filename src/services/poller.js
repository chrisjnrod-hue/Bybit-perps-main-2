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

function validUsdtSymbol(symbol) {
  if (!symbol || typeof symbol !== 'string') return false;
  const sym = symbol.toUpperCase();
  return /USDT(\.P)?$/.test(sym) && !/USDT[QHUZ0-9]/.test(sym.slice(-6));
}

function normalizeTf(tf) {
  const value = String(tf || '').trim();
  if (/^(D|1D|1d|day)$/i.test(value)) return 'D';
  return value;
}

function timeframeMs(tf) {
  const normalized = normalizeTf(tf);

  if (normalized === 'D') return 24 * 60 * 60 * 1000;
  if (normalized === 'W') return 7 * 24 * 60 * 60 * 1000;
  if (normalized === 'M') return 30 * 24 * 60 * 60 * 1000;

  const minutes = Number(normalized);
  return Number.isFinite(minutes) && minutes > 0
    ? minutes * 60 * 1000
    : null;
}

function currentCandleOpenTime(tf, now = Date.now()) {
  const duration = timeframeMs(tf);
  if (!duration) return null;
  return Math.floor(now / duration) * duration;
}

function stableAlignmentSignature(alignment = {}) {
  const out = {};
  for (const key of Object.keys(alignment).sort()) {
    const info = alignment[key] || {};
    out[key] = info.positive ? 'positive' : (info.ok ? 'negative' : 'unknown');
  }
  return JSON.stringify(out);
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

        if (config.ROOT_SCAN_5MIN_BOUNDARY) {
          this.startBoundaryScanLoop();
        } else {
          logger.info('poller: ROOT_SCAN_5MIN_BOUNDARY disabled; boundary loop skipped');
        }

        if (config.ROOT_CANDLE_OPEN_SCAN_ENABLED) {
          this.startRootCandleOpenScanLoop();
        } else {
          logger.info('poller: ROOT_CANDLE_OPEN_SCAN_ENABLED disabled; root open scan loop skipped');
        }
      } catch (err) {
        logger.error({ err }, 'poller: initialScan failed');
      }
    })();
  },

  startBoundaryScanLoop() {
    if (this._boundaryScanLoopTimer) return;

    const intervalMs = Number(
      process.env.ROOT_SCAN_5MIN_BOUNDARY_INTERVAL_MS ||
      config.ROOT_SCAN_5MIN_BOUNDARY_INTERVAL_MS ||
      300000
    );

    logger.info({ intervalMs }, 'poller: starting 5m root boundary scan loop');

    const run = async () => {
      if (this._boundaryScanRunning) {
        logger.debug('poller: boundary scan already running; skipping overlapping run');
        return;
      }

      this._boundaryScanRunning = true;

      try {
        await this.runBoundaryScanLoop();
      } catch (err) {
        logger.error({ err }, 'poller: boundary scan loop error');
      } finally {
        this._boundaryScanRunning = false;
      }
    };

    this._boundaryScanLoopTimer = setInterval(run, intervalMs);
    run().catch(() => {});
  },

  startRootCandleOpenScanLoop() {
    if (this._rootCandleOpenLoopTimer) return;

    const intervalMs = Number(
      process.env.ROOT_CANDLE_OPEN_SCAN_INTERVAL_SECS ||
      config.ROOT_CANDLE_OPEN_SCAN_INTERVAL_SECS ||
      10
    ) * 1000;

    logger.info({ intervalMs }, 'poller: starting root candle open scan loop');

    const run = async () => {
      if (this._rootCandleOpenScanRunning) {
        logger.debug('poller: root candle open scan already running; skipping overlap');
        return;
      }

      this._rootCandleOpenScanRunning = true;

      try {
        await this.runRootCandleOpenScanLoop();
      } catch (err) {
        logger.error({ err }, 'poller: root candle open scan loop error');
      } finally {
        this._rootCandleOpenScanRunning = false;
      }
    };

    this._rootCandleOpenLoopTimer = setInterval(run, intervalMs);
    run().catch(() => {});
  },

  async runBoundaryScanLoop() {
    const db = dbModule.get();
    const rows = db.prepare('SELECT symbol FROM symbols ORDER BY symbol COLLATE NOCASE ASC').all();

    const validSymbols = rows
      .map(r => String(r.symbol || '').trim())
      .filter(validUsdtSymbol);

    for (const symbol of validSymbols) {
      for (const tf of (config.ROOT_TFS || []).map(normalizeTf)) {
        try {
          await this.ensureKlinesReady(symbol, tf);

          const key = `boundary_scan:${symbol}:${tf}`;
          const lastSeen = Number(dbModule.getState(key) || 0);
          const now = Date.now();

          if (lastSeen && (now - lastSeen) < 5 * 60 * 1000) {
            continue;
          }

          const detected = await this.checkNewBoundarySignal(symbol, tf);
          if (detected) {
            dbModule.setState(key, now);
          }

          await this.checkMtfAlignmentAlert(symbol, tf);
        } catch (err) {
          logger.debug({ err, symbol, tf }, 'poller: boundary scan symbol iteration failed');
        }
      }
    }
  },

  async checkNewBoundarySignal(symbol, tf) {
    try {
      const normalizedTf = normalizeTf(tf);
      const db = dbModule.get();
      const selectStmt = db.prepare('SELECT open_time, close, open FROM klines WHERE symbol=? AND timeframe=? ORDER BY open_time DESC LIMIT 3');
      const rows = selectStmt.all(symbol, normalizedTf);

      if (!rows || rows.length < 3) {
        await this.seedKlinesForSymbol(symbol, normalizedTf);
        return false;
      }

      const event = await macdUtil.getMacdFlipEvent(symbol, normalizedTf);
      if (!event) return false;

      const eventKey = `root_flip_sent:${event.eventId}`;
      if (dbModule.getState(eventKey)) {
        return false;
      }

      const signalObj = await require('./signalManager').handleRootSignal({
        symbol,
        root_tf: normalizedTf,
        detected_at: Date.now(),
        notifyImmediately: true,
        eventId: event.eventId,
        candleTime: event.candleTime
      });

      if (signalObj) {
        dbModule.setState(eventKey, true);
        logger.info({ symbol, root_tf: normalizedTf, eventId: event.eventId }, 'poller: boundary scan emitted new signal block');
        return true;
      }

      return false;
    } catch (err) {
      logger.debug({ err, symbol, tf }, 'poller: checkNewBoundarySignal error');
      return false;
    }
  },

  async checkMtfAlignmentAlert(symbol, tf) {
    try {
      const normalizedTf = normalizeTf(tf);
      const db = dbModule.get();

      const row = db.prepare(`
        SELECT symbol, root_tf, meta, detected_at
        FROM signals
        WHERE symbol = ? AND root_tf = ?
        ORDER BY detected_at DESC
        LIMIT 1
      `).get(symbol, normalizedTf);

      if (!row) return false;

      let meta = {};
      try { meta = row.meta ? JSON.parse(row.meta) : {}; } catch (e) { meta = {}; }

      const alignment = await signalManager.evaluateMtfAlignment(symbol);
      const newSignature = stableAlignmentSignature(alignment);

      const lastSignature = dbModule.getState(`mtf_align_sig:${symbol}:${normalizedTf}`);
      if (lastSignature && lastSignature === newSignature) {
        return false;
      }

      dbModule.setState(`mtf_align_sig:${symbol}:${normalizedTf}`, newSignature);

      const alertMeta = {
        ...(meta || {}),
        alignment,
        mtfScore: (() => {
          const mtfTfs = Object.keys(alignment || {});
          const positiveCount = mtfTfs.reduce((acc, t) => acc + ((alignment[t] && alignment[t].positive) ? 1 : 0), 0);
          return (mtfTfs.length ? (positiveCount / mtfTfs.length) : 0);
        })(),
        decision: meta.decision || 'monitor',
        acceptReason: 'mtf_alignment_alert'
      };

      const signalObj = {
        key: `${symbol}:${normalizedTf}`,
        symbol,
        root_tf: normalizedTf,
        detected_at: Date.now(),
        state: 'monitor',
        eventId: `mtf_alert_${symbol}_${normalizedTf}_${Date.now()}`,
        candleTime: Date.now(),
        meta: alertMeta
      };

      notificationQueue.enqueueSignal(signalObj, 'realtime');

      logger.info({ symbol, root_tf: normalizedTf, signature: newSignature }, 'poller: MTF alignment alert sent');
      return true;
    } catch (err) {
      logger.debug({ err, symbol, tf }, 'poller: MTF alignment alert error');
      return false;
    }
  },

  async runRootCandleOpenScanLoop() {
    if (!config.ROOT_CANDLE_OPEN_SCAN_ENABLED) {
      logger.debug('poller: root candle open scan disabled');
      return;
    }

    const db = dbModule.get();

    const rows = db.prepare(`
      SELECT symbol
      FROM symbols
      ORDER BY symbol COLLATE NOCASE ASC
    `).all();

    const validSymbols = rows
      .map(r => String(r.symbol || '').trim())
      .filter(validUsdtSymbol);

    const rootTfs = Array.isArray(config.ROOT_TFS)
      ? config.ROOT_TFS.map(normalizeTf)
      : ['60', '240', 'D'];

    const signals = [];

    for (const symbol of validSymbols) {
      for (const tf of rootTfs) {
        const signal = await this.scanRootCandleOpenForSymbol(symbol, tf);
        if (signal) signals.push(signal);
      }
    }

    if (signals.length > 0) {
      logger.info({ signalCount: signals.length }, 'poller: queueing root candle-open summary batch');
      notificationQueue.enqueueStartupBatch(signals);
    }
  },

  async scanRootCandleOpenForSymbol(symbol, tf) {
    const normalizedTf = normalizeTf(tf);

    try {
      const rows = await this.refreshKlinesForSymbol(symbol, normalizedTf);

      if (!rows || rows.length < 3) {
        logger.debug({ symbol, tf: normalizedTf }, 'scanRootCandleOpenForSymbol: insufficient refreshed klines');
        return null;
      }

      const latestOpenTime = Number(rows[0].open_time);
      if (!Number.isFinite(latestOpenTime)) return null;

      const stateKey = `root_open_scan_candle:${symbol}:${normalizedTf}`;
      const previousScannedOpen = Number(dbModule.getState(stateKey) || 0);

      if (previousScannedOpen && latestOpenTime <= previousScannedOpen) {
        return null;
      }

      dbModule.setState(stateKey, latestOpenTime);

      const event = await macdUtil.getMacdFlipEvent(symbol, normalizedTf);
      if (!event) {
        logger.debug({ symbol, tf: normalizedTf, latestOpenTime }, 'scanRootCandleOpenForSymbol: no MACD flip on current candle');
        return null;
      }

      const eventKey = `root_open_signal:${event.eventId}`;
      if (dbModule.getState(eventKey)) {
        logger.debug({ symbol, tf: normalizedTf, eventId: event.eventId }, 'scanRootCandleOpenForSymbol: event already processed');
        return null;
      }

      const signal = await signalManager.handleRootSignal({
        symbol,
        root_tf: normalizedTf,
        detected_at: Date.now(),
        notifyImmediately: false,
        eventId: event.eventId,
        candleTime: event.candleTime
      });

      if (!signal) {
        return null;
      }

      dbModule.setState(eventKey, true);

      logger.info({ symbol, root_tf: normalizedTf, eventId: event.eventId, latestOpenTime }, 'poller: new root candle-open signal detected');
      return signal;
    } catch (err) {
      logger.debug({ err, symbol, tf: normalizedTf }, 'scanRootCandleOpenForSymbol error');
      return null;
    }
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

  async refreshKlinesForSymbol(symbol, timeframe) {
    const normalizedTf = normalizeTf(timeframe);
    const interval = normalizedTf === 'D' ? 'D' : normalizedTf;

    const klines = await limiter.schedule(() =>
      bybit.fetchKlines(symbol, interval, config.SEED_KLINES_LIMIT)
    );

    if (!Array.isArray(klines) || klines.length === 0) {
      logger.debug({ symbol, timeframe: normalizedTf }, 'refreshKlinesForSymbol: no klines returned');
      return [];
    }

    const db = dbModule.get();

    const upsert = db.prepare(`
      INSERT OR REPLACE INTO klines
        (symbol, timeframe, open_time, open, high, low, close, volume)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `);

    const saveMany = db.transaction(rows => {
      for (const k of rows) {
        const openTime = Number(k.open_time);
        if (!Number.isFinite(openTime)) continue;

        upsert.run(
          symbol,
          normalizedTf,
          openTime,
          Number(k.open || 0),
          Number(k.high || 0),
          Number(k.low || 0),
          Number(k.close || 0),
          Number(k.volume || 0)
        );
      }
    });

    saveMany(klines);

    return db.prepare(`
      SELECT open_time, open, high, low, close, volume
      FROM klines
      WHERE symbol = ? AND timeframe = ?
      ORDER BY open_time DESC
      LIMIT ?
    `).all(symbol, normalizedTf, config.SEED_KLINES_LIMIT);
  },

  async ensureKlinesReady(symbol, tf) {
    const normalizedTf = normalizeTf(tf);
    const db = dbModule.get();
    const selectStmt = db.prepare('SELECT open_time, close, open FROM klines WHERE symbol=? AND timeframe=? ORDER BY open_time DESC LIMIT 3');
    let rows = selectStmt.all(symbol, normalizedTf);

    if (!rows || rows.length < 3) {
      await this.seedKlinesForSymbol(symbol, normalizedTf);
      rows = selectStmt.all(symbol, normalizedTf);
    }

    return rows || [];
  },

  async seedKlinesForSymbol(symbol, timeframe = null) {
    try {
      const symUpper = String(symbol || '').toUpperCase();
      if (!/USDT(\.P)?$/.test(symUpper)) {
        logger.warn({ symbol }, 'seedKlinesForSymbol: symbol is not valid USDT, skipping');
        return;
      }

      if (/USDT[QHUZ0-9]/.test(symUpper.slice(-6))) {
        logger.warn({ symbol }, 'seedKlinesForSymbol: symbol is a dated variant, skipping');
        return;
      }

      const rootTfs = timeframe ? [normalizeTf(timeframe)] : (config.ROOT_TFS || []).map(normalizeTf);
      const mtfTfs = Array.isArray(config.MTF_TFS) ? config.MTF_TFS.map(normalizeTf) : [];
      const tfsSet = new Set([...(rootTfs || []), ...(mtfTfs || [])]);
      const tfs = Array.from(tfsSet);

      for (const tf of tfs) {
        const interval = String(tf) === 'D' ? 'D' : String(tf);
        try {
          const klines = await limiter.schedule(() => bybit.fetchKlines(symbol, interval, config.SEED_KLINES_LIMIT));
          if (!klines || klines.length === 0) {
            logger.debug({ symbol, tf }, 'seedKlinesForSymbol: no klines returned from API');
            continue;
          }

          const db = dbModule.get();
          const insert = db.prepare('INSERT OR REPLACE INTO klines (symbol, timeframe, open_time, open, high, low, close, volume) VALUES (?, ?, ?, ?, ?, ?, ?, ?)');
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
        return !/USDT(\.P)?$/.test(sym);
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
          .filter(r => validUsdtSymbol(r.symbol))
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
    const tfList = (config.ROOT_TFS || []).map(normalizeTf);
    const results = [];

    if (!validUsdtSymbol(symbol)) {
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
          } else {
            logger.info({ symbol, tf }, 'scanSymbolRoots: klines seeded and available, re-checking flip');
          }
        }

        const event = await macdUtil.getMacdFlipEvent(symbol, tf);
        if (event) {
          const eventKey = `startup_root_flip:${event.eventId}`;
          if (dbModule.getState(eventKey)) continue;
          dbModule.setState(eventKey, true);

          const sig = await require('./signalManager').handleRootSignal({
            symbol,
            root_tf: tf,
            detected_at: Date.now(),
            notifyImmediately: false,
            eventId: event.eventId,
            candleTime: event.candleTime
          });

          if (sig) results.push(sig);
        }
      } catch (err) {
        logger.debug({ err, symbol, tf }, 'scanSymbolRoots: error checking flip');
      }
    }

    return results;
  }
};
