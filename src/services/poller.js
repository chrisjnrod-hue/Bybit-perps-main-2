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

        // Loop 2: 5m boundary scan + MTF alignment change alerts
        this.startBoundaryScanLoop();

        // Loop 3: root candle open scan (startup-like batch)
        this.startRootCandleOpenScanLoop();

      } catch (err) {
        logger.error({ err }, 'poller: initialScan failed');
      }
    })();
  },

  // -------- LOOP 2: 5-minute boundary scan + MTF alignment alert --------
  startBoundaryScanLoop() {
    if (this._boundaryScanLoopTimer) return;

    const intervalMs = Number(process.env.ROOT_SCAN_5MIN_BOUNDARY_INTERVAL_MS || config.ROOT_SCAN_5MIN_BOUNDARY_INTERVAL_MS || 300000);

    logger.info({ intervalMs }, 'poller: starting 5m root boundary scan loop');

    this._boundaryScanLoopTimer = setInterval(() => {
      this.runBoundaryScanLoop().catch((err) => {
        logger.error({ err }, 'poller: boundary scan loop error');
      });
    }, intervalMs);

    // initial run
    this.runBoundaryScanLoop().catch((err) => {
      logger.error({ err }, 'poller: initial boundary scan failed');
    });
  },

  async runBoundaryScanLoop() {
    const db = dbModule.get();
    const rows = db.prepare('SELECT symbol FROM symbols ORDER BY symbol COLLATE NOCASE ASC').all();

    const validSymbols = rows
      .map(r => String(r.symbol || '').trim())
      .filter(validUsdtSymbol);

    for (const symbol of validSymbols) {
      for (const tf of config.ROOT_TFS || []) {
        try {
          // refresh seed data for root tf before evaluation
          await this.ensureKlinesReady(symbol, tf);

          const key = `boundary_scan:${symbol}:${tf}`;
          const lastSeen = Number(dbModule.getState(key) || 0);
          const now = Date.now();

          // prevent duplicate boundary scans too close together
          if (lastSeen && (now - lastSeen) < 5 * 60 * 1000) {
            continue;
          }

          const detected = await this.checkNewBoundarySignal(symbol, tf);
          if (detected) {
            dbModule.setState(key, now);
          }

          // MTF alignment alert for active/monitored root signal
          const alertSent = await this.checkMtfAlignmentAlert(symbol, tf);
          if (alertSent) {
            dbModule.setState(`mtf_alert:${symbol}:${tf}`, now);
          }
        } catch (err) {
          logger.debug({ err, symbol, tf }, 'poller: boundary scan symbol iteration failed');
        }
      }
    }
  },

  async checkNewBoundarySignal(symbol, tf) {
    try {
      const db = dbModule.get();
      const selectStmt = db.prepare('SELECT open_time, close, open FROM klines WHERE symbol=? AND timeframe=? ORDER BY open_time DESC LIMIT 3');
      const rows = selectStmt.all(symbol, tf);

      if (!rows || rows.length < 3) {
        await this.seedKlinesForSymbol(symbol, tf);
        return false;
      }

      // strict: only first closed-candle flip
      const flip = await macdUtil.isMacdFlip(symbol, tf);
      if (!flip) return false;

      // IMPORTANT: this loop must send one single block per new signal only
      const signalObj = await require('./signalManager').handleRootSignal({
        symbol,
        root_tf: tf,
        detected_at: Date.now(),
        notifyImmediately: true
      });

      if (signalObj) {
        logger.info({ symbol, root_tf: tf }, 'poller: boundary scan emitted new signal block');
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
      // Only evaluate active/monitored root signals for this symbol+root_tf
      const db = dbModule.get();
      const row = db.prepare(`
        SELECT symbol, root_tf, meta, detected_at
        FROM signals
        WHERE symbol = ? AND root_tf = ?
        ORDER BY detected_at DESC
        LIMIT 1
      `).get(symbol, tf);

      if (!row) {
        return false;
      }

      let meta = {};
      try { meta = row.meta ? JSON.parse(row.meta) : {}; } catch (e) { meta = {}; }

      const alignment = await signalManager.evaluateMtfAlignment(symbol);
      const newSignature = stableAlignmentSignature(alignment);

      const lastSignature = dbModule.getState(`mtf_align_sig:${symbol}:${tf}`);
      if (lastSignature && lastSignature === newSignature) {
        return false;
      }

      // persist new signature
      dbModule.setState(`mtf_align_sig:${symbol}:${tf}`, newSignature);

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
        key: `${symbol}:${tf}`,
        symbol,
        root_tf: tf,
        detected_at: Date.now(),
        state: 'monitor',
        meta: alertMeta
      };

      // Loop 2: send only a single signal block; no summary, no recommended blocks
      notificationQueue.enqueueSignal(signalObj, 'realtime');

      logger.info({ symbol, root_tf: tf, signature: newSignature }, 'poller: MTF alignment alert sent');
      return true;
    } catch (err) {
      logger.debug({ err, symbol, tf }, 'poller: MTF alignment alert error');
      return false;
    }
  },

  // -------- LOOP 3: root candle open scan --------
  startRootCandleOpenScanLoop() {
    if (this._rootCandleOpenLoopTimer) return;

    const intervalMs = Number(process.env.ROOT_CANDLE_OPEN_SCAN_INTERVAL_SECS || config.ROOT_CANDLE_OPEN_SCAN_INTERVAL_SECS || 60) * 1000;

    logger.info({ intervalMs }, 'poller: starting root candle open scan loop');

    this._rootCandleOpenLoopTimer = setInterval(() => {
      this.runRootCandleOpenScanLoop().catch((err) => {
        logger.error({ err }, 'poller: root candle open scan loop error');
      });
    }, intervalMs);

    // initial run
    this.runRootCandleOpenScanLoop().catch((err) => {
      logger.error({ err }, 'poller: initial root candle open scan failed');
    });
  },

  async runRootCandleOpenScanLoop() {
    const db = dbModule.get();
    const rows = db.prepare('SELECT symbol FROM symbols ORDER BY symbol COLLATE NOCASE ASC').all();

    const validSymbols = rows
      .map(r => String(r.symbol || '').trim())
      .filter(validUsdtSymbol);

    const rootTfs = Array.isArray(config.ROOT_TFS) ? config.ROOT_TFS.map(String) : ['60', '240', 'D'];

    const signals = [];

    for (const symbol of validSymbols) {
      for (const tf of rootTfs) {
        try {
          const result = await this.scanRootCandleOpenForSymbol(symbol, tf);
          if (result) signals.push(result);
        } catch (err) {
          logger.debug({ err, symbol, tf }, 'poller: root candle open scan symbol iteration failed');
        }
      }
    }

    // Loop 3: same startup-like bundle format as loop 1
    if (signals.length > 0) {
      logger.info({ signalCount: signals.length }, 'poller: queueing root candle open startup-like batch');
      notificationQueue.enqueueStartupBatch(signals);
    }
  },

  async scanRootCandleOpenForSymbol(symbol, tf) {
    try {
      const db = dbModule.get();
      const selectStmt = db.prepare('SELECT open_time, close, open FROM klines WHERE symbol=? AND timeframe=? ORDER BY open_time DESC LIMIT 3');
      const rows = selectStmt.all(symbol, tf);

      if (!rows || rows.length < 3) {
        await this.seedKlinesForSymbol(symbol, tf);
        const retry = selectStmt.all(symbol, tf);
        if (!retry || retry.length < 3) return null;
      }

      const flip = await macdUtil.isMacdFlipAtOpen(symbol, tf);
      if (!flip) return null;

      const sig = await require('./signalManager').handleRootSignal({
        symbol,
        root_tf: tf,
        detected_at: Date.now(),
        notifyImmediately: false
      });

      return sig;
    } catch (err) {
      logger.debug({ err, symbol, tf }, 'poller: root candle open scan error');
      return null;
    }
  },

  // -------- existing startup/seed logic --------
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

  async ensureKlinesReady(symbol, tf) {
    const db = dbModule.get();
    const selectStmt = db.prepare('SELECT open_time, close, open FROM klines WHERE symbol=? AND timeframe=? ORDER BY open_time DESC LIMIT 3');
    let rows = selectStmt.all(symbol, tf);

    if (!rows || rows.length < 3) {
      await this.seedKlinesForSymbol(symbol, tf);
      rows = selectStmt.all(symbol, tf);
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

      const rootTfs = timeframe ? [String(timeframe)] : (config.ROOT_TFS || []);
      const mtfTfs = Array.isArray(config.MTF_TFS) ? config.MTF_TFS.map(String) : [];
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
    const tfList = config.ROOT_TFS || [];
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
  }
};
