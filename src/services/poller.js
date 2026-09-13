// src/services/poller.js
const dbModule = require('../db');
const bybit = require('./bybitRest');
const config = require('../config');
const logger = require('pino')();
const Bottleneck = require('bottleneck');
const macdUtil = require('./macd');
const signalManager = require('./signalManager');
const tradeManager = require('./tradeManager');
const telegram = require('./telegram');

const limiter = new Bottleneck({ minTime: 50 });
const SEED_CONCURRENCY = Number(config.SEED_CONCURRENCY || 6);

let isRunning = false;
let lastRootCandleState = {}; // Track last root candle to prevent duplicate flips

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms || 0));
}

function getMsToNext5m() {
  const d = new Date();
  const m = d.getUTCMinutes();
  const next = new Date(d);
  const deltaM = 5 - (m % 5);
  next.setUTCMinutes(m + deltaM);
  next.setUTCSeconds(0);
  next.setUTCMilliseconds(500);
  return next - d;
}

function getMsToNextRootTf(tfs = ['15']) {
  const d = new Date();
  let minWait = 60000;
  for (const tf of tfs) {
    if (String(tf).toUpperCase() === 'D') {
      const nextDay = new Date(d);
      nextDay.setUTCHours(24, 0, 0, 500);
      const wait = nextDay - d;
      if (wait < minWait || minWait === 60000) minWait = wait;
    } else {
      const mins = Number(tf);
      if (!isNaN(mins) && mins > 0) {
        const currentMin = d.getUTCMinutes() + d.getUTCHours() * 60;
        const remainder = currentMin % mins;
        const deltaM = mins - remainder;
        const next = new Date(d);
        next.setUTCMinutes(d.getUTCMinutes() + deltaM, 0, 500);
        const wait = next - d;
        if (wait > 0 && wait < minWait) minWait = wait;
      }
    }
  }
  return minWait > 0 ? minWait : 1000;
}

module.exports = {
  start() {
    if (isRunning) return;
    isRunning = true;

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

    // ==========================================
    // LOOP 1: Initial / Deploy Scan Loop (Run once at startup, always send telegram)
    // ==========================================
    (async () => {
      try {
        logger.info('poller: [Loop 1] starting initial/deploy scan');
        await this.initialScan();
        await this.scanAllSymbolsAndNotify({ isInitial: true });
        logger.info('poller: [Loop 1] initial/deploy scan completed');
        try { signalManager.setOpenTradesAllowed(true); } catch (e) { /* ignore */ }
      } catch (err) {
        logger.error({ err }, 'poller: [Loop 1] initial/deploy scan failed');
      }
    })();

    // ==========================================
    // LOOP 2: 5m boundary scan loop (Recurring forever, always send telegram per block, no summary/recommended blocks + MTF alignment)
    // ==========================================
    this.start5mBoundaryLoop();

    // ==========================================
    // LOOP 3: New root candle open scan loop (Recurring forever, always send telegram)
    // ==========================================
    this.startNewRootCandleLoop();
  },

  async initialScan() {
    logger.info('poller.initialScan: starting');
    let allSymbols = [];
    const useWs = !!config.USE_WS;

    if (useWs) {
      try {
        const wsTimeoutMs = config.WS_INITIAL_SCAN_TIMEOUT || 10000;
        allSymbols = await Promise.race([
          this.performWsInitialScan(),
          new Promise((_, reject) => setTimeout(() => reject(new Error('WS scan timeout')), wsTimeoutMs))
        ]);
      } catch (e) {
        allSymbols = [];
      }
    }

    if (!allSymbols || allSymbols.length === 0) {
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
        insert.run(s.symbol, s.base || s.symbol.replace(/USDT[Pp]?$/i, ''), s.quote || 'USDT', now);
      }
    });
    insertMany(allSymbols.filter(s => s && s.symbol));

    const seedSymbols = bybit.getSeedSymbols(allSymbols);
    if (seedSymbols && seedSymbols.length) {
      setImmediate(() => this.backgroundSeedKlines(seedSymbols));
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
    if (!Array.isArray(symbols) || symbols.length === 0) return;
    for (let i = 0; i < symbols.length; i += SEED_CONCURRENCY) {
      const batch = symbols.slice(i, i + SEED_CONCURRENCY);
      const jobs = batch.map(s => limiter.schedule(() => this.seedKlinesForSymbol(s.symbol)));
      try { await Promise.all(jobs); } catch (e) { /* ignore */ }
    }
  },

  async seedKlinesForSymbol(symbol, timeframe = null) {
    try {
      const rootTfs = timeframe ? [String(timeframe)] : (config.ROOT_TFS || []);
      const mtfTfs = Array.isArray(config.MTF_TFS) ? config.MTF_TFS.map(String) : [];
      const tfs = Array.from(new Set([...rootTfs, ...mtfTfs]));

      for (const tf of tfs) {
        const interval = String(tf) === 'D' ? 'D' : String(tf);
        try {
          const klines = await limiter.schedule(() => bybit.fetchKlines(symbol, interval, config.SEED_KLINES_LIMIT));
          if (!klines || klines.length === 0) continue;

          const db = dbModule.get();
          const insert = db.prepare('INSERT OR IGNORE INTO klines (symbol, timeframe, open_time, open, high, low, close, volume) VALUES (?, ?, ?, ?, ?, ?, ?, ?)');
          const insertMany = db.transaction((rows) => {
            for (const k of rows) {
              insert.run(symbol, tf, k.open_time, k.open, k.high, k.low, k.close, k.volume);
            }
          });
          insertMany(klines);

          try {
            if (typeof macdUtil.computeAndStoreMacd === 'function') {
              await macdUtil.computeAndStoreMacd(symbol, tf);
            } else if (typeof macdUtil.computeMacdHistogram === 'function') {
              await macdUtil.computeMacdHistogram(symbol, tf);
            }
          } catch (err) { /* ignore */ }
        } catch (err) { /* ignore */ }
      }
    } catch (err) { /* ignore */ }
  },

  async scanAllSymbolsAndNotify({ isInitial = false } = {}) {
    try {
      const db = dbModule;
      const prev = db.getLatestSignalsSnapshot();
      const prevKeys = new Set(prev.map(r => r.key));
      const scanStart = Date.now();

      const rows = db.get().prepare('SELECT symbol FROM symbols ORDER BY symbol COLLATE NOCASE ASC').all();
      for (let i = 0; i < rows.length; i += config.PAGE_SIZE) {
        const page = rows.slice(i, i + config.PAGE_SIZE);
        const tasks = page.map(r => this.scanSymbolRoots(r.symbol, { notifyImmediately: false, detected_ts: scanStart }));
        try { await Promise.all(tasks); } catch (e) { /* ignore */ }
      }

      const after = db.getLatestSignalsSnapshot();
      const newSignals = after.filter(r => !prevKeys.has(r.key) && r.detected_at >= scanStart);

      if (newSignals.length > 0) {
        logger.info({ count: newSignals.length, isInitial }, 'scanAllSymbolsAndNotify: new signals found');
        for (const s of newSignals) {
          try {
            await telegram.sendNewSignalSingleBlock(s);
          } catch (e) {
            logger.debug({ e, s }, 'scanAllSymbolsAndNotify: send failed');
          }
          await sleep(config.TELEGRAM_SEND_DELAY_MS || 100);
        }
      }

      try {
        db.setState('lastScanAt', scanStart);
        db.setState('lastScanSignals', after.map(r => r.key));
      } catch (e) { /* ignore */ }
    } catch (err) {
      logger.error({ err }, 'scanAllSymbolsAndNotify error');
    }
  },

  async scanSymbolRoots(symbol, { notifyImmediately = true, detected_ts = null } = {}) {
    const tfList = config.ROOT_TFS || [];
    const results = [];
    for (const tf of tfList) {
      try {
        const db = dbModule.get();
        const selectStmt = db.prepare('SELECT open_time, close, open FROM klines WHERE symbol=? AND timeframe=? ORDER BY open_time DESC LIMIT 2');
        let rows = selectStmt.all(symbol, tf);
        if (!rows || rows.length < 2) {
          await this.seedKlinesForSymbol(symbol, tf);
          rows = selectStmt.all(symbol, tf);
          if (!rows || rows.length < 2) continue;
        }

        const stateKey = `${symbol}:${tf}`;
        const latestCandleTime = rows[rows.length - 1].open_time;
        if (lastRootCandleState[stateKey] === latestCandleTime) continue;
        lastRootCandleState[stateKey] = latestCandleTime;

        const flip = await require('./macd').isMacdFlip(symbol, tf);
        if (flip) {
          const sig = await signalManager.handleRootSignal({
            symbol,
            root_tf: tf,
            detected_at: detected_ts || Date.now(),
            notifyImmediately
          });
          if (sig) results.push(sig);
        }
      } catch (err) { /* ignore */ }
    }
    return results;
  },

  start5mBoundaryLoop() {
    const runLoop = async () => {
      const wait = getMsToNext5m();
      logger.info({ wait }, '[Loop 2] 5mboundaryscan waiting until next 5m boundary');
      setTimeout(async () => {
        try {
          logger.info('[Loop 2] executing 5mboundaryscan');
          await this.scanAllSymbolsAndNotify({ isInitial: false });

          // Monitored root signals MTF alignment alert check and telegram send
          try {
            if (typeof signalManager.checkAndNotifyMtfAlignments === 'function') {
              await signalManager.checkAndNotifyMtfAlignments();
            } else if (typeof signalManager.getMonitoredMtfAlignments === 'function') {
              const alerts = await signalManager.getMonitoredMtfAlignments();
              for (const a of alerts) {
                if (typeof telegram.sendMtfAlignmentAlert === 'function') {
                  await telegram.sendMtfAlignmentAlert(a);
                }
              }
            }
          } catch (e) {
            logger.debug({ e }, '[Loop 2] MTF alignment alert check error');
          }

          if (config.CLOSE_LEAST_PROFITABLE_ENABLED) {
            const openCount = dbModule.get().prepare('SELECT COUNT(*) as c FROM trades WHERE status = ?').get('open').c || 0;
            if (openCount >= config.MAX_OPEN_TRADES) {
              const minutesSinceHour = new Date().getUTCMinutes();
              const closeMins = config.CLOSE_LEAST_PROFITABLE_MINS_BEFORE_BOUNDARY || 5;
              if (minutesSinceHour >= (60 - closeMins)) {
                try { await tradeManager.closeLeastProfitableTrade(); } catch (err) { /* ignore */ }
              }
            }
          }
        } catch (err) {
          logger.error({ err }, '[Loop 2] 5mboundaryscan failed');
        } finally {
          runLoop();
        }
      }, wait);
    };
    runLoop();
  },

  startNewRootCandleLoop() {
    const runLoop = async () => {
      const rootTfs = config.ROOT_TFS || ['15'];
      const wait = getMsToNextRootTf(rootTfs);
      logger.info({ wait }, '[Loop 3] newrootcandle scan waiting until next boundary');
      setTimeout(async () => {
        try {
          logger.info('[Loop 3] executing newrootcandle open scan');
          const now = new Date();
          const minute = now.getUTCMinutes();
          const hour = now.getUTCHours();
          const newRootTfs = [];
          for (const tf of rootTfs) {
            if (String(tf).toUpperCase() === 'D') {
              if (hour === 0 && minute === 0) newRootTfs.push('D');
            } else {
              const tfNum = Number(tf);
              if (!isNaN(tfNum)) {
                const minutesSinceEpoch = Math.floor(now.getTime() / 60000);
                if (minutesSinceEpoch % tfNum === 0) newRootTfs.push(String(tf));
              }
            }
          }

          if (newRootTfs.length > 0) {
            const db = dbModule.get();
            const rows = db.prepare('SELECT symbol FROM symbols ORDER BY symbol COLLATE NOCASE ASC').all();
            for (const r of rows) {
              for (const tf of newRootTfs) {
                await this.seedKlinesForSymbol(r.symbol, tf);
              }
            }
            if (config.NEW_ROOT_CANDLE_NOTIFY) {
              await signalManager.handleNewRootCandle(newRootTfs);
            }
            await this.scanAllSymbolsAndNotify({ isInitial: false });
          }
        } catch (err) {
          logger.error({ err }, '[Loop 3] newrootcandle scan failed');
        } finally {
          runLoop();
        }
      }, wait);
    };
    runLoop();
  }
};
