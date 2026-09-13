// src/services/poller.js
const dbModule = require('../db');
const bybit = require('./bybitRest');
const config = require('../config');
const logger = require('pino')();
const Bottleneck = require('bottleneck');
const macdUtil = require('./macd');
const signalManager = require('./signalManager');
const tradeManager = require('./tradeManager');

const limiter = new Bottleneck({ minTime: 50 });
const SEED_CONCURRENCY = Number(config.SEED_CONCURRENCY || 6);

let isRunning = false;
let lastRootCandleState = {}; // Track last root candle to prevent duplicate flips

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms || 0));
}

// Helper: build startup / TF-scoped summary text block (A-Z symbols + counts per TF + signals snapshot)
async function buildStartupSummaryText(dbHandle, symbolRows = [], scopeTfs = null) {
  try {
    const symbols = Array.isArray(symbolRows) ? symbolRows.map(r => r.symbol).filter(Boolean) : [];
    symbols.sort((a, b) => a.localeCompare(b, 'en', { sensitivity: 'base' }));
    const total = symbols.length;

    const tfList = Array.isArray(scopeTfs) && scopeTfs.length ? scopeTfs : (Array.isArray(config.ROOT_TFS) ? config.ROOT_TFS : []);
    const countsPerTf = {};
    try {
      for (const tf of tfList) {
        try {
          const row = dbHandle.prepare('SELECT COUNT(DISTINCT symbol) as c FROM klines WHERE timeframe = ?').get(String(tf));
          countsPerTf[String(tf)] = (row && row.c) ? row.c : 0;
        } catch (e) {
          countsPerTf[String(tf)] = 0;
        }
      }
    } catch (e) {
      logger.debug({ e }, 'buildStartupSummaryText: failed to compute countsPerTf');
      for (const tf of tfList) countsPerTf[String(tf)] = countsPerTf[String(tf)] || 0;
    }

    // Signals snapshot (best-effort)
    let signals = [];
    try {
      if (typeof dbModule.getLatestSignalsSnapshot === 'function') {
        signals = dbModule.getLatestSignalsSnapshot();
      } else {
        signals = [];
      }
    } catch (e) {
      logger.debug({ e }, 'buildStartupSummaryText: failed to fetch latest signals snapshot');
      signals = [];
    }

    const lines = [];
    lines.push('Startup Scan Summary');
    lines.push(`Total symbols: ${total}`);
    lines.push('');
    lines.push('Symbols (A-Z):');
    if (symbols.length) {
      // chunk to keep lines reasonable
      for (let i = 0; i < symbols.length; i += 12) {
        lines.push(symbols.slice(i, i + 12).join(', '));
      }
    } else {
      lines.push('(no symbols)');
    }
    lines.push('');
    lines.push('Counts per TF:');
    if (Object.keys(countsPerTf).length) {
      for (const tfKey of Object.keys(countsPerTf)) {
        lines.push(` - ${tfKey}: ${countsPerTf[tfKey]}`);
      }
    } else {
      lines.push(' - (no TFs configured)');
    }
    lines.push('');
    lines.push('Signals snapshot:');
    if (Array.isArray(signals) && signals.length) {
      for (const s of signals) {
        const sigLine = `${s.key || s.symbol || 'unknown'} | root_tf=${s.root_tf || '-'} | ${s.side || '-'} | detected_at=${new Date(s.detected_at || 0).toISOString()}`;
        lines.push(` - ${sigLine}`);
      }
    } else {
      lines.push(' - (no signals)');
    }

    return lines.join('\n');
  } catch (err) {
    logger.debug({ err }, 'buildStartupSummaryText: unexpected error');
    return 'Startup Scan Summary: (error building summary)';
  }
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

    (async () => {
      try {
        logger.info('poller: starting initialScan');
        await this.initialScan();
        logger.info('poller: initialScan completed');
        
        // Full silent startup scan (LOOP 1 - Deploy)
        try {
          await this.scanAllForStartup();
          logger.info('poller: startup full scan completed');
        } catch (err) {
          logger.error({ err }, 'poller: scanAllForStartup error');
        }

        // Enable trading after initial scan complete
        try {
          signalManager.setOpenTradesAllowed(true);
          logger.info('poller: open trades enabled after initial scan');
        } catch (e) { 
          logger.debug({ e }, 'Failed to set open trades allowed'); 
        }

        // Start LOOP 2 - 5m boundary scan
        this.start5mBoundaryScanLoop();

        // Start LOOP 3 - Root TF candle open scan
        this.startRootTfCandleOpenScanLoop();

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
      logger.info('poller: fetching symbols via REST (cursor pagination)');
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
    logger.info({ total: allSymbols.length }, 'poller.initialScan: symbols persisted');

    const seedSymbols = bybit.getSeedSymbols(allSymbols);
    if (seedSymbols && seedSymbols.length) {
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

  // ============================================================
  // LOOP 1: Initial/Deploy Scan (runs once at startup)
  // Sends startup summary (A-Z list + counts per TF + signals snapshot), then per-signal blocks, then recommended trades
  // ============================================================
  async scanAllForStartup() {
    try {
      logger.info('scanAllForStartup: starting full startup pass (silent)');
      const dbHandle = dbModule.get();
      const rows = dbHandle.prepare('SELECT symbol FROM symbols ORDER BY symbol COLLATE NOCASE ASC').all();

      for (let i = 0; i < rows.length; i += config.PAGE_SIZE) {
        const page = rows.slice(i, i + config.PAGE_SIZE);
        const tasks = page.map(r => this.scanSymbolRoots(r.symbol, { notifyImmediately: false, detected_ts: Date.now() }));
        try {
          await Promise.all(tasks);
        } catch (e) {
          logger.debug({ e }, 'scanAllForStartup: page tasks error (continuing)');
        }
      }

      // Build and send a single summary block (A-Z list + counts per TF + signals snapshot)
      try {
        const telegram = require('./telegram');
        const summaryText = await buildStartupSummaryText(dbHandle, rows);
        if (telegram && typeof telegram.sendStartupSummaryBlock === 'function') {
          await telegram.sendStartupSummaryBlock({ text: summaryText });
        } else if (telegram && typeof telegram.sendRawMessage === 'function') {
          await telegram.sendRawMessage(summaryText);
        } else {
          logger.info('scanAllForStartup: telegram startup summary method not found (skipping)');
        }
      } catch (e) {
        logger.debug({ e }, 'scanAllForStartup: failed to send startup summary');
      }

      // After summary block, send one block per currently detected signal
      try {
        const telegram = require('./telegram');
        const currentSignals = typeof dbModule.getLatestSignalsSnapshot === 'function' ? dbModule.getLatestSignalsSnapshot() : [];
        if (Array.isArray(currentSignals) && currentSignals.length) {
          for (let i = 0; i < currentSignals.length; i++) {
            const s = currentSignals[i];
            try {
              await telegram.sendNewSignalSingleBlock(s);
            } catch (e) {
              logger.debug({ e, s }, 'scanAllForStartup: failed to send signal block');
            }
            await sleep(config.TELEGRAM_SEND_DELAY_MS || 100);
          }
        } else {
          logger.info('scanAllForStartup: no current signals to send individually');
        }
      } catch (e) {
        logger.debug({ e }, 'scanAllForStartup: error sending individual signal blocks');
      }

      // Finally, send a recommended trades block if available
      try {
        const telegram = require('./telegram');
        if (tradeManager && typeof tradeManager.getRecommendedTrades === 'function') {
          const rec = await tradeManager.getRecommendedTrades();
          if (rec && rec.length) {
            if (telegram && typeof telegram.sendRecommendedTradesBlock === 'function') {
              await telegram.sendRecommendedTradesBlock(rec);
            } else if (telegram && typeof telegram.sendRawMessage === 'function') {
              await telegram.sendRawMessage('Recommended trades:\n' + JSON.stringify(rec, null, 2));
            }
          } else {
            logger.info('scanAllForStartup: no recommended trades returned');
          }
        } else {
          logger.debug('scanAllForStartup: tradeManager.getRecommendedTrades not available (skipping recommended trades block)');
        }
      } catch (e) {
        logger.debug({ e }, 'scanAllForStartup: failed to send recommended trades block');
      }

      logger.info('scanAllForStartup: completed full startup pass');
    } catch (err) {
      logger.error({ err }, 'scanAllForStartup: unexpected error');
    }
  },

  // ============================================================
  // LOOP 2: 5m Boundary Scan (runs at every 5m boundary)
  // Sends ONE new signal per block (only signals not in previous snapshot)
  // Also sends MTF alignment alerts when new root candles found
  // ============================================================
  start5mBoundaryScanLoop() {
    const msToNext5 = () => {
      const d = new Date();
      const m = d.getUTCMinutes();
      const next = new Date(d);
      const deltaM = 5 - (m % 5);
      next.setUTCMinutes(m + deltaM);
      next.setUTCSeconds(0);
      next.setUTCMilliseconds(500);
      return next - d;
    };

    const loop = async () => {
      const wait = msToNext5();
      logger.info({ wait, loop: '5m-boundary-scan' }, 'Next 5m boundary scan in ms');
      
      setTimeout(async () => {
        try {
          await this.scan5mBoundary();
        } catch (err) {
          logger.error({ err }, 'LOOP 2 (5m-boundary-scan): error');
        } finally {
          loop();
        }
      }, wait);
    };

    loop();
  },

  async scan5mBoundary() {
    try {
      const db = dbModule;
      const prev = db.getLatestSignalsSnapshot();
      const prevKeys = new Set(prev.map(r => r.key));

      const scanStart = Date.now();

      logger.info({ loop: '5m-boundary-scan' }, 'Starting 5m boundary scan');

      const rows = db.get().prepare('SELECT symbol FROM symbols ORDER BY symbol COLLATE NOCASE ASC').all();
      for (let i = 0; i < rows.length; i += config.PAGE_SIZE) {
        const page = rows.slice(i, i + config.PAGE_SIZE);
        const tasks = page.map(r => this.scanSymbolRoots(r.symbol, { notifyImmediately: false, detected_ts: scanStart }));
        try {
          await Promise.all(tasks);
        } catch (e) {
          logger.debug({ e }, 'scan5mBoundary: page tasks error (continuing)');
        }
      }

      const after = db.getLatestSignalsSnapshot();
      const newSignals = after.filter(r => !prevKeys.has(r.key) && r.detected_at >= scanStart);

      if (newSignals.length > 0) {
        logger.info({ newSignals: newSignals.length, loop: '5m-boundary-scan' }, 'New signals detected at 5m boundary');

        const telegram = require('./telegram');
        for (let i = 0; i < newSignals.length; i++) {
          const s = newSignals[i];
          try {
            // Send only one signal per block (existing layout)
            await telegram.sendNewSignalSingleBlock(s);
          } catch (e) {
            logger.debug({ e, s }, 'scan5mBoundary: failed to send new-signal message');
          }
          await sleep(config.TELEGRAM_SEND_DELAY_MS || 100);
        }
      } else {
        logger.info({ loop: '5m-boundary-scan' }, 'No new signals at 5m boundary');
      }

      try {
        db.setState('lastScanAt', scanStart);
        db.setState('lastScanSignals', after.map(r => r.key));
      } catch (e) {
        logger.debug({ e }, 'scan5mBoundary: failed to persist scan state');
      }

      // Check for new root TF candles and send MTF alignment alerts
      try {
        const now = new Date();
        const minute = now.getUTCMinutes();
        const hour = now.getUTCHours();
        const newRootTfs = [];
        
        for (const tf of config.ROOT_TFS) {
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

        if (newRootTfs.length && config.NEW_ROOT_CANDLE_NOTIFY) {
          logger.info({ newRootTfs, loop: '5m-boundary-scan' }, 'New root candles detected');
          try {
            await signalManager.handleNewRootCandle(newRootTfs);
          } catch (e) {
            logger.debug({ e, newRootTfs }, 'scan5mBoundary: handleNewRootCandle failed');
          }

          // Send explicit MTF alignment/notify via telegram (best-effort)
          try {
            const telegram = require('./telegram');
            if (telegram && typeof telegram.sendMtfAlignmentAlert === 'function') {
              await telegram.sendMtfAlignmentAlert(newRootTfs);
            } else if (telegram && typeof telegram.sendRawMessage === 'function') {
              await telegram.sendRawMessage(`MTF alignment: new root candles: ${newRootTfs.join(', ')}`);
            }
          } catch (e) {
            logger.debug({ e, newRootTfs }, 'scan5mBoundary: failed to send MTF alignment alert');
          }
        }
      } catch (e) {
        logger.debug({ e }, 'scan5mBoundary: root TF detection failed');
      }

      // Close least profitable trade if enabled and max trades filled
      if (config.CLOSE_LEAST_PROFITABLE_ENABLED) {
        try {
          const openCount = dbModule.get().prepare('SELECT COUNT(*) as c FROM trades WHERE status = ?').get('open').c || 0;
          if (openCount >= config.MAX_OPEN_TRADES) {
            const minutesSinceHour = new Date().getUTCMinutes();
            const closeMins = config.CLOSE_LEAST_PROFITABLE_MINS_BEFORE_BOUNDARY || 5;
            if (minutesSinceHour >= (60 - closeMins)) {
              logger.info({ openCount, closeMins, loop: '5m-boundary-scan' }, 'Closing least profitable trade before boundary');
              try {
                await tradeManager.closeLeastProfitableTrade();
              } catch (err) {
                logger.error({ err }, 'Error closing least profitable trade');
              }
            }
          }
        } catch (err) {
          logger.debug({ err }, 'scan5mBoundary: least profitable check failed');
        }
      }

    } catch (err) {
      logger.error({ err }, 'scan5mBoundary: unexpected error');
    }
  },

  // ============================================================
  // LOOP 3: Root TF Candle Open Scan (triggered at candle open)
  // For relevant new TFs: sends TF-scoped summary (A-Z + counts), one block per flip, then recommended trades for that TF
  // ============================================================
  startRootTfCandleOpenScanLoop() {
    const getNextCandleOpenTime = () => {
      const now = new Date();
      const currentMinutes = now.getUTCMinutes();
      const currentSeconds = now.getUTCSeconds();
      
      // Find smallest root TF
      const rootTfs = config.ROOT_TFS || [];
      let minTfMinutes = Infinity;
      
      for (const tf of rootTfs) {
        if (String(tf).toUpperCase() === 'D') {
          minTfMinutes = Math.min(minTfMinutes, 1440);
        } else {
          const tfNum = Number(tf);
          if (!isNaN(tfNum)) {
            minTfMinutes = Math.min(minTfMinutes, tfNum);
          }
        }
      }
      
      if (minTfMinutes === Infinity) return null;
      
      // Next candle open time
      const minutesSinceEpoch = Math.floor(now.getTime() / 60000);
      const nextCandleEpoch = (Math.floor(minutesSinceEpoch / minTfMinutes) + 1) * minTfMinutes;
      const nextCandleTime = new Date(nextCandleEpoch * 60000);
      
      return nextCandleTime;
    };

    const loop = async () => {
      const nextCandleTime = getNextCandleOpenTime();
      if (!nextCandleTime) {
        logger.warn({ loop: 'root-tf-candle-open-scan' }, 'Could not determine next candle time');
        setTimeout(() => loop(), 60000);
        return;
      }

      const wait = nextCandleTime - new Date();
      logger.info({ wait, nextCandleTime: nextCandleTime.toISOString(), loop: 'root-tf-candle-open-scan' }, 'Next root TF candle open scan in ms');
      
      setTimeout(async () => {
        try {
          await this.scanRootTfCandleOpen();
        } catch (err) {
          logger.error({ err }, 'LOOP 3 (root-tf-candle-open-scan): error');
        } finally {
          loop();
        }
      }, Math.max(wait, 0));
    };

    loop();
  },

  async scanRootTfCandleOpen() {
    try {
      const dbHandle = dbModule.get();
      const scanStart = Date.now();

      logger.info({ loop: 'root-tf-candle-open-scan' }, 'Scanning root TF candle opens for flips');

      const rows = dbHandle.prepare('SELECT symbol FROM symbols ORDER BY symbol COLLATE NOCASE ASC').all();
      const detectedFlips = [];

      for (let i = 0; i < rows.length; i += config.PAGE_SIZE) {
        const page = rows.slice(i, i + config.PAGE_SIZE);
        const tasks = page.map(r => 
          this.scanSymbolRootsForCandleOpen(r.symbol, scanStart)
            .then(flips => {
              if (Array.isArray(flips) && flips.length) detectedFlips.push(...flips);
            })
        );
        try {
          await Promise.all(tasks);
        } catch (e) {
          logger.debug({ e }, 'scanRootTfCandleOpen: page tasks error (continuing)');
        }
      }

      if (detectedFlips.length > 0) {
        logger.info({ detectedFlips: detectedFlips.length, loop: 'root-tf-candle-open-scan' }, 'Flips detected at candle open');

        const telegram = require('./telegram');

        // Group flips by root_tf so we can send TF-scoped summary blocks
        const byTf = detectedFlips.reduce((acc, s) => {
          const tf = String(s.root_tf || 'unknown');
          (acc[tf] = acc[tf] || []).push(s);
          return acc;
        }, {});

        for (const tf of Object.keys(byTf)) {
          const flipsForTf = byTf[tf];

          // Build a TF-scoped symbol list for this TF (A-Z)
          const symbolsInTf = flipsForTf.map(f => ({ symbol: f.symbol }));
          const tfSummaryText = await buildStartupSummaryText(dbHandle, symbolsInTf, [tf]);

          // Send TF-scoped summary block
          try {
            if (telegram && typeof telegram.sendStartupSummaryBlock === 'function') {
              await telegram.sendStartupSummaryBlock({ text: tfSummaryText, tf });
            } else if (telegram && typeof telegram.sendRawMessage === 'function') {
              await telegram.sendRawMessage(`TF ${tf} summary\n${tfSummaryText}`);
            } else {
              logger.info('scanRootTfCandleOpen: telegram startup summary method not found (skipping TF summary)');
            }
          } catch (e) {
            logger.debug({ e, tf }, 'scanRootTfCandleOpen: failed to send TF summary block');
          }

          // Then send one block per flip (existing layout)
          for (let i = 0; i < flipsForTf.length; i++) {
            const s = flipsForTf[i];
            try {
              await telegram.sendNewSignalSingleBlock(s);
            } catch (e) {
              logger.debug({ e, s }, 'scanRootTfCandleOpen: failed to send flip message');
            }
            await sleep(config.TELEGRAM_SEND_DELAY_MS || 100);
          }

          // Finally recommend trades for this TF if available
          try {
            if (tradeManager && typeof tradeManager.getRecommendedTradesForTf === 'function') {
              const rec = await tradeManager.getRecommendedTradesForTf(tf);
              if (rec && rec.length) {
                if (telegram && typeof telegram.sendRecommendedTradesBlock === 'function') {
                  await telegram.sendRecommendedTradesBlock(rec, { tf });
                } else if (telegram && typeof telegram.sendRawMessage === 'function') {
                  await telegram.sendRawMessage(`Recommended trades (${tf}):\n` + JSON.stringify(rec, null, 2));
                }
              } else {
                logger.info({ tf }, 'scanRootTfCandleOpen: no recommended trades for TF');
              }
            }
          } catch (e) {
            logger.debug({ e, tf }, 'scanRootTfCandleOpen: failed to send recommended trades block');
          }
        } // end for each tf
      } else {
        logger.info({ loop: 'root-tf-candle-open-scan' }, 'No flips detected at candle open');
      }

    } catch (err) {
      logger.error({ err }, 'scanRootTfCandleOpen: unexpected error');
    }
  },

  async scanSymbolRootsForCandleOpen(symbol, detected_ts = null) {
    const tfList = config.ROOT_TFS || [];
    const flips = [];

    for (const tf of tfList) {
      try {
        const db = dbModule.get();
        const selectStmt = db.prepare('SELECT open_time, close, open FROM klines WHERE symbol=? AND timeframe=? ORDER BY open_time DESC LIMIT 2');
        let rows = selectStmt.all(symbol, tf);

        if (!rows || rows.length < 2) {
          logger.debug({ symbol, tf, loop: 'root-tf-candle-open-scan' }, 'Insufficient klines for flip check');
          continue;
        }

        // Check if we've already processed this exact candle
        const stateKey = `${symbol}:${tf}`;
        const latestCandleTime = rows[rows.length - 1].open_time; // oldest in DESC = latest candle
        
        if (lastRootCandleState[stateKey] === latestCandleTime) {
          logger.debug({ symbol, tf, candleTime: latestCandleTime, loop: 'root-tf-candle-open-scan' }, 'Already processed this candle, skipping');
          continue;
        }
        
        lastRootCandleState[stateKey] = latestCandleTime;

        // Check for MACD flip
        const flip = await require('./macd').isMacdFlip(symbol, tf);
        if (flip) {
          const sig = await signalManager.handleRootSignal({
            symbol,
            root_tf: tf,
            detected_at: detected_ts || Date.now(),
            notifyImmediately: true
          });
          if (sig) flips.push(sig);
          logger.info({ symbol, tf, loop: 'root-tf-candle-open-scan' }, 'MACD flip detected and signal created');
        }
      } catch (err) {
        logger.debug({ err, symbol, tf }, 'scanSymbolRootsForCandleOpen: error checking flip');
      }
    }

    return flips;
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
          logger.debug({ symbol, tf }, 'scanSymbolRoots: insufficient klines, seeding now (will also seed MTF TFs)');
          await this.seedKlinesForSymbol(symbol, tf);

          rows = selectStmt.all(symbol, tf);
          if (!rows || rows.length < 2) {
            logger.debug({ symbol, tf }, 'scanSymbolRoots: still insufficient klines after seeding, skipping tf for now');
            continue;
          } else {
            logger.info({ symbol, tf }, 'scanSymbolRoots: klines seeded and available, re-checking flip');
          }
        }

        // Track last root candle open_time to prevent duplicate flip detections
        const stateKey = `${symbol}:${tf}`;
        const latestCandleTime = rows[rows.length - 1].open_time; // oldest in DESC = latest candle
        
        if (lastRootCandleState[stateKey] === latestCandleTime) {
          logger.debug({ symbol, tf, candleTime: latestCandleTime }, 'scanSymbolRoots: already processed this candle, skipping flip detection');
          continue;
        }
        
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
      } catch (err) {
        logger.debug({ err, symbol, tf }, 'scanSymbolRoots: error checking flip');
      }
    }
    return results;
  }
};
