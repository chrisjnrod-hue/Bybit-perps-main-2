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

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms || 0));
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
        logger.info('poller: LOOP 1 - Initial/Deploy scan starting');
        await this.initialScan();
        logger.info('poller: initialScan completed');
        
        // Full scan on startup (silent, no notifications)
        try {
          await this.scanAllForStartup();
          logger.info('poller: startup full scan completed');
        } catch (err) {
          logger.error({ err }, 'poller: scanAllForStartup error');
        }

        // Send startup summary via telegram (includes header + per-signal blocks + recommended)
        try {
          const telegram = require('./telegram');
          const snapshot = dbModule.getLatestSignalsSnapshot();
          await telegram.sendStartupSummary({ snapshot });
          logger.info('poller: Startup summary telegram sent');
        } catch (err) {
          logger.error({ err }, 'poller: Failed to send startup summary');
        }

      } catch (err) {
        logger.error({ err }, 'poller: initialScan failed');
      }
    })();

    // LOOP 2: 5m Boundary Scan (precise 5-minute aligned scanning)
    this.schedule5mBoundaryScan();

    // LOOP 3: New Root Candle Open Scan (aligned to root TF candle opens)
    this.scheduleNewRootCandleScan();
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

  async scanAllForStartup() {
    try {
      logger.info('scanAllForStartup: starting full startup pass (silent)');
      const db = dbModule.get();
      const rows = db.prepare('SELECT symbol FROM symbols ORDER BY symbol COLLATE NOCASE ASC').all();
      for (let i = 0; i < rows.length; i += config.PAGE_SIZE) {
        const page = rows.slice(i, i + config.PAGE_SIZE);
        const tasks = page.map(r => this.scanSymbolRootsWithoutTracking(r.symbol, { notifyImmediately: false, detected_ts: Date.now() }));
        try {
          await Promise.all(tasks);
        } catch (e) {
          logger.debug({ e }, 'scanAllForStartup: page tasks error (continuing)');
        }
      }
      logger.info('scanAllForStartup: completed full startup pass');
    } catch (err) {
      logger.error({ err }, 'scanAllForStartup: unexpected error');
    }
  },

  /**
   * scanSymbolRootsWithoutTracking:
   * Used by LOOP 1 (initial scan) and LOOP 3 (new candle scan)
   * Does NOT use lastRootCandleState tracking, allowing fresh detection
   */
  async scanSymbolRootsWithoutTracking(symbol, { notifyImmediately = true, detected_ts = null } = {}) {
    const tfList = config.ROOT_TFS || [];
    const results = [];
    
    for (const tf of tfList) {
      try {
        const db = dbModule.get();
        const selectStmt = db.prepare('SELECT open_time, close, open FROM klines WHERE symbol=? AND timeframe=? ORDER BY open_time DESC LIMIT 2');
        let rows = selectStmt.all(symbol, tf);
        
        if (!rows || rows.length < 2) {
          logger.debug({ symbol, tf }, 'scanSymbolRootsWithoutTracking: insufficient klines, seeding');
          await this.seedKlinesForSymbol(symbol, tf);
          rows = selectStmt.all(symbol, tf);
          if (!rows || rows.length < 2) {
            logger.debug({ symbol, tf }, 'scanSymbolRootsWithoutTracking: still insufficient after seeding, skipping');
            continue;
          }
        }

        // Check for MACD flip WITHOUT using lastRootCandleState
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
        logger.debug({ err, symbol, tf }, 'scanSymbolRootsWithoutTracking: error');
      }
    }
    return results;
  },

  /**
   * LOOP 2: 5m Boundary Scan
   * - Runs every 5 minutes aligned to UTC boundaries (0, 5, 10, 15, ... 55 minutes)
   * - Scans all symbols for root signals
   * - Sends one telegram per NEW signal detected since last boundary using the per-signal block
   * - Sends MTF alignment/confirmation alerts for monitored root signals
   */
  schedule5mBoundaryScan() {
    const msToNext5 = () => {
      const d = new Date();
      const m = d.getUTCMinutes();
      const next = new Date(d);
      const deltaM = 5 - (m % 5);
      next.setUTCMinutes(m + deltaM);
      // precise alignment at second 0, small offset for scheduling (100ms)
      next.setUTCSeconds(0);
      next.setUTCMilliseconds(100);
      return next - d;
    };

    let firstBoundaryPassed = false;
    let lastBoundarySignalKeys = new Set(); // Track signals from LAST boundary

    const schedule = async () => {
      const wait = msToNext5();
      logger.info({ wait, loopName: 'LOOP_2_5m_boundary_scan', nextBoundary: new Date(Date.now() + wait).toISOString() }, 'Waiting ms until next 5m boundary');
      
      setTimeout(async () => {
        try {
          // execute exact-boundary scan
          await this.scan5mBoundary(lastBoundarySignalKeys);
          
          // Update lastBoundarySignalKeys for next iteration
          const currentSignals = dbModule.getLatestSignalsSnapshot();
          lastBoundarySignalKeys = new Set(currentSignals.map(s => s.key));

          // Close least profitable trade if enabled
          if (config.CLOSE_LEAST_PROFITABLE_ENABLED) {
            const openCount = dbModule.get().prepare('SELECT COUNT(*) as c FROM trades WHERE status = ?').get('open').c || 0;
            if (openCount >= config.MAX_OPEN_TRADES) {
              const minutesSinceHour = new Date().getUTCMinutes();
              const closeMins = config.CLOSE_LEAST_PROFITABLE_MINS_BEFORE_BOUNDARY || 5;
              if (minutesSinceHour >= (60 - closeMins)) {
                logger.info({ openCount, closeMins }, 'LOOP 2: Closing least profitable trade before boundary');
                try {
                  await tradeManager.closeLeastProfitableTrade();
                } catch (err) {
                  logger.error({ err }, 'LOOP 2: Error closing trade');
                }
              }
            }
          }

          if (!firstBoundaryPassed) {
            firstBoundaryPassed = true;
            try {
              signalManager.setOpenTradesAllowed(true);
              logger.info('LOOP 2: Open trades enabled after first 5m boundary');
            } catch (e) {
              logger.debug({ e }, 'LOOP 2: Failed to enable open trades');
            }
          }
        } catch (err) {
          logger.error({ err }, 'LOOP 2: Boundary task failed');
        } finally {
          schedule();
        }
      }, wait);
    };

    schedule();
  },

  /**
   * scan5mBoundary:
   * - Actively SCANS all symbols for root signals at the boundary
   * - Compares newly detected signals against previous boundary
   * - Sends telegram ONLY for signals new since last boundary (one block per signal)
   * - Sends MTF alignment alerts for monitored root signals
   */
  async scan5mBoundary(lastBoundarySignalKeys = new Set()) {
    try {
      const scanStart = Date.now();
      logger.info({ scanStart: new Date(scanStart).toISOString() }, 'LOOP 2: Starting 5m boundary scan');

      // ACTIVELY SCAN all symbols for root signals (seed if needed, compute macd, etc)
      const db = dbModule.get();
      const rows = db.prepare('SELECT symbol FROM symbols ORDER BY symbol COLLATE NOCASE ASC').all();

      for (let i = 0; i < rows.length; i += config.PAGE_SIZE) {
        const page = rows.slice(i, i + config.PAGE_SIZE);
        const tasks = page.map(r => 
          this.scanSymbolRootsWithoutTracking(r.symbol, { notifyImmediately: false, detected_ts: scanStart })
        );
        try {
          await Promise.all(tasks);
        } catch (e) {
          logger.debug({ e }, 'LOOP 2: Page error (continuing)');
        }
      }

      // Get current signals and identify NEW ones (not present in last boundary)
      const currentSignals = dbModule.getLatestSignalsSnapshot();
      const currentKeys = new Set(currentSignals.map(s => s.key));
      const newSignals = currentSignals.filter(s => 
        !lastBoundarySignalKeys.has(s.key) && s.detected_at >= scanStart
      );

      logger.info({ 
        totalSignals: currentSignals.length, 
        previousBoundary: lastBoundarySignalKeys.size, 
        newCount: newSignals.length 
      }, 'LOOP 2: Scan complete');

      const telegram = require('./telegram');

      if (newSignals.length > 0) {
        logger.info({ newCount: newSignals.length }, 'LOOP 2: New signals found, sending telegram blocks');
        // Send exactly one per-signal block for each new signal
        for (let i = 0; i < newSignals.length; i++) {
          const s = newSignals[i];
          try {
            // Use per-signal block function exactly
            if (typeof telegram.sendNewSignalSingleBlock === 'function') {
              await telegram.sendNewSignalSingleBlock(s);
            } else if (typeof telegram.sendSignalBlock === 'function') {
              // fallback older naming if exists
              await telegram.sendSignalBlock(s);
            } else {
              logger.warn({ symbol: s.symbol, tf: s.root_tf }, 'LOOP 2: No telegram per-signal function found - skipping send');
            }
            logger.debug({ symbol: s.symbol, tf: s.root_tf }, 'LOOP 2: Sent signal block');
          } catch (e) {
            logger.warn({ err: e, symbol: s.symbol }, 'LOOP 2: Failed to send block');
          }
          await sleep(config.TELEGRAM_SEND_DELAY_MS || 100);
        }
      } else {
        logger.info('LOOP 2: No new signals found this boundary');
      }

      // Detect MTF alignment/confirmation alerts for monitored root signals
      try {
        const mtfTfs = Array.isArray(config.MTF_TFS) ? config.MTF_TFS.map(String) : [];
        if (mtfTfs.length > 0 && currentSignals && currentSignals.length > 0) {
          // find monitored signals (support multiple possible property names)
          const monitoredSignals = currentSignals.filter(s => s && (s.monitored === 1 || s.monitored === true || s.is_monitored === 1 || s.is_monitored === true || s.monitor === 1 || s.monitor === true));
          for (const msig of monitoredSignals) {
            try {
              const sameSymbolMtf = currentSignals.filter(o => 
                o && o.symbol === msig.symbol && mtfTfs.includes(String(o.root_tf))
              );
              for (const other of sameSymbolMtf) {
                // check if the signal direction matches (support property names)
                const msigSide = msig.side || msig.direction || msig.type || msig.signal;
                const otherSide = other.side || other.direction || other.type || other.signal;
                if (msigSide && otherSide && String(msigSide) === String(otherSide)) {
                  // we have an MTF confirmation/alignment
                  try {
                    if (typeof telegram.sendMtfAlignmentSingleBlock === 'function') {
                      await telegram.sendMtfAlignmentSingleBlock({
                        baseSignal: msig,
                        alignedSignal: other
                      });
                    } else if (typeof telegram.sendNewSignalSingleBlock === 'function') {
                      // fallback: send as per-signal block with an mtf_alignment annotation
                      await telegram.sendNewSignalSingleBlock(Object.assign({}, msig, { mtf_alignment: { aligned_tf: other.root_tf, aligned_with: other.key || other.root_tf } }));
                    } else {
                      logger.warn('LOOP 2: No telegram MTF/per-signal function found - skipping alignment send');
                    }
                    logger.info({ symbol: msig.symbol, root_tf: msig.root_tf, aligned_with: other.root_tf }, 'LOOP 2: Sent MTF alignment alert');
                  } catch (e) {
                    logger.warn({ err: e, symbol: msig.symbol }, 'LOOP 2: Failed to send MTF alignment block');
                  }
                }
              }
            } catch (e) {
              logger.debug({ err: e, symbol: msig && msig.symbol }, 'LOOP 2: Error evaluating MTF for monitored signal (continuing)');
            }
          }
        }
      } catch (e) {
        logger.debug({ e }, 'LOOP 2: MTF alignment detection failed (continuing)');
      }

    } catch (err) {
      logger.error({ err }, 'LOOP 2: Unexpected error');
    }
  },

  /**
   * LOOP 3: New Root Candle Open Scan
   * - Runs at PRECISE root timeframe candle opens
   * - ACTIVELY SCANS for new signals AT candle open time
   * - Uses same notification format as LOOP 1:
   *   - per-signal immediate sends during scan (via notifyImmediately=true)
   *   - then a summary-style update for the relevant candle-open root TF signals (startup-style summary)
   */
  scheduleNewRootCandleScan() {
    const schedule = async () => {
      try {
        const nextEvent = this.calculateNextCandleEvent();
        const waitMs = Math.max(nextEvent.waitMs, 100);
        
        logger.info({ 
          nextTf: nextEvent.tf, 
          nextOpenTime: new Date(nextEvent.nextOpenTime).toISOString(),
          waitMs,
          loopName: 'LOOP_3_new_root_candle_scan' 
        }, 'LOOP 3: Scheduled next candle scan');
        
        await sleep(waitMs);
        
        // Execute scan at candle open
        const detectedTfs = this.detectCurrentCandleOpens();
        if (detectedTfs && detectedTfs.length > 0) {
          logger.info({ detectedTfs, time: new Date().toISOString() }, 'LOOP 3: Candle(s) detected as open');
          await this.scanAndNotifyNewCandles(detectedTfs);
        } else {
          logger.debug('LOOP 3: No candles detected as open this cycle');
        }
      } catch (err) {
        logger.error({ err }, 'LOOP 3: Unexpected error');
      } finally {
        // Reschedule immediately
        schedule();
      }
    };

    schedule();
  },

  /**
   * scanAndNotifyNewCandles:
   * - ACTIVELY SCANS all symbols for root signals on newly opened TFs
   * - Uses the same notification pattern as LOOP 1:
   *   - during scan: per-signal immediate sends via signalManager.handleRootSignal (notifyImmediately=true)
   *   - after scan: sends a summary-style update for the relevant candle-open root TF signals
   * - Also sends MTF alignment alerts for monitored signals
   */
  async scanAndNotifyNewCandles(detectedTfs = []) {
    try {
      if (!detectedTfs || detectedTfs.length === 0) {
        logger.info('LOOP 3: No TFs to scan');
        return;
      }

      logger.info({ detectedTfs, time: new Date().toISOString() }, 'LOOP 3: Starting active scan for new candle signals');
      const scanStart = Date.now();

      // ACTIVELY SCAN all symbols for signals on the newly opened TFs
      // NOTE: For Loop 3 we run notifyImmediately=true so signalManager will send per-signal blocks during scanning (same as Loop 1)
      const db = dbModule.get();
      const rows = db.prepare('SELECT symbol FROM symbols ORDER BY symbol COLLATE NOCASE ASC').all();

      for (let i = 0; i < rows.length; i += config.PAGE_SIZE) {
        const page = rows.slice(i, i + config.PAGE_SIZE);
        const tasks = page.map(r => 
          this.scanSymbolRootsWithoutTracking(r.symbol, { notifyImmediately: true, detected_ts: scanStart })
        );
        try {
          await Promise.all(tasks);
        } catch (e) {
          logger.debug({ e }, 'LOOP 3: Page error (continuing)');
        }
      }

      // After per-symbol immediate sends, build a snapshot of relevant signals for these TFs and send a summary (same format as Loop 1 startup summary)
      try {
        const allSignals = dbModule.getLatestSignalsSnapshot();
        const candleSignals = allSignals.filter(s => 
          detectedTfs.includes(String(s.root_tf)) && s.detected_at >= scanStart
        );

        logger.info({ 
          detectedTfs, 
          foundCount: candleSignals.length 
        }, 'LOOP 3: Scan complete');

        if (candleSignals.length > 0) {
          try {
            const telegram = require('./telegram');
            // sendStartupSummary expects a snapshot; we provide the filtered snapshot for these candle opens
            if (typeof telegram.sendStartupSummary === 'function') {
              await telegram.sendStartupSummary({ snapshot: candleSignals });
              logger.info({ count: candleSignals.length }, 'LOOP 3: Sent summary-style update for candle-open signals');
            } else {
              // Fallback: send per-signal blocks again if summary function is not available
              for (const s of candleSignals) {
                try {
                  if (typeof telegram.sendNewSignalSingleBlock === 'function') {
                    await telegram.sendNewSignalSingleBlock(s);
                  } else if (typeof telegram.sendSignalBlock === 'function') {
                    await telegram.sendSignalBlock(s);
                  }
                  await sleep(config.TELEGRAM_SEND_DELAY_MS || 100);
                } catch (e) {
                  logger.warn({ err: e, symbol: s.symbol }, 'LOOP 3: Failed fallback per-signal send');
                }
              }
            }
          } catch (e) {
            logger.warn({ err: e }, 'LOOP 3: Failed to send summary-style update (continuing)');
          }
        } else {
          logger.info({ detectedTfs }, 'LOOP 3: No signals detected on these candle opens');
        }

        // Also detect MTF alignments like LOOP 2 (for monitored signals)
        try {
          const mtfTfs = Array.isArray(config.MTF_TFS) ? config.MTF_TFS.map(String) : [];
          if (mtfTfs.length > 0 && allSignals && allSignals.length > 0) {
            const monitoredSignals = allSignals.filter(s => s && (s.monitored === 1 || s.monitored === true || s.is_monitored === 1 || s.is_monitored === true || s.monitor === 1 || s.monitor === true));
            const telegram = require('./telegram');
            for (const msig of monitoredSignals) {
              try {
                const sameSymbolMtf = allSignals.filter(o => 
                  o && o.symbol === msig.symbol && mtfTfs.includes(String(o.root_tf))
                );
                for (const other of sameSymbolMtf) {
                  const msigSide = msig.side || msig.direction || msig.type || msig.signal;
                  const otherSide = other.side || other.direction || other.type || other.signal;
                  if (msigSide && otherSide && String(msigSide) === String(otherSide)) {
                    try {
                      if (typeof telegram.sendMtfAlignmentSingleBlock === 'function') {
                        await telegram.sendMtfAlignmentSingleBlock({
                          baseSignal: msig,
                          alignedSignal: other
                        });
                      } else if (typeof telegram.sendNewSignalSingleBlock === 'function') {
                        await telegram.sendNewSignalSingleBlock(Object.assign({}, msig, { mtf_alignment: { aligned_tf: other.root_tf, aligned_with: other.key || other.root_tf } }));
                      } else {
                        logger.warn('LOOP 3: No telegram MTF/per-signal function found - skipping alignment send');
                      }
                      logger.info({ symbol: msig.symbol, root_tf: msig.root_tf, aligned_with: other.root_tf }, 'LOOP 3: Sent MTF alignment alert');
                    } catch (e) {
                      logger.warn({ err: e, symbol: msig.symbol }, 'LOOP 3: Failed to send MTF alignment block');
                    }
                  }
                }
              } catch (e) {
                logger.debug({ err: e, symbol: msig && msig.symbol }, 'LOOP 3: Error evaluating MTF for monitored signal (continuing)');
              }
            }
          }
        } catch (e) {
          logger.debug({ e }, 'LOOP 3: MTF alignment detection failed (continuing)');
        }

      } catch (err) {
        logger.error({ err }, 'LOOP 3: post-scan processing error');
      }

    } catch (err) {
      logger.error({ err }, 'LOOP 3: scanAndNotifyNewCandles error');
    }
  },

  /**
   * Helper: Convert timeframe string to milliseconds
   */
  timeframeToMs(tf) {
    if (!tf) return -1;
    const tfStr = String(tf).toUpperCase();
    if (tfStr === 'D') return 24 * 60 * 60 * 1000;
    const match = tfStr.match(/^(\d+)([MHWD])$/);
    if (!match) return -1;
    const num = parseInt(match[1], 10);
    const unit = match[2];
    switch (unit) {
      case 'M': return num * 60 * 1000;
      case 'H': return num * 60 * 60 * 1000;
      case 'W': return num * 7 * 24 * 60 * 60 * 1000;
      case 'D': return num * 24 * 60 * 60 * 1000;
      default: return -1;
    }
  },

  /**
   * calculateNextCandleEvent:
   * - Calculate NEXT candle open time across ALL root TFs
   * - Factor in 100ms buffer for system latency
   */
  calculateNextCandleEvent() {
    const nowMs = Date.now();
    let nextOpenMs = Infinity;
    let nextTf = null;

    for (const tf of (config.ROOT_TFS || [])) {
      const tfMs = this.timeframeToMs(tf);
      if (tfMs <= 0) continue;

      const epochFloor = Math.floor(nowMs / tfMs);
      const candidateMs = (epochFloor + 1) * tfMs;

      if (candidateMs < nextOpenMs) {
        nextOpenMs = candidateMs;
        nextTf = tf;
      }
    }

    if (nextOpenMs === Infinity) {
      return {
        tf: '1m',
        nextOpenTime: nowMs + 60000,
        waitMs: 60000
      };
    }

    const bufferMs = 100;
    const waitMs = Math.max(nextOpenMs - nowMs - bufferMs, 100);

    return {
      tf: nextTf,
      nextOpenTime: nextOpenMs,
      waitMs
    };
  },

  /**
   * detectCurrentCandleOpens:
   * - Check which root TFs are currently opening (within ±2 second window)
   */
  detectCurrentCandleOpens() {
    const nowMs = Date.now();
    const detectedTfs = [];
    const openWindowMs = 2000;

    for (const tf of (config.ROOT_TFS || [])) {
      const tfMs = this.timeframeToMs(tf);
      if (tfMs <= 0) continue;

      const epochFloor = Math.floor(nowMs / tfMs);
      const candleOpenMs = epochFloor * tfMs;
      const timeSinceCandleOpen = nowMs - candleOpenMs;

      if (timeSinceCandleOpen >= -openWindowMs && timeSinceCandleOpen <= openWindowMs) {
        detectedTfs.push(tf);
        logger.debug({ 
          tf, 
          timeSinceCandleOpen,
          candleOpenTime: new Date(candleOpenMs).toISOString()
        }, 'LOOP 3: Detected candle open');
      }
    }

    return detectedTfs;
  }
};
