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
let lastRootCandleState = {}; // Track last root candle per symbol:tf to prevent duplicate flips
let last5mBoundaryScan = {}; // Track signals from last 5m boundary scan per symbol

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

    // ===== LOOP 1: INITIAL/DEPLOY SCAN (runs once) =====
    (async () => {
      try {
        logger.info('poller: LOOP1 - initialScan starting');
        await this.initialScan();
        logger.info('poller: LOOP1 - initialScan completed');
      } catch (err) {
        logger.error({ err }, 'poller: LOOP1 - initialScan failed');
      }
    })();

    // ===== LOOP 2: 5M BOUNDARY SCAN (runs forever) =====
    this.scheduleAlignedTo5mBoundaryScan();

    // ===== LOOP 3: NEW ROOT CANDLE OPEN SCAN (runs forever) =====
    this.scheduleNewRootCandleOpenScan();
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

    // Seed klines for all symbols
    const seedSymbols = bybit.getSeedSymbols(allSymbols);
    if (seedSymbols && seedSymbols.length) {
      logger.info({ count: seedSymbols.length }, 'poller.initialScan: seeding klines');
      await this.backgroundSeedKlines(seedSymbols);
    } else {
      logger.info('poller.initialScan: no seed symbols to process');
    }

    // Scan all symbols for initial signals - LOOP 1 always sends telegram
    logger.info('poller.initialScan: scanning all symbols for initial signals');
    const initialSignals = await this.scanAllSymbolsForSignals(allSymbols.map(s => s.symbol), {
      sendTelegram: true,
      scanName: 'LOOP1-INITIAL'
    });

    logger.info({ count: initialSignals.length }, 'poller.initialScan: initial scan complete with signals');

    // Enable trades after initial scan and schedule aligned 5m boundary scans
    try {
      signalManager.setOpenTradesAllowed(true);
      logger.info('poller.initialScan: open trades enabled after initial scan');
    } catch (e) {
      logger.debug({ e }, 'poller.initialScan: failed to set open trades allowed');
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

  // ===== LOOP 2: 5M BOUNDARY SCAN =====
  scheduleAlignedTo5mBoundaryScan() {
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

    const schedule = async () => {
      const wait = msToNext5();
      logger.info({ wait, loopName: 'LOOP2-5M' }, 'scheduleAlignedTo5mBoundaryScan: waiting ms until next 5m boundary');
      
      setTimeout(async () => {
        try {
          const scanStart = Date.now();
          logger.info({ loopName: 'LOOP2-5M', timestamp: new Date().toISOString() }, 'scheduleAlignedTo5mBoundaryScan: executing at 5m boundary');

          // Get all symbols
          const db = dbModule.get();
          const rows = db.prepare('SELECT symbol FROM symbols ORDER BY symbol COLLATE NOCASE ASC').all();
          const symbols = rows.map(r => r.symbol);

          // Scan all symbols for new signals
          const newSignals = await this.scanAllSymbolsForSignals(symbols, {
            sendTelegram: true,
            scanName: 'LOOP2-5M-BOUNDARY',
            sendPerBlock: true // Send 1 signal per block format (no summary)
          });

          logger.info({ 
            loopName: 'LOOP2-5M', 
            newSignalsCount: newSignals.length,
            timestamp: new Date().toISOString()
          }, 'scheduleAlignedTo5mBoundaryScan: boundary scan completed');

          // Detect and send MTF alignment confirmations from monitored signals
          try {
            await this.checkAndSendMtfAlignmentAlerts(symbols);
          } catch (err) {
            logger.debug({ err, loopName: 'LOOP2-5M' }, 'scheduleAlignedTo5mBoundaryScan: MTF alignment check failed');
          }

          // Close least profitable trade if enabled and max trades filled
          if (config.CLOSE_LEAST_PROFITABLE_ENABLED && symbols.length > 0) {
            const openCount = dbModule.get().prepare('SELECT COUNT(*) as c FROM trades WHERE status = ?').get('open').c || 0;
            if (openCount >= config.MAX_OPEN_TRADES) {
              const minutesSinceHour = new Date().getUTCMinutes();
              const closeMins = config.CLOSE_LEAST_PROFITABLE_MINS_BEFORE_BOUNDARY || 5;
              if (minutesSinceHour >= (60 - closeMins)) {
                logger.info({ openCount, closeMins, loopName: 'LOOP2-5M' }, 'Closing least profitable trade before boundary');
                try {
                  await tradeManager.closeLeastProfitableTrade();
                } catch (err) {
                  logger.error({ err, loopName: 'LOOP2-5M' }, 'Error closing least profitable trade');
                }
              }
            }
          }

          try {
            dbModule.setState('lastScanAt', scanStart);
            dbModule.setState('lastScanSignals', newSignals.map(s => s.key || `${s.symbol}:${s.root_tf}`));
          } catch (e) {
            logger.debug({ e }, 'scheduleAlignedTo5mBoundaryScan: failed to persist scan state');
          }
        } catch (err) {
          logger.error({ err, loopName: 'LOOP2-5M' }, 'scheduleAlignedTo5mBoundaryScan: boundary task failed');
        } finally {
          schedule();
        }
      }, wait);
    };

    schedule();
  },

  // ===== LOOP 3: NEW ROOT CANDLE OPEN SCAN =====
  scheduleNewRootCandleOpenScan() {
    const msToNextRootCandle = () => {
      const d = new Date();
      const rootTfs = config.ROOT_TFS || ['D', '4h', '1h'];
      let nextOpen = null;

      for (const tf of rootTfs) {
        let tfMs;
        if (String(tf).toUpperCase() === 'D') {
          // Next daily candle at UTC 00:00
          const next = new Date(d);
          next.setUTCDate(next.getUTCDate() + 1);
          next.setUTCHours(0, 0, 0, 0);
          tfMs = next - d;
        } else {
          const tfNum = Number(tf);
          if (!isNaN(tfNum)) {
            const minutesSinceEpoch = Math.floor(d.getTime() / 60000);
            const nextCandleNum = Math.ceil((minutesSinceEpoch + 1) / tfNum);
            const nextCandleMs = nextCandleNum * tfNum * 60000;
            tfMs = nextCandleMs - d.getTime();
          }
        }
        if (tfMs && (!nextOpen || tfMs < nextOpen)) {
          nextOpen = tfMs;
        }
      }
      return nextOpen || 60000; // fallback 1m
    };

    const schedule = async () => {
      const wait = msToNextRootCandle();
      logger.info({ wait, loopName: 'LOOP3-ROOTCANDLE' }, 'scheduleNewRootCandleOpenScan: waiting ms until next root candle open');
      
      setTimeout(async () => {
        try {
          const scanStart = Date.now();
          const now = new Date();
          logger.info({ 
            loopName: 'LOOP3-ROOTCANDLE',
            timestamp: now.toISOString()
          }, 'scheduleNewRootCandleOpenScan: executing at root candle open');

          // Determine which root TFs just opened
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

          if (newRootTfs.length === 0) {
            logger.debug({ loopName: 'LOOP3-ROOTCANDLE' }, 'scheduleNewRootCandleOpenScan: no root candles opened this boundary');
          } else {
            logger.info({ newRootTfs, loopName: 'LOOP3-ROOTCANDLE' }, 'scheduleNewRootCandleOpenScan: root candles opened');

            // Get all symbols
            const db = dbModule.get();
            const rows = db.prepare('SELECT symbol FROM symbols ORDER BY symbol COLLATE NOCASE ASC').all();
            const symbols = rows.map(r => r.symbol);

            // Seed klines for the newly opened root TFs
            const seedSymbols = bybit.getSeedSymbols(
              symbols.map(s => ({ symbol: s, base: s, quote: 'USDT' }))
            );
            if (seedSymbols && seedSymbols.length) {
              logger.info({ 
                count: seedSymbols.length, 
                tfs: newRootTfs,
                loopName: 'LOOP3-ROOTCANDLE'
              }, 'scheduleNewRootCandleOpenScan: seeding klines for new root candles');
              
              for (const seedSym of seedSymbols) {
                for (const tf of newRootTfs) {
                  try {
                    await this.seedKlinesForSymbol(seedSym.symbol, tf);
                  } catch (err) {
                    logger.debug({ 
                      err, 
                      symbol: seedSym.symbol, 
                      tf,
                      loopName: 'LOOP3-ROOTCANDLE'
                    }, 'scheduleNewRootCandleOpenScan: seed failed for symbol/tf');
                  }
                }
              }
            }

            // Scan all symbols for new signals on newly opened root candles
            const newSignals = await this.scanSymbolsForNewRootCandles(symbols, newRootTfs, {
              sendTelegram: true,
              scanName: 'LOOP3-ROOTCANDLE-OPEN',
              sendPerBlock: true // Send 1 signal per block format
            });

            logger.info({ 
              loopName: 'LOOP3-ROOTCANDLE',
              newSignalsCount: newSignals.length,
              timestamp: new Date().toISOString()
            }, 'scheduleNewRootCandleOpenScan: root candle scan completed');
          }
        } catch (err) {
          logger.error({ err, loopName: 'LOOP3-ROOTCANDLE' }, 'scheduleNewRootCandleOpenScan: root candle task failed');
        } finally {
          schedule();
        }
      }, wait);
    };

    schedule();
  },

  // ===== SHARED SCANNING FUNCTIONS =====

  async scanAllSymbolsForSignals(symbols, { sendTelegram = false, scanName = '', sendPerBlock = false } = {}) {
    const results = [];
    try {
      const db = dbModule.get();
      const prevSnapshot = db.getLatestSignalsSnapshot();
      const prevKeys = new Set(prevSnapshot.map(r => r.key || `${r.symbol}:${r.root_tf}`));

      for (let i = 0; i < symbols.length; i += config.PAGE_SIZE) {
        const page = symbols.slice(i, i + config.PAGE_SIZE);
        const tasks = page.map(symbol => 
          this.scanSymbolRoots(symbol, { 
            notifyImmediately: false,
            detected_ts: Date.now(),
            scanName
          })
        );
        try {
          const pageResults = await Promise.all(tasks);
          for (const res of pageResults) {
            if (res && Array.isArray(res)) results.push(...res);
          }
        } catch (e) {
          logger.debug({ e, scanName }, 'scanAllSymbolsForSignals: page tasks error (continuing)');
        }
      }

      // Send telegram notifications
      if (sendTelegram && results.length > 0) {
        const telegram = require('./telegram');
        
        if (sendPerBlock) {
          // Send 1 signal per block (new signals only, not in previous scan)
          for (const sig of results) {
            const sigKey = sig.key || `${sig.symbol}:${sig.root_tf}`;
            if (!prevKeys.has(sigKey)) {
              try {
                logger.debug({ 
                  scanName, 
                  signal: sigKey,
                  blockType: 'new_signal_block'
                }, 'scanAllSymbolsForSignals: sending new signal block');
                
                await telegram.sendNewSignalSingleBlock(sig);
              } catch (e) {
                logger.debug({ e, sig, scanName }, 'scanAllSymbolsForSignals: failed to send signal block');
              }
              await sleep(config.TELEGRAM_SEND_DELAY_MS || 100);
            }
          }
        } else {
          // Send all signals (initial scan mode - send all blocks)
          for (const sig of results) {
            try {
              logger.debug({ 
                scanName,
                signal: `${sig.symbol}:${sig.root_tf}`,
                blockType: 'new_signal_block'
              }, 'scanAllSymbolsForSignals: sending signal block');
              
              await telegram.sendNewSignalSingleBlock(sig);
            } catch (e) {
              logger.debug({ e, sig, scanName }, 'scanAllSymbolsForSignals: failed to send signal block');
            }
            await sleep(config.TELEGRAM_SEND_DELAY_MS || 100);
          }
        }
      }
    } catch (err) {
      logger.error({ err, scanName }, 'scanAllSymbolsForSignals: unexpected error');
    }
    return results;
  },

  async scanSymbolsForNewRootCandles(symbols, newRootTfs, { sendTelegram = false, scanName = '', sendPerBlock = false } = {}) {
    const results = [];
    try {
      logger.info({ 
        symbolCount: symbols.length, 
        newRootTfs, 
        scanName 
      }, 'scanSymbolsForNewRootCandles: starting scan for newly opened root candles');

      const db = dbModule.get();
      const prevSnapshot = db.getLatestSignalsSnapshot();
      const prevKeys = new Set(prevSnapshot.map(r => r.key || `${r.symbol}:${r.root_tf}`));

      for (let i = 0; i < symbols.length; i += config.PAGE_SIZE) {
        const page = symbols.slice(i, i + config.PAGE_SIZE);
        const tasks = page.map(symbol => 
          this.scanSymbolRootsForSpecificTfs(symbol, newRootTfs, {
            notifyImmediately: false,
            detected_ts: Date.now(),
            scanName
          })
        );
        try {
          const pageResults = await Promise.all(tasks);
          for (const res of pageResults) {
            if (res && Array.isArray(res)) results.push(...res);
          }
        } catch (e) {
          logger.debug({ e, scanName }, 'scanSymbolsForNewRootCandles: page tasks error (continuing)');
        }
      }

      // Send telegram notifications
      if (sendTelegram && results.length > 0) {
        const telegram = require('./telegram');
        
        for (const sig of results) {
          const sigKey = sig.key || `${sig.symbol}:${sig.root_tf}`;
          if (!prevKeys.has(sigKey)) {
            try {
              logger.debug({ 
                scanName,
                signal: sigKey,
                blockType: 'new_signal_block'
              }, 'scanSymbolsForNewRootCandles: sending new signal block');
              
              await telegram.sendNewSignalSingleBlock(sig);
            } catch (e) {
              logger.debug({ e, sig, scanName }, 'scanSymbolsForNewRootCandles: failed to send signal block');
            }
            await sleep(config.TELEGRAM_SEND_DELAY_MS || 100);
          }
        }
      }
    } catch (err) {
      logger.error({ err, scanName }, 'scanSymbolsForNewRootCandles: unexpected error');
    }
    return results;
  },

  async scanSymbolRoots(symbol, { notifyImmediately = true, detected_ts = null, scanName = '' } = {}) {
    const tfList = config.ROOT_TFS || [];
    const results = [];
    for (const tf of tfList) {
      try {
        const db = dbModule.get();
        const selectStmt = db.prepare('SELECT open_time, close, open FROM klines WHERE symbol=? AND timeframe=? ORDER BY open_time DESC LIMIT 2');
        let rows = selectStmt.all(symbol, tf);
        
        if (!rows || rows.length < 2) {
          logger.debug({ symbol, tf, scanName }, 'scanSymbolRoots: insufficient klines, seeding now');
          await this.seedKlinesForSymbol(symbol, tf);
          rows = selectStmt.all(symbol, tf);
          
          if (!rows || rows.length < 2) {
            logger.debug({ symbol, tf, scanName }, 'scanSymbolRoots: still insufficient klines after seeding, skipping tf');
            continue;
          } else {
            logger.info({ symbol, tf, scanName }, 'scanSymbolRoots: klines seeded, re-checking flip');
          }
        }

        // Track last root candle to prevent duplicate flip detections
        const stateKey = `${symbol}:${tf}`;
        const latestCandleTime = rows[0].open_time; // Latest in DESC order
        
        if (lastRootCandleState[stateKey] === latestCandleTime) {
          logger.debug({ 
            symbol, 
            tf, 
            candleTime: latestCandleTime,
            scanName
          }, 'scanSymbolRoots: already processed this candle, skipping');
          continue;
        }
        
        lastRootCandleState[stateKey] = latestCandleTime;

        const flip = await require('./macd').isMacdFlip(symbol, tf);
        if (flip) {
          const sig = await signalManager.handleRootSignal({
            symbol,
            root_tf: tf,
            detected_at: detected_ts || Date.now(),
            notifyImmediately: false // Never notify immediately from here; handle via telegram in scan functions
          });
          if (sig) results.push(sig);
        }
      } catch (err) {
        logger.debug({ err, symbol, tf, scanName }, 'scanSymbolRoots: error checking flip');
      }
    }
    return results;
  },

  async scanSymbolRootsForSpecificTfs(symbol, rootTfs, { notifyImmediately = false, detected_ts = null, scanName = '' } = {}) {
    const results = [];
    for (const tf of rootTfs) {
      try {
        const db = dbModule.get();
        const selectStmt = db.prepare('SELECT open_time, close, open FROM klines WHERE symbol=? AND timeframe=? ORDER BY open_time DESC LIMIT 2');
        let rows = selectStmt.all(symbol, tf);
        
        if (!rows || rows.length < 2) {
          logger.debug({ symbol, tf, scanName }, 'scanSymbolRootsForSpecificTfs: insufficient klines, seeding now');
          await this.seedKlinesForSymbol(symbol, tf);
          rows = selectStmt.all(symbol, tf);
          
          if (!rows || rows.length < 2) {
            logger.debug({ symbol, tf, scanName }, 'scanSymbolRootsForSpecificTfs: still insufficient after seeding, skipping');
            continue;
          }
        }

        // For new root candle scan, reset the state tracking to allow re-detection on new candle
        const stateKey = `${symbol}:${tf}`;
        const latestCandleTime = rows[0].open_time;
        
        if (lastRootCandleState[stateKey] === latestCandleTime) {
          logger.debug({ 
            symbol, 
            tf, 
            candleTime: latestCandleTime,
            scanName
          }, 'scanSymbolRootsForSpecificTfs: already processed this candle, skipping');
          continue;
        }
        
        lastRootCandleState[stateKey] = latestCandleTime;

        const flip = await require('./macd').isMacdFlip(symbol, tf);
        if (flip) {
          const sig = await signalManager.handleRootSignal({
            symbol,
            root_tf: tf,
            detected_at: detected_ts || Date.now(),
            notifyImmediately: false
          });
          if (sig) results.push(sig);
        }
      } catch (err) {
        logger.debug({ err, symbol, tf, scanName }, 'scanSymbolRootsForSpecificTfs: error checking flip');
      }
    }
    return results;
  },

  async checkAndSendMtfAlignmentAlerts(symbols) {
    try {
      logger.info({ symbolCount: symbols.length }, 'checkAndSendMtfAlignmentAlerts: checking MTF alignments');
      
      const db = dbModule.get();
      const mtfTfs = Array.isArray(config.MTF_TFS) ? config.MTF_TFS.map(String) : [];
      
      if (!mtfTfs || mtfTfs.length === 0) {
        logger.debug('checkAndSendMtfAlignmentAlerts: no MTF timeframes configured, skipping');
        return;
      }

      const telegram = require('./telegram');
      let alertsSent = 0;

      for (const symbol of symbols) {
        try {
          // Get all root signals for this symbol
          const rootSignals = db.prepare(
            'SELECT root_tf, detected_at FROM signals WHERE symbol = ? AND root_tf IN (' + 
            config.ROOT_TFS.map(() => '?').join(',') + 
            ') ORDER BY detected_at DESC LIMIT 5'
          ).all(symbol, ...config.ROOT_TFS);

          if (!rootSignals || rootSignals.length === 0) continue;

          // For each root signal, check MTF confirmations
          for (const rootSig of rootSignals) {
            const mtfConfirmations = [];
            
            for (const mtfTf of mtfTfs) {
              try {
                const isMtfFlip = await require('./macd').isMacdFlip(symbol, mtfTf);
                if (isMtfFlip) {
                  mtfConfirmations.push(mtfTf);
                }
              } catch (err) {
                logger.debug({ err, symbol, mtfTf }, 'checkAndSendMtfAlignmentAlerts: MTF check failed');
              }
            }

            if (mtfConfirmations.length > 0) {
              try {
                logger.info({ 
                  symbol, 
                  rootTf: rootSig.root_tf,
                  mtfConfirmations
                }, 'checkAndSendMtfAlignmentAlerts: sending MTF alignment alert');

                // Send MTF alignment alert block via telegram
                await telegram.sendMtfAlignmentAlert({
                  symbol,
                  root_tf: rootSig.root_tf,
                  mtf_confirmations: mtfConfirmations,
                  detected_at: rootSig.detected_at
                });
                
                alertsSent++;
              } catch (e) {
                logger.debug({ e, symbol }, 'checkAndSendMtfAlignmentAlerts: failed to send MTF alert');
              }
              await sleep(config.TELEGRAM_SEND_DELAY_MS || 100);
            }
          }
        } catch (err) {
          logger.debug({ err, symbol }, 'checkAndSendMtfAlignmentAlerts: error processing symbol');
        }
      }

      logger.info({ alertsSent }, 'checkAndSendMtfAlignmentAlerts: completed');
    } catch (err) {
      logger.error({ err }, 'checkAndSendMtfAlignmentAlerts: unexpected error');
    }
  }
};
