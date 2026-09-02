// src/services/bybitWs.js
/**
 * Batched Bybit WebSocket manager (v5-aware).
 *
 * - Groups symbols into connections up to config.BATCH_WS_SIZE per connection.
 * - Caps total connections at config.MAX_CONCURRENT_WS.
 * - Subscribes to both klineV2.<interval>.<symbol> and fallback kline.<interval>.<symbol>.
 * - Emits 'kline' events: payload { symbol, timeframe, data, raw } where data has { open_time, open, high, low, close, volume }.
 * - Provides closeAll() for graceful shutdown.
 * - Provides performInitialScan() for WS-based symbol discovery.
 *
 * WS host selection:
 * - BYBIT_WS_PUBLIC (env or config) wins if present.
 * - Otherwise MAINNET env controls default WS host:
 *     MAINNET=true  -> mainnet WS
 *     MAINNET=false -> testnet WS
 *
 * Improvements:
 * - Queue subscribe/unsubscribe topics until socket is OPEN (avoid readyState 0 errors).
 * - Chunk subscribe/unsubscribe messages to avoid huge payloads.
 * - Exponential backoff retry for failed sends.
 * - Better logging around pending/flush/retry events.
 * - performInitialScan() returns discovered symbols from WS subscriptions.
 */

const WebSocket = require('ws');
const EventEmitter = require('events');
const config = require('../config');
const logger = require('pino')();

/* Helper to interpret env boolean */
function envBool(name, defaultVal = false) {
  if (typeof process.env[name] === 'undefined') return defaultVal;
  const v = String(process.env[name]).toLowerCase().trim();
  return v === '1' || v === 'true' || v === 'yes';
}

const MAINNET = envBool('MAINNET', true);

/* Choose websocket URL:
   - env BYBIT_WS_PUBLIC or config.BYBIT_WS_PUBLIC takes precedence
   - otherwise default to mainnet/testnet standard endpoints
*/
function getWsUrl() {
  const explicit = process.env.BYBIT_WS_PUBLIC || (config && config.BYBIT_WS_PUBLIC);
  if (explicit) return String(explicit);

  // Default safe WS endpoints (v5 public streaming endpoints)
  return MAINNET ? 'wss://stream.bybit.com/v5/public/linear' : 'wss://stream-testnet.bybit.com/v5/public/linear';
}

/* Subscription send chunk size and retry/backoff configuration */
const WS_SUBSCRIBE_CHUNK = config.WS_SUBSCRIBE_CHUNK || (process.env.WS_SUBSCRIBE_CHUNK ? Number(process.env.WS_SUBSCRIBE_CHUNK) : 50);
const WS_SUBSCRIBE_RETRY_BASE_MS = config.WS_SUBSCRIBE_RETRY_BASE_MS || (process.env.WS_SUBSCRIBE_RETRY_BASE_MS ? Number(process.env.WS_SUBSCRIBE_RETRY_BASE_MS) : 1000);
const WS_SUBSCRIBE_RETRY_MAX_MS = config.WS_SUBSCRIBE_RETRY_MAX_MS || (process.env.WS_SUBSCRIBE_RETRY_MAX_MS ? Number(process.env.WS_SUBSCRIBE_RETRY_MAX_MS) : 60000);

class WSManager extends EventEmitter {
  constructor() {
    super();
    this.connections = []; // array of connection objects
    this.symbolToConn = new Map(); // symbol -> connection object
    this.openSockets = 0;
    this.maxSockets = config.MAX_CONCURRENT_WS || 20;
    this.batchSize = config.BATCH_WS_SIZE || 20;
    this.klineBuffer = new Map(); // symbol -> latest kline data (for performInitialScan)
  }

  start() {
    logger.info({ wsUrl: getWsUrl(), maxSockets: this.maxSockets, batchSize: this.batchSize }, 'WS Manager ready (batched)');
  }

  intervalToTopicPart(tf) {
    return tf === 'D' ? 'D' : String(tf);
  }

  _createConnection() {
    if (this.openSockets >= this.maxSockets) {
      logger.warn({ maxSockets: this.maxSockets }, 'Max WS connections reached; cannot create new connection');
      return null;
    }

    const wsUrl = getWsUrl();
    const ws = new WebSocket(wsUrl);
    const conn = {
      ws,
      symbols: new Set(),
      id: Date.now() + '-' + Math.random().toString(16).slice(2),
      _topics: new Set(),         // authoritative set of subscribed topics (unique)
      pendingTopics: new Set(),   // topics pending to be sent (queue until OPEN and successful)
      ready: false,
      retryDelayMs: WS_SUBSCRIBE_RETRY_BASE_MS,
      _retryTimer: null
    };

    ws.on('open', () => {
      conn.ready = true;
      logger.info({ connId: conn.id, wsUrl }, 'WS connection opened');
      // flush any pending topics (if any)
      if (conn.pendingTopics && conn.pendingTopics.size) {
        logger.info({ connId: conn.id, pending: conn.pendingTopics.size }, 'Flushing pending subscribe topics on open');
        this._flushPendingForConn(conn);
      }
    });

    ws.on('message', (msg) => {
      try {
        const data = JSON.parse(msg);
        
        // Handle ping/pong
        if (data && data.op === 'ping') {
          try { ws.send(JSON.stringify({ op: 'pong' })); } catch (e) { /* ignore */ }
          return;
        }

        // Handle kline data
        if (data && data.topic && Array.isArray(data.data) && data.data.length > 0) {
          const topic = String(data.topic);
          const topicParts = topic.split('.');
          
          // Expected format: kline.<interval>.<symbol> or klineV2.<interval>.<symbol>
          if ((topicParts[0] === 'kline' || topicParts[0] === 'klineV2') && topicParts.length >= 3) {
            const tf = topicParts[1];
            const sym = topicParts.slice(2).join('.');
            
            if (data.data[0]) {
              const d = data.data[0];
              const k = this.normalizeKlinePayload(d, tf, sym);
              
              // Buffer latest kline for this symbol
              if (!this.klineBuffer.has(sym)) {
                this.klineBuffer.set(sym, {});
              }
              this.klineBuffer.get(sym)[tf] = k;
              
              // Emit kline event
              this.emit('kline', { symbol: sym, timeframe: tf, data: k, raw: data });
              logger.debug({ symbol: sym, timeframe: tf, close: k.close }, 'Kline received');
            }
          }
        } else if (data && data.ret_code !== undefined) {
          // Handle subscription confirmation or error
          if (data.ret_code !== 0) {
            logger.warn({ connId: conn.id, retCode: data.ret_code, retMsg: data.ret_msg }, 'WS subscription error');
          } else {
            logger.debug({ connId: conn.id, op: data.op }, 'WS operation successful');
          }
        }
      } catch (err) {
        logger.debug({ err: err && err.message ? err.message : err }, 'Failed to parse WS message');
      }
    });

    ws.on('error', (err) => {
      logger.warn({ err: err && err.message ? err.message : err, connId: conn.id }, 'WS error');
    });

    ws.on('close', (code, reason) => {
      logger.info({ connId: conn.id, code, reason: reason ? reason.toString() : '' }, 'WS connection closed');

      // capture symbols that were assigned to this connection so we can re-subscribe them
      const symbolsToRecover = Array.from(conn.symbols || []);
      for (const s of conn.symbols) this.symbolToConn.delete(s);

      // remove conn from active list
      this.connections = this.connections.filter(c => c !== conn);
      this.openSockets = Math.max(0, this.openSockets - 1);

      // Attempt to re-subscribe symbols that were on this connection by re-calling subscribeSymbolMTF()
      // This will place them onto new connections (respecting batching)
      if (symbolsToRecover && symbolsToRecover.length) {
        logger.info({ connId: conn.id, recoverCount: symbolsToRecover.length }, 'Re-queueing symbols from closed connection for re-subscription');
        // small delay to avoid busy loop on immediate reconnects
        setTimeout(() => {
          for (const sym of symbolsToRecover) {
            try {
              // re-subscribe; subscribeSymbolMTF will allocate to existing/new conn
              this.subscribeSymbolMTF(sym);
            } catch (e) {
              logger.debug({ err: e, symbol: sym }, 'Error while re-subscribing symbol after close');
            }
          }
        }, 500);
      }
    });

    this.connections.push(conn);
    this.openSockets++;
    logger.info({ connId: conn.id, openSockets: this.openSockets }, 'WS connection created');
    return conn;
  }

  /**
   * Send subscribe/unsubscribe messages in chunks, with retry on failure.
   * Internal helper that actually attempts to send a batch of topics.
   */
  _sendTopicsBatch(conn, topicsArray, op = 'subscribe') {
    if (!conn || !conn.ws) return Promise.reject(new Error('Invalid connection'));
    if (!Array.isArray(topicsArray) || topicsArray.length === 0) return Promise.resolve();

    return new Promise((resolve, reject) => {
      if (conn.ws.readyState !== WebSocket.OPEN) {
        // not open, re-queue them
        for (const t of topicsArray) conn.pendingTopics.add(t);
        return reject(new Error('WS not open, queued for retry'));
      }

      try {
        const payload = { op, args: topicsArray };
        conn.ws.send(JSON.stringify(payload), (err) => {
          if (err) {
            // send failed; re-queue and reject
            for (const t of topicsArray) conn.pendingTopics.add(t);
            logger.warn({ err: err && err.message ? err.message : err, connId: conn.id, op, batchSize: topicsArray.length }, 'Failed to send batch; queued for retry');
            return reject(err);
          }
          // success: remove from pendingTopics (if present)
          for (const t of topicsArray) conn.pendingTopics.delete(t);
          // reset retry delay after a successful send
          conn.retryDelayMs = WS_SUBSCRIBE_RETRY_BASE_MS;
          logger.debug({ connId: conn.id, op, batchSize: topicsArray.length }, 'Batch sent successfully');
          return resolve();
        });
      } catch (err) {
        for (const t of topicsArray) conn.pendingTopics.add(t);
        logger.warn({ err: err && err.message ? err.message : err, connId: conn.id }, 'Exception while sending batch; queued for retry');
        return reject(err);
      }
    });
  }

  /**
   * Flush pending topics for a given connection in chunked batches.
   * Uses conn.pendingTopics as source of truth and retries with exponential backoff on failure.
   */
  _flushPendingForConn(conn) {
    if (!conn || !conn.pendingTopics || conn.pendingTopics.size === 0) return;

    // If not open, do nothing (open handler will call flush)
    if (!conn.ready || !conn.ws || conn.ws.readyState !== WebSocket.OPEN) {
      logger.debug({ connId: conn.id, pending: conn.pendingTopics.size, readyState: conn.ws ? conn.ws.readyState : 'no-ws' }, 'Not flushing pending because socket not OPEN');
      // schedule a retry attempt when possible
      this._scheduleFlushRetry(conn);
      return;
    }

    const topics = Array.from(conn.pendingTopics);
    // send in chunks
    const chunks = [];
    for (let i = 0; i < topics.length; i += WS_SUBSCRIBE_CHUNK) {
      chunks.push(topics.slice(i, i + WS_SUBSCRIBE_CHUNK));
    }

    const sendNextChunk = (index) => {
      if (index >= chunks.length) {
        logger.debug({ connId: conn.id }, 'All pending batches sent');
        return;
      }
      const batch = chunks[index];
      this._sendTopicsBatch(conn, batch, 'subscribe').then(() => {
        // proceed to next
        sendNextChunk(index + 1);
      }).catch((err) => {
        // schedule retry with exponential backoff
        logger.warn({ connId: conn.id, err: err && err.message ? err.message : err, retryIn: conn.retryDelayMs }, 'Failed to flush pending batch; scheduling retry');
        this._scheduleFlushRetry(conn);
      });
    };

    if (chunks.length) {
      logger.info({ connId: conn.id, batches: chunks.length, totalTopics: topics.length }, 'Flushing pending subscribe topics in batches');
      sendNextChunk(0);
    }
  }

  _scheduleFlushRetry(conn) {
    if (!conn) return;
    if (conn._retryTimer) return; // already scheduled

    const delay = Math.min(conn.retryDelayMs || WS_SUBSCRIBE_RETRY_BASE_MS, WS_SUBSCRIBE_RETRY_MAX_MS);
    conn._retryTimer = setTimeout(() => {
      conn._retryTimer = null;
      // increase for next time
      conn.retryDelayMs = Math.min((conn.retryDelayMs || WS_SUBSCRIBE_RETRY_BASE_MS) * 2, WS_SUBSCRIBE_RETRY_MAX_MS);
      if (conn.ws && conn.ws.readyState === WebSocket.OPEN) {
        logger.debug({ connId: conn.id, retryDelayMs: conn.retryDelayMs }, 'Retrying flush for connection');
        this._flushPendingForConn(conn);
      } else {
        // if not open, schedule again later
        this._scheduleFlushRetry(conn);
      }
    }, delay);
    logger.debug({ connId: conn.id, delayMs: delay }, 'Scheduled flush retry for connection');
  }

  /**
   * Returns a connection that has capacity (< batchSize).
   * If none, creates a new connection.
   */
  _getOrCreateTargetConnection() {
    let target = this.connections.find(c => c.symbols.size < this.batchSize);
    if (!target) {
      target = this._createConnection();
    }
    return target;
  }

  /**
   * Subscribe symbol to the WS with multi-timeframe topics.
   * This now queues subscribe topics and only sends when connection is OPEN (flush).
   */
  subscribeSymbolMTF(symbol, tfs = null) {
    if (!symbol) return null;
    if (this.symbolToConn.has(symbol)) {
      logger.debug({ symbol }, 'Symbol already subscribed, skipping');
      return this.symbolToConn.get(symbol);
    }

    const timeframes = tfs || config.MTF_TFS || ['5', '15', '60', 'D'];
    const target = this._getOrCreateTargetConnection();
    if (!target) {
      logger.warn({ symbol }, 'No available WS connection could be created for subscription');
      return null;
    }

    const tfParts = timeframes.map(tf => this.intervalToTopicPart(tf));
    const topics = [];
    for (const tfp of tfParts) {
      topics.push(`kline.${tfp}.${symbol}`);
    }

    // add to authoritative set and pending queue; don't attempt to send immediately if socket isn't OPEN
    for (const t of topics) {
      target._topics.add(t);
      target.pendingTopics.add(t);
    }

    // record symbol mapping
    target.symbols.add(symbol);
    this.symbolToConn.set(symbol, target);

    logger.info({ symbol, topicsCount: topics.length, connId: target.id, pending: target.pendingTopics.size }, 'Queued symbol for subscription (pending until socket OPEN)');
    
    // attempt flush if possible
    if (target.ws && target.ws.readyState === WebSocket.OPEN) {
      this._flushPendingForConn(target);
    } else {
      // schedule a retry (open handler will flush when ready; this ensures we will retry if connecting stalls)
      this._scheduleFlushRetry(target);
    }
    return target;
  }

  unsubscribeSymbol(symbol) {
    if (!this.symbolToConn.has(symbol)) {
      logger.debug({ symbol }, 'Symbol not found in subscriptions');
      return;
    }
    
    const conn = this.symbolToConn.get(symbol);
    if (!conn) return;

    // determine topics to unsubscribe (only those ending with .<symbol>)
    const topicsToUnsub = Array.from(conn._topics || []).filter(t => t.endsWith(`.${symbol}`));

    // remove them from authoritative topics and pending queue
    for (const t of topicsToUnsub) {
      conn._topics.delete(t);
      conn.pendingTopics.delete(t); // remove if pending
    }

    // attempt to send unsubscribe only if socket is OPEN else rely on state to avoid "not open" errors
    if (topicsToUnsub.length && conn.ws && conn.ws.readyState === WebSocket.OPEN) {
      // chunk unsub messages similarly to subscribe
      for (let i = 0; i < topicsToUnsub.length; i += WS_SUBSCRIBE_CHUNK) {
        const batch = topicsToUnsub.slice(i, i + WS_SUBSCRIBE_CHUNK);
        try {
          this._sendTopicsBatch(conn, batch, 'unsubscribe').catch(err => {
            logger.debug({ err: err && err.message ? err.message : err, connId: conn.id }, 'Unsubscribe batch failed but continuing');
          });
        } catch (err) {
          logger.debug({ err: err && err.message ? err.message : err, connId: conn.id }, 'Exception sending unsubscribe');
        }
      }
    } else {
      logger.debug({ connId: conn.id, reason: conn.ws ? `readyState=${conn.ws.readyState}` : 'no-ws', queuedUnsubs: topicsToUnsub.length }, 'Socket not OPEN, unsubscriptions queued or removed from topics');
    }

    conn.symbols.delete(symbol);
    this.symbolToConn.delete(symbol);
    this.klineBuffer.delete(symbol);
    logger.info({ symbol, connId: conn.id, unsubscribedTopics: topicsToUnsub.length }, 'Unsubscribed symbol from connection');

    // close connection if empty
    if (conn.symbols.size === 0) {
      try {
        if (conn._retryTimer) clearTimeout(conn._retryTimer);
      } catch (e) { /* ignore */ }
      try { if (conn.ws) conn.ws.close(); } catch (e) { /* ignore */ }
    }
  }

  normalizeKlinePayload(d, tf, symbol) {
    // Handle both array and object formats from Bybit
    let open_time, open, high, low, close, volume;

    if (Array.isArray(d)) {
      // Array format: [start, open, high, low, close, volume, turnover, confirm, interval, ...]
      open_time = d[0];
      open = Number(d[1] || 0);
      high = Number(d[2] || 0);
      low = Number(d[3] || 0);
      close = Number(d[4] || 0);
      volume = Number(d[5] || 0);
    } else if (typeof d === 'object') {
      // Object format
      open_time = d.t || d.start || d.start_at || d.open_time || null;
      open = Number(d.o || d.open || 0);
      high = Number(d.h || d.high || 0);
      low = Number(d.l || d.low || 0);
      close = Number(d.c || d.close || 0);
      volume = Number(d.v || d.volume || 0);
    } else {
      // Fallback
      open_time = null;
      open = high = low = close = volume = 0;
    }

    return { open_time, open, high, low, close, volume, timeframe: tf, symbol };
  }

  /**
   * performInitialScan()
   * Returns an array of symbols discovered via WS subscriptions
   * Called by poller for WS-based symbol discovery
   */
  async performInitialScan() {
    try {
      if (this.connections.length === 0) {
        logger.warn('performInitialScan: no active connections yet');
        return [];
      }

      // Collect all subscribed symbols across connections
      const symbols = new Set();
      for (const conn of this.connections) {
        for (const sym of conn.symbols) {
          symbols.add(sym);
        }
      }

      const result = Array.from(symbols).map(s => ({
        symbol: s,
        base: s.replace(/USDT(\.P)?$/i, ''),
        quote: 'USDT'
      }));

      logger.info({ count: result.length }, 'performInitialScan: returning subscribed symbols');
      return result;
    } catch (err) {
      logger.error({ err: err && err.message ? err.message : err }, 'performInitialScan: error');
      return [];
    }
  }

  async closeAll() {
    try {
      logger.info({ connectionsCount: this.connections.length }, 'WSManager: closing all connections');
      
      for (const conn of this.connections.slice()) {
        try {
          const topics = Array.from(conn._topics || []);
          if (topics.length && conn.ws && conn.ws.readyState === WebSocket.OPEN) {
            try {
              conn.ws.send(JSON.stringify({ op: 'unsubscribe', args: topics }));
              logger.debug({ connId: conn.id, topicsCount: topics.length }, 'Unsubscribed all topics');
            } catch (e) { 
              logger.debug({ err: e }, 'Failed to unsubscribe on close');
            }
          }
          if (conn.ws && (conn.ws.readyState === WebSocket.OPEN || conn.ws.readyState === WebSocket.CONNECTING)) {
            try { conn.ws.close(); } catch (e) { /* ignore */ }
          }
          if (conn._retryTimer) clearTimeout(conn._retryTimer);
        } catch (e) {
          logger.debug({ err: e, connId: conn.id }, 'Error closing connection');
        }
      }
      this.connections = [];
      this.symbolToConn = new Map();
      this.klineBuffer = new Map();
      this.openSockets = 0;
      logger.info('WSManager: closed all connections');
    } catch (err) {
      logger.warn({ err: err && err.message ? err.message : err }, 'WSManager.closeAll error');
    }
  }
}

module.exports = new WSManager();
