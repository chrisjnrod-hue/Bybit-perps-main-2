// src/services/bybitWs.js
/**
 * Batched Bybit WebSocket manager (v5-aware).
 * 
 * CRITICAL FIXES:
 * - Proper connection establishment with auto-reconnect
 * - Heartbeat/keepalive (ping/pong every 30s)
 * - Better error recovery and exponential backoff
 * - Socket state validation before sending
 * - Connection pooling with proper cleanup
 */

const WebSocket = require('ws');
const EventEmitter = require('events');
const config = require('../config');
const logger = require('pino')();

function envBool(name, defaultVal = false) {
  if (typeof process.env[name] === 'undefined') return defaultVal;
  const v = String(process.env[name]).toLowerCase().trim();
  return v === '1' || v === 'true' || v === 'yes';
}

const MAINNET = envBool('MAINNET', true);

function getWsUrl() {
  const explicit = process.env.BYBIT_WS_PUBLIC || (config && config.BYBIT_WS_PUBLIC);
  if (explicit) return String(explicit);
  return MAINNET ? 'wss://stream.bybit.com/v5/public/linear' : 'wss://stream-testnet.bybit.com/v5/public/linear';
}

const WS_SUBSCRIBE_CHUNK = config.WS_SUBSCRIBE_CHUNK || 50;
const WS_SUBSCRIBE_RETRY_BASE_MS = config.WS_SUBSCRIBE_RETRY_BASE_MS || 1000;
const WS_SUBSCRIBE_RETRY_MAX_MS = config.WS_SUBSCRIBE_RETRY_MAX_MS || 60000;

class WSManager extends EventEmitter {
  constructor() {
    super();
    this.connections = [];
    this.symbolToConn = new Map();
    this.openSockets = 0;
    this.maxSockets = config.MAX_CONCURRENT_WS || 20;
    this.batchSize = config.BATCH_WS_SIZE || 20;
    this.klineBuffer = new Map();
    this.heartbeatInterval = null;
    this.connectionAttempts = new Map();
  }

  start() {
    logger.info({ wsUrl: getWsUrl(), maxSockets: this.maxSockets, batchSize: this.batchSize }, 'WS Manager starting');
    
    // Create initial connections proactively
    this._ensureMinConnections();
    
    // Start heartbeat
    this.heartbeatInterval = setInterval(() => {
      for (const conn of this.connections) {
        if (conn && conn.ws && conn.ws.readyState === WebSocket.OPEN) {
          try {
            conn.ws.ping();
          } catch (e) {
            logger.debug({ connId: conn.id }, 'Heartbeat ping failed');
          }
        }
      }
    }, 30000);

    logger.info('WS Manager started and ready');
  }

  // NEW: Ensure minimum number of connections exist
  _ensureMinConnections() {
    const minConnections = Math.ceil(this.maxSockets / 2) || 1;
    while (this.openSockets < minConnections && this.openSockets < this.maxSockets) {
      this._createConnection();
    }
  }

  intervalToTopicPart(tf) {
    return tf === 'D' ? 'D' : String(tf);
  }

  _createConnection() {
    if (this.openSockets >= this.maxSockets) {
      logger.warn({ maxSockets: this.maxSockets }, 'Max WS connections reached');
      return null;
    }

    const wsUrl = getWsUrl();
    let ws;
    
    try {
      ws = new WebSocket(wsUrl, {
        handshakeTimeout: 10000, // 10 second timeout
        perMessageDeflate: false
      });
    } catch (err) {
      logger.error({ err, wsUrl }, 'Failed to create WebSocket');
      return null;
    }

    const conn = {
      ws,
      symbols: new Set(),
      id: Date.now() + '-' + Math.random().toString(16).slice(2),
      _topics: new Set(),
      pendingTopics: new Set(),
      ready: false,
      retryDelayMs: WS_SUBSCRIBE_RETRY_BASE_MS,
      _retryTimer: null,
      reconnectAttempts: 0,
      createdAt: Date.now()
    };

    ws.on('open', () => {
      conn.ready = true;
      conn.reconnectAttempts = 0;
      logger.info({ connId: conn.id, wsUrl }, '✅ WS connection opened successfully');
      
      if (conn.pendingTopics && conn.pendingTopics.size > 0) {
        logger.info({ connId: conn.id, pending: conn.pendingTopics.size }, 'Flushing pending topics on open');
        this._flushPendingForConn(conn);
      }
    });

    ws.on('message', (msg) => {
      try {
        const data = JSON.parse(msg);
        
        if (data && data.op === 'ping') {
          try { ws.send(JSON.stringify({ op: 'pong' })); } catch (e) { /* ignore */ }
          return;
        }

        if (data && data.topic && Array.isArray(data.data) && data.data.length > 0) {
          const topic = String(data.topic);
          const topicParts = topic.split('.');
          
          if ((topicParts[0] === 'kline' || topicParts[0] === 'klineV2') && topicParts.length >= 3) {
            const tf = topicParts[1];
            const sym = topicParts.slice(2).join('.');
            
            if (data.data[0]) {
              const d = data.data[0];
              const k = this.normalizeKlinePayload(d, tf, sym);
              
              if (!this.klineBuffer.has(sym)) {
                this.klineBuffer.set(sym, {});
              }
              this.klineBuffer.get(sym)[tf] = k;
              
              this.emit('kline', { symbol: sym, timeframe: tf, data: k, raw: data });
              logger.debug({ symbol: sym, timeframe: tf, close: k.close }, 'Kline received');
            }
          }
        } else if (data && (data.ret_code !== undefined || data.retCode !== undefined)) {
          const rc = data.ret_code !== undefined ? data.ret_code : data.retCode;
          if (rc !== 0) {
            logger.warn({ connId: conn.id, retCode: rc, retMsg: data.ret_msg }, 'WS subscription error');
          }
        }
      } catch (err) {
        logger.debug({ err: err && err.message ? err.message : err }, 'Failed to parse WS message');
      }
    });

    ws.on('error', (err) => {
      logger.warn({ err: err && err.message ? err.message : err, connId: conn.id }, 'WS error event');
    });

    ws.on('close', (code, reason) => {
      logger.warn({ connId: conn.id, code, reason: reason ? reason.toString() : 'unknown' }, 'WS connection closed');

      if (conn._retryTimer) {
        clearTimeout(conn._retryTimer);
        conn._retryTimer = null;
      }

      const symbolsToRecover = Array.from(conn.symbols || []);
      for (const s of conn.symbols) this.symbolToConn.delete(s);

      this.connections = this.connections.filter(c => c !== conn);
      this.openSockets = Math.max(0, this.openSockets - 1);

      // Exponential backoff reconnection
      if (symbolsToRecover && symbolsToRecover.length > 0) {
        conn.reconnectAttempts = (conn.reconnectAttempts || 0) + 1;
        const retryDelay = Math.min(1000 * Math.pow(2, Math.min(conn.reconnectAttempts - 1, 5)), 30000);
        
        logger.info({ 
          connId: conn.id, 
          recoverCount: symbolsToRecover.length,
          retryDelay,
          reconnectAttempts: conn.reconnectAttempts
        }, 'Scheduling symbol recovery with exponential backoff');
        
        setTimeout(() => {
          for (const sym of symbolsToRecover) {
            try {
              this.subscribeSymbolMTF(sym);
            } catch (e) {
              logger.debug({ err: e, symbol: sym }, 'Error re-subscribing');
            }
          }
        }, retryDelay);
      }

      // Ensure minimum connections
      this._ensureMinConnections();
    });

    this.connections.push(conn);
    this.openSockets++;
    logger.info({ connId: conn.id, openSockets: this.openSockets }, 'WS connection created');
    return conn;
  }

  _sendTopicsBatch(conn, topicsArray, op = 'subscribe') {
    if (!conn || !conn.ws) return Promise.reject(new Error('Invalid connection'));
    if (!Array.isArray(topicsArray) || topicsArray.length === 0) return Promise.resolve();

    return new Promise((resolve, reject) => {
      // CRITICAL: Check socket is OPEN before sending
      if (conn.ws.readyState !== WebSocket.OPEN) {
        for (const t of topicsArray) conn.pendingTopics.add(t);
        logger.debug({ connId: conn.id, readyState: conn.ws.readyState }, 'Socket not OPEN, queuing topics');
        return reject(new Error(`WS not open (state=${conn.ws.readyState}), queued for retry`));
      }

      try {
        const payload = { op, args: topicsArray };
        conn.ws.send(JSON.stringify(payload), (err) => {
          if (err) {
            for (const t of topicsArray) conn.pendingTopics.add(t);
            logger.warn({ err: err && err.message ? err.message : err, connId: conn.id, op, batchSize: topicsArray.length }, 'Send failed; queued for retry');
            return reject(err);
          }
          for (const t of topicsArray) conn.pendingTopics.delete(t);
          conn.retryDelayMs = WS_SUBSCRIBE_RETRY_BASE_MS;
          logger.debug({ connId: conn.id, op, batchSize: topicsArray.length }, 'Batch sent successfully');
          return resolve();
        });
      } catch (err) {
        for (const t of topicsArray) conn.pendingTopics.add(t);
        logger.warn({ err: err && err.message ? err.message : err, connId: conn.id }, 'Exception during send; queued for retry');
        return reject(err);
      }
    });
  }

  _flushPendingForConn(conn) {
    if (!conn || !conn.pendingTopics || conn.pendingTopics.size === 0) return;

    if (!conn.ready || !conn.ws || conn.ws.readyState !== WebSocket.OPEN) {
      logger.debug({ connId: conn.id, pending: conn.pendingTopics.size, readyState: conn.ws ? conn.ws.readyState : 'no-ws' }, 'Socket not OPEN, scheduling retry');
      this._scheduleFlushRetry(conn);
      return;
    }

    const topics = Array.from(conn.pendingTopics);
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
        sendNextChunk(index + 1);
      }).catch((err) => {
        logger.warn({ connId: conn.id, err: err && err.message ? err.message : err }, 'Flush batch failed; scheduling retry');
        this._scheduleFlushRetry(conn);
      });
    };

    if (chunks.length) {
      logger.info({ connId: conn.id, batches: chunks.length, totalTopics: topics.length }, 'Flushing pending topics in batches');
      sendNextChunk(0);
    }
  }

  _scheduleFlushRetry(conn) {
    if (!conn) return;
    if (conn._retryTimer) return;

    const delay = Math.min(conn.retryDelayMs || WS_SUBSCRIBE_RETRY_BASE_MS, WS_SUBSCRIBE_RETRY_MAX_MS);
    conn._retryTimer = setTimeout(() => {
      conn._retryTimer = null;
      conn.retryDelayMs = Math.min((conn.retryDelayMs || WS_SUBSCRIBE_RETRY_BASE_MS) * 2, WS_SUBSCRIBE_RETRY_MAX_MS);
      if (conn.ws && conn.ws.readyState === WebSocket.OPEN) {
        logger.debug({ connId: conn.id, retryDelayMs: conn.retryDelayMs }, 'Retrying flush');
        this._flushPendingForConn(conn);
      } else {
        this._scheduleFlushRetry(conn);
      }
    }, delay);
    logger.debug({ connId: conn.id, delayMs: delay }, 'Scheduled flush retry');
  }

  _getOrCreateTargetConnection() {
    let target = this.connections.find(c => c.symbols.size < this.batchSize && c.ready);
    if (!target) {
      target = this._createConnection();
    }
    return target;
  }

  subscribeSymbolMTF(symbol, tfs = null) {
    if (!symbol) return null;
    if (this.symbolToConn.has(symbol)) {
      logger.debug({ symbol }, 'Symbol already subscribed');
      return this.symbolToConn.get(symbol);
    }

    const timeframes = tfs || config.MTF_TFS || ['5', '15', '60', 'D'];
    const target = this._getOrCreateTargetConnection();
    if (!target) {
      logger.warn({ symbol }, 'No available WS connection could be created');
      return null;
    }

    const tfParts = timeframes.map(tf => this.intervalToTopicPart(tf));
    const topics = [];
    for (const tfp of tfParts) {
      topics.push(`kline.${tfp}.${symbol}`);
    }

    for (const t of topics) {
      target._topics.add(t);
      target.pendingTopics.add(t);
    }

    target.symbols.add(symbol);
    this.symbolToConn.set(symbol, target);

    logger.info({ symbol, topicsCount: topics.length, connId: target.id, pending: target.pendingTopics.size }, 'Queued symbol for subscription');
    
    if (target.ws && target.ws.readyState === WebSocket.OPEN) {
      this._flushPendingForConn(target);
    } else {
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

    const topicsToUnsub = Array.from(conn._topics || []).filter(t => t.endsWith(`.${symbol}`));

    for (const t of topicsToUnsub) {
      conn._topics.delete(t);
      conn.pendingTopics.delete(t);
    }

    if (topicsToUnsub.length && conn.ws && conn.ws.readyState === WebSocket.OPEN) {
      for (let i = 0; i < topicsToUnsub.length; i += WS_SUBSCRIBE_CHUNK) {
        const batch = topicsToUnsub.slice(i, i + WS_SUBSCRIBE_CHUNK);
        try {
          this._sendTopicsBatch(conn, batch, 'unsubscribe').catch(err => {
            logger.debug({ err: err && err.message ? err.message : err, connId: conn.id }, 'Unsubscribe batch failed');
          });
        } catch (err) {
          logger.debug({ err: err && err.message ? err.message : err, connId: conn.id }, 'Exception sending unsubscribe');
        }
      }
    }

    conn.symbols.delete(symbol);
    this.symbolToConn.delete(symbol);
    this.klineBuffer.delete(symbol);
    logger.info({ symbol, connId: conn.id, unsubscribedTopics: topicsToUnsub.length }, 'Unsubscribed symbol');

    if (conn.symbols.size === 0) {
      try {
        if (conn._retryTimer) clearTimeout(conn._retryTimer);
      } catch (e) { /* ignore */ }
      try { if (conn.ws) conn.ws.close(); } catch (e) { /* ignore */ }
    }
  }

  normalizeKlinePayload(d, tf, symbol) {
    let open_time, open, high, low, close, volume;

    if (Array.isArray(d)) {
      open_time = d[0];
      open = Number(d[1] || 0);
      high = Number(d[2] || 0);
      low = Number(d[3] || 0);
      close = Number(d[4] || 0);
      volume = Number(d[5] || 0);
    } else if (typeof d === 'object') {
      open_time = d.t || d.start || d.start_at || d.open_time || null;
      open = Number(d.o || d.open || 0);
      high = Number(d.h || d.high || 0);
      low = Number(d.l || d.low || 0);
      close = Number(d.c || d.close || 0);
      volume = Number(d.v || d.volume || 0);
    } else {
      open_time = null;
      open = high = low = close = volume = 0;
    }

    return { open_time, open, high, low, close, volume, timeframe: tf, symbol };
  }

  async performInitialScan() {
    try {
      if (this.connections.length === 0) {
        logger.warn('performInitialScan: no active connections');
        return [];
      }

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
      logger.error({ err: err && err.message ? err.message : err }, 'performInitialScan error');
      return [];
    }
  }

  async closeAll() {
    try {
      if (this.heartbeatInterval) {
        clearInterval(this.heartbeatInterval);
        this.heartbeatInterval = null;
      }

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
      logger.info('WSManager: all connections closed');
    } catch (err) {
      logger.warn({ err: err && err.message ? err.message : err }, 'WSManager.closeAll error');
    }
  }
}

module.exports = new WSManager();
