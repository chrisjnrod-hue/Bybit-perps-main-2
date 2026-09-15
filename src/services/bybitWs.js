// src/services/bybitWs.js
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
  // Use realtime_public endpoint from config (which is 'wss://stream.bybit.com/realtime_public')
  return MAINNET ? 'wss://stream.bybit.com/realtime_public' : 'wss://stream-testnet.bybit.com/realtime_public';
}

function validateSymbol(symbol) {
  if (!symbol || typeof symbol !== 'string') return false;
  if (!config.SYMBOL_FILTER) return true; // no filter = allow all
  try {
    const regex = new RegExp(config.SYMBOL_FILTER);
    return regex.test(symbol);
  } catch (e) {
    logger.warn({ filter: config.SYMBOL_FILTER, err: e.message }, 'Invalid SYMBOL_FILTER regex');
    return true;
  }
}

const WS_SUBSCRIBE_CHUNK = config.WS_SUBSCRIBE_CHUNK || (process.env.WS_SUBSCRIBE_CHUNK ? Number(process.env.WS_SUBSCRIBE_CHUNK) : 50);
const WS_SUBSCRIBE_RETRY_BASE_MS = config.WS_SUBSCRIBE_RETRY_BASE_MS || (process.env.WS_SUBSCRIBE_RETRY_BASE_MS ? Number(process.env.WS_SUBSCRIBE_RETRY_BASE_MS) : 1000);
const WS_SUBSCRIBE_RETRY_MAX_MS = config.WS_SUBSCRIBE_RETRY_MAX_MS || (process.env.WS_SUBSCRIBE_RETRY_MAX_MS ? Number(process.env.WS_SUBSCRIBE_RETRY_MAX_MS) : 60000);

class WSManager extends EventEmitter {
  constructor() {
    super();
    this.connections = [];
    this.symbolToConn = new Map();
    this.openSockets = 0;
    this.maxSockets = config.MAX_CONCURRENT_WS || 20;
    this.batchSize = config.BATCH_WS_SIZE || 20;
    this.klineBuffer = new Map();
  }

  start() {
    logger.info({ wsUrl: getWsUrl(), maxSockets: this.maxSockets, batchSize: this.batchSize, symbolFilter: config.SYMBOL_FILTER }, 'WS Manager ready (batched, filtered)');
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
      _topics: new Set(),
      pendingTopics: new Set(),
      ready: false,
      retryDelayMs: WS_SUBSCRIBE_RETRY_BASE_MS,
      _retryTimer: null
    };

    ws.on('open', () => {
      conn.ready = true;
      logger.info({ connId: conn.id, wsUrl }, 'WS connection opened');
      if (conn.pendingTopics && conn.pendingTopics.size) {
        logger.info({ connId: conn.id, pending: conn.pendingTopics.size }, 'Flushing pending subscribe topics on open');
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
            
            if (!validateSymbol(sym)) {
              logger.debug({ symbol: sym }, 'Kline received for filtered-out symbol, ignoring');
              return;
            }
            
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
        } else if (data && typeof data.ret_code !== 'undefined') {
          if (data.ret_code !== 0) {
            logger.error({ 
              connId: conn.id, 
              retCode: data.ret_code, 
              retMsg: data.ret_msg,
              op: data.op,
              req_id: data.req_id
            }, 'WS subscription error response');
            
            // Log the entire response for debugging
            logger.debug({ fullResponse: data }, 'Full error response from Bybit');\n            \n            // Handle specific error codes
            if (data.ret_code === 403 || data.ret_code === 401) {
              logger.error({ connId: conn.id, retCode: data.ret_code }, 'Auth/permission error - closing connection');
              try {
                if (ws && ws.readyState === WebSocket.OPEN) {
                  ws.close(1008, 'Auth error');
                }
              } catch (e) { /* ignore */ }
              return;
            }
            
            if (data.ret_code === 429) {
              logger.warn({ connId: conn.id }, 'Rate limited - backing off');
              conn.retryDelayMs = Math.min(conn.retryDelayMs * 3, WS_SUBSCRIBE_RETRY_MAX_MS);
              this._scheduleFlushRetry(conn);
              return;
            }
          } else {
            logger.debug({ connId: conn.id, op: data.op, req_id: data.req_id }, 'WS operation successful');
          }
        }
      } catch (err) {
        logger.debug({ err: err && err.message ? err.message : err }, 'Failed to parse WS message');
      }
    });

    ws.on('error', (err) => {
      logger.error({ err: err && err.message ? err.message : err, code: err.code, connId: conn.id }, 'WS error');
      
      if (err.code === 'ECONNREFUSED' || err.statusCode === 403) {
        logger.error({ connId: conn.id }, 'Connection refused or forbidden - check endpoint URL');
        try {
          if (ws && ws.readyState === WebSocket.OPEN) {
            ws.close(1008, 'Connection error');
          }
        } catch (e) { /* ignore */ }
      }
    });

    ws.on('close', (code, reason) => {
      logger.info({ connId: conn.id, code, reason: reason ? reason.toString() : '' }, 'WS connection closed');

      const symbolsToRecover = Array.from(conn.symbols || []);
      for (const s of conn.symbols) this.symbolToConn.delete(s);

      this.connections = this.connections.filter(c => c !== conn);
      this.openSockets = Math.max(0, this.openSockets - 1);

      if (symbolsToRecover && symbolsToRecover.length) {
        logger.info({ connId: conn.id, recoverCount: symbolsToRecover.length }, 'Re-queueing symbols from closed connection');
        setTimeout(() => {
          for (const sym of symbolsToRecover) {
            try {
              this.subscribeSymbolMTF(sym);
            } catch (e) {
              logger.debug({ err: e, symbol: sym }, 'Error re-subscribing symbol');
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

  _sendTopicsBatch(conn, topicsArray, op = 'subscribe') {
    if (!conn || !conn.ws) return Promise.reject(new Error('Invalid connection'));
    if (!Array.isArray(topicsArray) || topicsArray.length === 0) return Promise.resolve();

    return new Promise((resolve, reject) => {
      if (conn.ws.readyState !== WebSocket.OPEN) {
        for (const t of topicsArray) conn.pendingTopics.add(t);
        return reject(new Error('WS not open, queued for retry'));
      }

      try {
        const req_id = Date.now() + '-' + Math.random().toString(16).slice(2);
        const payload = { op, args: topicsArray, req_id };
        const payloadStr = JSON.stringify(payload);
        
        logger.debug({ 
          connId: conn.id, 
          payload: payloadStr,
          op, 
          topics: topicsArray,
          batchSize: topicsArray.length 
        }, 'Sending batch to Bybit WebSocket');
        
        conn.ws.send(payloadStr, (err) => {
          if (err) {
            for (const t of topicsArray) conn.pendingTopics.add(t);
            logger.warn({ err: err && err.message ? err.message : err, connId: conn.id, op, batchSize: topicsArray.length }, 'Failed to send batch; queued for retry');
            return reject(err);
          }
          for (const t of topicsArray) {
            conn.pendingTopics.delete(t);
            conn._topics.add(t);
          }
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

  _flushPendingForConn(conn) {
    if (!conn || !conn.pendingTopics || conn.pendingTopics.size === 0) return;

    if (!conn.ready || !conn.ws || conn.ws.readyState !== WebSocket.OPEN) {
      logger.debug({ connId: conn.id, pending: conn.pendingTopics.size, readyState: conn.ws ? conn.ws.readyState : 'no-ws' }, 'Not flushing pending because socket not OPEN');
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
    if (conn._retryTimer) return;

    const delay = Math.min(conn.retryDelayMs || WS_SUBSCRIBE_RETRY_BASE_MS, WS_SUBSCRIBE_RETRY_MAX_MS);
    conn._retryTimer = setTimeout(() => {
      conn._retryTimer = null;
      conn.retryDelayMs = Math.min((conn.retryDelayMs || WS_SUBSCRIBE_RETRY_BASE_MS) * 2, WS_SUBSCRIBE_RETRY_MAX_MS);
      if (conn.ws && conn.ws.readyState === WebSocket.OPEN) {
        logger.debug({ connId: conn.id, retryDelayMs: conn.retryDelayMs }, 'Retrying flush for connection');
        this._flushPendingForConn(conn);
      } else {
        this._scheduleFlushRetry(conn);
      }
    }, delay);
    logger.debug({ connId: conn.id, delayMs: delay }, 'Scheduled flush retry for connection');
  }

  _getOrCreateTargetConnection() {
    let target = this.connections.find(c => c.symbols.size < this.batchSize);
    if (!target) {
      target = this._createConnection();
    }
    return target;
  }

  subscribeSymbolMTF(symbol, tfs = null) {
    if (!symbol || !validateSymbol(symbol)) {
      logger.debug({ symbol }, 'Symbol rejected by filter or invalid, skipping subscription');
      return null;
    }
    if (this.symbolToConn.has(symbol)) {
      logger.debug({ symbol }, 'Symbol already subscribed, skipping');
      return this.symbolToConn.get(symbol);
    }

    const timeframes = tfs || config.MTF_TFS || ['5', '15', '60', '240', 'D'];
    const target = this._getOrCreateTargetConnection();
    if (!target) {
      logger.warn({ symbol }, 'No available WS connection could be created for subscription');
      return null;
    }

    const tfParts = timeframes.map(tf => this.intervalToTopicPart(tf));
    const topics = [];
    for (const tfp of tfParts) {
      topics.push(`kline.${tfp}.${symbol}`);\n    }\n\n    for (const t of topics) {\n      target._topics.add(t);\n      target.pendingTopics.add(t);\n    }\n\n    target.symbols.add(symbol);\n    this.symbolToConn.set(symbol, target);\n\n    logger.info({ symbol, topicsCount: topics.length, connId: target.id, pending: target.pendingTopics.size, topics: topics }, 'Queued symbol for subscription (pending until socket OPEN)');\n    \n    if (target.ws && target.ws.readyState === WebSocket.OPEN) {\n      this._flushPendingForConn(target);\n    } else {\n      this._scheduleFlushRetry(target);\n    }\n    return target;\n  }\n\n  unsubscribeSymbol(symbol) {\n    if (!this.symbolToConn.has(symbol)) {\n      logger.debug({ symbol }, 'Symbol not found in subscriptions');\n      return;\n    }\n    \n    const conn = this.symbolToConn.get(symbol);\n    if (!conn) return;\n\n    const topicsToUnsub = Array.from(conn._topics || []).filter(t => t.endsWith(`.${symbol}`));\n\n    for (const t of topicsToUnsub) {\n      conn._topics.delete(t);\n      conn.pendingTopics.delete(t);\n    }\n\n    if (topicsToUnsub.length && conn.ws && conn.ws.readyState === WebSocket.OPEN) {\n      for (let i = 0; i < topicsToUnsub.length; i += WS_SUBSCRIBE_CHUNK) {\n        const batch = topicsToUnsub.slice(i, i + WS_SUBSCRIBE_CHUNK);\n        try {\n          this._sendTopicsBatch(conn, batch, 'unsubscribe').catch(err => {\n            logger.debug({ err: err && err.message ? err.message : err, connId: conn.id }, 'Unsubscribe batch failed but continuing');\n          });\n        } catch (err) {\n          logger.debug({ err: err && err.message ? err.message : err, connId: conn.id }, 'Exception sending unsubscribe');\n        }\n      }\n    } else {\n      logger.debug({ connId: conn.id, reason: conn.ws ? `readyState=${conn.ws.readyState}` : 'no-ws', queuedUnsubs: topicsToUnsub.length }, 'Socket not OPEN, unsubscriptions queued or removed');\n    }\n\n    conn.symbols.delete(symbol);\n    this.symbolToConn.delete(symbol);\n    this.klineBuffer.delete(symbol);\n    logger.info({ symbol, connId: conn.id, unsubscribedTopics: topicsToUnsub.length }, 'Unsubscribed symbol from connection');\n\n    if (conn.symbols.size === 0) {\n      try {\n        if (conn._retryTimer) clearTimeout(conn._retryTimer);\n      } catch (e) { /* ignore */ }\n      try { if (conn.ws) conn.ws.close(); } catch (e) { /* ignore */ }\n    }\n  }\n\n  normalizeKlinePayload(d, tf, symbol) {\n    let open_time, open, high, low, close, volume;\n\n    if (Array.isArray(d)) {\n      open_time = d[0];\n      open = Number(d[1] || 0);\n      high = Number(d[2] || 0);\n      low = Number(d[3] || 0);\n      close = Number(d[4] || 0);\n      volume = Number(d[5] || 0);\n    } else if (typeof d === 'object') {\n      open_time = d.t || d.start || d.start_at || d.open_time || null;\n      open = Number(d.o || d.open || 0);\n      high = Number(d.h || d.high || 0);\n      low = Number(d.l || d.low || 0);\n      close = Number(d.c || d.close || 0);\n      volume = Number(d.v || d.volume || 0);\n    } else {\n      open_time = null;\n      open = high = low = close = volume = 0;\n    }\n\n    return { open_time, open, high, low, close, volume, timeframe: tf, symbol };\n  }\n\n  async performInitialScan() {\n    try {\n      if (this.connections.length === 0) {\n        logger.warn('performInitialScan: no active connections yet');\n        return [];\n      }\n\n      const symbols = new Set();\n      for (const conn of this.connections) {\n        for (const sym of conn.symbols) {\n          if (validateSymbol(sym)) {\n            symbols.add(sym);\n          }\n        }\n      }\n\n      const result = Array.from(symbols).map(s => ({\n        symbol: s,\n        base: s.replace(/USDT[Pp]?$/i, ''),\n        quote: 'USDT'\n      }));\n\n      logger.info({ count: result.length }, 'performInitialScan: returning subscribed symbols');\n      return result;\n    } catch (err) {\n      logger.error({ err: err && err.message ? err.message : err }, 'performInitialScan: error');\n      return [];\n    }\n  }\n\n  async closeAll() {\n    try {\n      logger.info({ connectionsCount: this.connections.length }, 'WSManager: closing all connections');\n      \n      for (const conn of this.connections.slice()) {\n        try {\n          const topics = Array.from(conn._topics || []);\n          if (topics.length && conn.ws && conn.ws.readyState === WebSocket.OPEN) {\n            try {\n              conn.ws.send(JSON.stringify({ op: 'unsubscribe', args: topics }));\n              logger.debug({ connId: conn.id, topicsCount: topics.length }, 'Unsubscribed all topics');\n            } catch (e) { \n              logger.debug({ err: e }, 'Failed to unsubscribe on close');\n            }\n          }\n          if (conn.ws && (conn.ws.readyState === WebSocket.OPEN || conn.ws.readyState === WebSocket.CONNECTING)) {\n            try { conn.ws.close(); } catch (e) { /* ignore */ }\n          }\n          if (conn._retryTimer) clearTimeout(conn._retryTimer);\n        } catch (e) {\n          logger.debug({ err: e, connId: conn.id }, 'Error closing connection');\n        }\n      }\n      this.connections = [];\n      this.symbolToConn = new Map();\n      this.klineBuffer = new Map();\n      this.openSockets = 0;\n      logger.info('WSManager: closed all connections');\n    } catch (err) {\n      logger.warn({ err: err && err.message ? err.message : err }, 'WSManager.closeAll error');\n    }\n  }\n}\n\nmodule.exports = new WSManager();\n```

## Critical Changes Made:

1. **Fixed endpoint URL** - Now uses `wss://stream.bybit.com/realtime_public` from your config (was using wrong v5 endpoint)
2. **Added `req_id`** to subscription payloads (required by Bybit)
3. **Enhanced logging** - Now logs the exact payload being sent so you can debug
4. **Better error logging** - Logs full response object for debugging
5. **Maintained working retry logic** from your old version

**Test it now and share the logs.** The payload logging will show exactly what's being sent to Bybit, which will help identify if the topic format is still wrong.
