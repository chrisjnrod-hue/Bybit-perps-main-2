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
  return MAINNET ? 'wss://stream.bybit.com/v5/public/linear' : 'wss://stream-testnet.bybit.com/v5/public/linear';
}

function validateSymbol(symbol) {
  if (!symbol || typeof symbol !== 'string') return false;
  if (!config.SYMBOL_FILTER) return true;
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
    let ws = null;

    try {
      ws = new WebSocket(wsUrl, {
        perMessageDeflate: false,
        handshakeTimeout: 10000
      });
    } catch (err) {
      logger.error({ err: err.message, wsUrl }, 'Failed to create WebSocket');
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
      connectionAttempts: 0
    };

    ws.on('open', () => {
      conn.ready = true;
      conn.connectionAttempts = 0;
      logger.info({ connId: conn.id, wsUrl, topics: conn._topics.size }, 'WS connection opened successfully');

      if (conn.pendingTopics && conn.pendingTopics.size > 0) {
        logger.info({ connId: conn.id, pending: conn.pendingTopics.size }, 'Flushing pending subscribe topics on open');
        setImmediate(() => this._flushPendingForConn(conn));
      }
    });

    ws.on('message', (msg) => {
      try {
        if (!msg || typeof msg !== 'string') {
          return;
        }

        let data = null;
        try {
          data = JSON.parse(msg);
        } catch (parseErr) {
          logger.debug({ err: parseErr.message, snippet: msg.slice(0, 50) }, 'Failed to parse WS message');
          return;
        }

        if (!data || typeof data !== 'object') {
          return;
        }

        // Handle ping/pong
        if (data.op === 'ping') {
          try {
            ws.send(JSON.stringify({ op: 'pong' }));
          } catch (e) {
            logger.debug({ err: e.message }, 'Failed to send pong');
          }
          return;
        }

        // Handle kline data
        if (data.topic && typeof data.topic === 'string' && Array.isArray(data.data) && data.data.length > 0) {
          const topic = String(data.topic).trim();
          const topicParts = topic.split('.');

          if ((topicParts[0] === 'kline' || topicParts[0] === 'klineV2') && topicParts.length >= 3) {
            const tf = topicParts[1];
            const sym = topicParts.slice(2).join('.');

            if (!sym || !validateSymbol(sym)) {
              return;
            }

            const d = data.data[0];
            if (d && typeof d === 'object') {
              try {
                const k = this.normalizeKlinePayload(d, tf, sym);

                if (!this.klineBuffer.has(sym)) {
                  this.klineBuffer.set(sym, {});
                }
                this.klineBuffer.get(sym)[tf] = k;

                this.emit('kline', { symbol: sym, timeframe: tf, data: k, raw: data });
                logger.debug({ symbol: sym, timeframe: tf, close: k.close }, 'Kline received and emitted');
              } catch (normErr) {
                logger.debug({ err: normErr.message, symbol: sym, tf }, 'Failed to normalize kline');
              }
            }
          }
          return;
        }

        // Handle subscription responses
        if (typeof data.ret_code !== 'undefined') {
          const retCode = data.ret_code;
          if (retCode !== 0) {
            logger.warn({ connId: conn.id, retCode, retMsg: data.ret_msg || 'unknown', op: data.op }, 'WS API error response');
          } else {
            logger.debug({ connId: conn.id, op: data.op || 'unknown' }, 'WS operation successful');
          }
          return;
        }

        // Handle unknown message types
        if (data.success === false) {
          logger.warn({ connId: conn.id, msg: data.msg || 'unknown error' }, 'WS failure response');
          return;
        }
      } catch (err) {
        logger.debug({ err: err.message }, 'Exception in WS message handler');
      }
    });

    ws.on('error', (err) => {
      logger.warn({ err: err.message, code: err.code, connId: conn.id }, 'WS error event');
    });

    ws.on('close', (code, reason) => {
      logger.info({ connId: conn.id, code, reason: reason ? reason.toString() : 'no reason' }, 'WS connection closed');

      // Recover symbols
      const symbolsToRecover = Array.from(conn.symbols || []);
      for (const s of conn.symbols) {
        this.symbolToConn.delete(s);
      }

      this.connections = this.connections.filter(c => c !== conn);
      this.openSockets = Math.max(0, this.openSockets - 1);

      if (symbolsToRecover && symbolsToRecover.length > 0) {
        logger.info({ connId: conn.id, recoverCount: symbolsToRecover.length }, 'Re-queueing symbols from closed connection');
        setTimeout(() => {
          for (const sym of symbolsToRecover) {
            try {
              this.subscribeSymbolMTF(sym);
            } catch (e) {
              logger.debug({ err: e.message, symbol: sym }, 'Error re-subscribing');
            }
          }
        }, 1000);
      }

      // Clear timers
      if (conn._retryTimer) {
        clearTimeout(conn._retryTimer);
        conn._retryTimer = null;
      }
    });

    this.connections.push(conn);
    this.openSockets++;
    logger.info({ connId: conn.id, openSockets: this.openSockets, wsUrl }, 'WS connection created');
    return conn;
  }

  _sendTopicsBatch(conn, topicsArray, op = 'subscribe') {
    if (!conn || !conn.ws) {
      return Promise.reject(new Error('Invalid connection'));
    }

    if (!Array.isArray(topicsArray) || topicsArray.length === 0) {
      return Promise.resolve();
    }

    return new Promise((resolve, reject) => {
      if (conn.ws.readyState !== WebSocket.OPEN) {
        logger.debug({ connId: conn.id, readyState: conn.ws.readyState, batchSize: topicsArray.length }, 'Socket not OPEN, queuing topics');
        for (const t of topicsArray) {
          conn.pendingTopics.add(t);
        }
        return reject(new Error('WS not OPEN'));
      }

      try {
        const payload = { op, args: topicsArray };
        const payloadStr = JSON.stringify(payload);

        conn.ws.send(payloadStr, (err) => {
          if (err) {
            logger.warn({ connId: conn.id, err: err.message, op, batchSize: topicsArray.length }, 'Failed to send batch');
            for (const t of topicsArray) {
              conn.pendingTopics.add(t);
            }
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
        logger.warn({ connId: conn.id, err: err.message }, 'Exception sending batch');
        for (const t of topicsArray) {
          conn.pendingTopics.add(t);
        }
        return reject(err);
      }
    });
  }

  _flushPendingForConn(conn) {
    if (!conn || !conn.pendingTopics || conn.pendingTopics.size === 0) {
      return;
    }

    if (!conn.ready || !conn.ws || conn.ws.readyState !== WebSocket.OPEN) {
      logger.debug({ connId: conn.id, pending: conn.pendingTopics.size }, 'Socket not ready, scheduling retry');
      this._scheduleFlushRetry(conn);
      return;
    }

    const topics = Array.from(conn.pendingTopics);
    const chunks = [];

    for (let i = 0; i < topics.length; i += WS_SUBSCRIBE_CHUNK) {
      chunks.push(topics.slice(i, i + WS_SUBSCRIBE_CHUNK));
    }

    logger.debug({ connId: conn.id, chunks: chunks.length, totalTopics: topics.length }, 'Starting flush of pending topics');

    const sendNextChunk = (index) => {
      if (index >= chunks.length) {
        logger.debug({ connId: conn.id }, 'All pending chunks sent');
        return;
      }

      const batch = chunks[index];
      this._sendTopicsBatch(conn, batch, 'subscribe')
        .then(() => {
          sendNextChunk(index + 1);
        })
        .catch((err) => {
          logger.warn({ connId: conn.id, err: err.message, chunkIndex: index, retryIn: conn.retryDelayMs }, 'Chunk send failed, scheduling retry');
          this._scheduleFlushRetry(conn);
        });
    };

    sendNextChunk(0);
  }

  _scheduleFlushRetry(conn) {
    if (!conn) return;
    if (conn._retryTimer) return;

    const delay = Math.min(conn.retryDelayMs || WS_SUBSCRIBE_RETRY_BASE_MS, WS_SUBSCRIBE_RETRY_MAX_MS);
    conn._retryTimer = setTimeout(() => {
      conn._retryTimer = null;
      conn.retryDelayMs = Math.min((conn.retryDelayMs || WS_SUBSCRIBE_RETRY_BASE_MS) * 2, WS_SUBSCRIBE_RETRY_MAX_MS);

      if (conn.ws && conn.ws.readyState === WebSocket.OPEN && conn.pendingTopics.size > 0) {
        logger.debug({ connId: conn.id, delayMs: delay, retryDelayMs: conn.retryDelayMs }, 'Retrying flush');
        this._flushPendingForConn(conn);
      } else {
        this._scheduleFlushRetry(conn);
      }
    }, delay);

    logger.debug({ connId: conn.id, delayMs: delay }, 'Scheduled flush retry');
  }

  _getOrCreateTargetConnection() {
    let target = this.connections.find(c => c.symbols.size < this.batchSize && c.ws && c.ws.readyState === WebSocket.OPEN);
    if (!target) {
      target = this._createConnection();
    }
    return target;
  }

  subscribeSymbolMTF(symbol, tfs = null) {
    try {
      if (!symbol || typeof symbol !== 'string') {
        logger.debug({ symbol }, 'Invalid symbol, skipping');
        return null;
      }

      const symTrimmed = String(symbol).trim().toUpperCase();
      if (!validateSymbol(symTrimmed)) {
        logger.debug({ symbol: symTrimmed }, 'Symbol filtered out, skipping');
        return null;
      }

      if (this.symbolToConn.has(symTrimmed)) {
        logger.debug({ symbol: symTrimmed }, 'Symbol already subscribed');
        return this.symbolToConn.get(symTrimmed);
      }

      const timeframes = (tfs || config.MTF_TFS || ['5', '15', '60', 'D']).map(t => String(t).trim());
      if (!Array.isArray(timeframes) || timeframes.length === 0) {
        logger.warn({ symbol: symTrimmed }, 'No valid timeframes');
        return null;
      }

      const target = this._getOrCreateTargetConnection();
      if (!target) {
        logger.warn({ symbol: symTrimmed }, 'No connection available');
        return null;
      }

      const topics = [];
      for (const tf of timeframes) {
        const tfPart = this.intervalToTopicPart(tf);
        const topic = `kline.${tfPart}.${symTrimmed}`;
        topics.push(topic);
        target.pendingTopics.add(topic);
      }

      target.symbols.add(symTrimmed);
      this.symbolToConn.set(symTrimmed, target);

      logger.info({ symbol: symTrimmed, topicsCount: topics.length, connId: target.id }, 'Symbol queued for subscription');

      if (target.ws && target.ws.readyState === WebSocket.OPEN) {
        this._flushPendingForConn(target);
      } else {
        this._scheduleFlushRetry(target);
      }

      return target;
    } catch (err) {
      logger.error({ err: err.message, symbol }, 'Exception in subscribeSymbolMTF');
      return null;
    }
  }

  unsubscribeSymbol(symbol) {
    try {
      const symTrimmed = String(symbol).trim().toUpperCase();

      if (!this.symbolToConn.has(symTrimmed)) {
        logger.debug({ symbol: symTrimmed }, 'Symbol not in subscriptions');
        return;
      }

      const conn = this.symbolToConn.get(symTrimmed);
      if (!conn) return;

      const topicsToRemove = Array.from(conn._topics || []).filter(t => t.includes(`.${symTrimmed}`));

      for (const t of topicsToRemove) {
        conn._topics.delete(t);
        conn.pendingTopics.delete(t);
      }

      if (topicsToRemove.length > 0 && conn.ws && conn.ws.readyState === WebSocket.OPEN) {
        const chunks = [];
        for (let i = 0; i < topicsToRemove.length; i += WS_SUBSCRIBE_CHUNK) {
          chunks.push(topicsToRemove.slice(i, i + WS_SUBSCRIBE_CHUNK));
        }

        logger.info({ symbol: symTrimmed, chunks: chunks.length }, 'Unsubscribing symbol');

        const unsub = (index) => {
          if (index >= chunks.length) return;
          this._sendTopicsBatch(conn, chunks[index], 'unsubscribe')
            .then(() => unsub(index + 1))
            .catch(err => {
              logger.debug({ err: err.message }, 'Unsubscribe failed');
              unsub(index + 1);
            });
        };

        unsub(0);
      }

      conn.symbols.delete(symTrimmed);
      this.symbolToConn.delete(symTrimmed);
      this.klineBuffer.delete(symTrimmed);

      logger.info({ symbol: symTrimmed, connId: conn.id }, 'Symbol unsubscribed');

      if (conn.symbols.size === 0) {
        if (conn._retryTimer) clearTimeout(conn._retryTimer);
        try {
          if (conn.ws) conn.ws.close();
        } catch (e) { /* ignore */ }
      }
    } catch (err) {
      logger.error({ err: err.message, symbol }, 'Exception in unsubscribeSymbol');
    }
  }

  normalizeKlinePayload(d, tf, symbol) {
    try {
      let open_time, open, high, low, close, volume;

      if (Array.isArray(d)) {
        open_time = d[0];
        open = Number(d[1] || 0);
        high = Number(d[2] || 0);
        low = Number(d[3] || 0);
        close = Number(d[4] || 0);
        volume = Number(d[5] || 0);
      } else if (typeof d === 'object' && d !== null) {
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
    } catch (err) {
      logger.debug({ err: err.message }, 'Error normalizing kline');
      return { open_time: null, open: 0, high: 0, low: 0, close: 0, volume: 0, timeframe: tf, symbol };
    }
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
          if (validateSymbol(sym)) {
            symbols.add(sym);
          }
        }
      }

      const result = Array.from(symbols)
        .filter(s => typeof s === 'string' && s.length > 0)
        .map(s => ({
          symbol: s,
          base: s.replace(/USDT(\.P)?$/i, ''),
          quote: 'USDT'
        }))
        .sort((a, b) => a.symbol.localeCompare(b.symbol));

      logger.info({ count: result.length }, 'performInitialScan: returning symbols');
      return result;
    } catch (err) {
      logger.error({ err: err.message }, 'performInitialScan error');
      return [];
    }
  }

  async closeAll() {
    try {
      logger.info({ connectionsCount: this.connections.length }, 'WSManager: closing all connections');

      for (const conn of this.connections.slice()) {
        try {
          if (conn._retryTimer) clearTimeout(conn._retryTimer);

          if (conn.ws) {
            if (conn.ws.readyState === WebSocket.OPEN) {
              try {
                const topics = Array.from(conn._topics || []);
                if (topics.length > 0) {
                  conn.ws.send(JSON.stringify({ op: 'unsubscribe', args: topics }));
                }
              } catch (e) { /* ignore */ }
            }

            try {
              conn.ws.close();
            } catch (e) { /* ignore */ }
          }
        } catch (e) {
          logger.debug({ err: e.message }, 'Error closing connection');
        }
      }

      this.connections = [];
      this.symbolToConn = new Map();
      this.klineBuffer = new Map();
      this.openSockets = 0;
      logger.info('WSManager: all connections closed');
    } catch (err) {
      logger.warn({ err: err.message }, 'WSManager.closeAll error');
    }
  }
}

module.exports = new WSManager();
