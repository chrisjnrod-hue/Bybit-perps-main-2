// src/services/bybitWs.js
/**
 * Batched Bybit WebSocket manager (v5-aware).
 *
 * - Groups symbols into connections up to config.BATCH_WS_SIZE per connection.
 * - Caps total connections at config.MAX_CONCURRENT_WS.
 * - Subscribes to both klineV2.<interval>.<symbol> and fallback kline.<interval>.<symbol>.
 * - Emits 'kline' events: payload { symbol, timeframe, data, raw } where data has { open_time, open, high, low, close, volume }.
 * - Provides closeAll() for graceful shutdown.
 */

const WebSocket = require('ws');
const EventEmitter = require('events');
const config = require('../config');
const logger = require('pino')();

class WSManager extends EventEmitter {
  constructor() {
    super();
    this.connections = []; // array of { ws, symbols:Set, id, _topics:Set }
    this.symbolToConn = new Map(); // symbol -> connection index
    this.openSockets = 0;
    this.maxSockets = config.MAX_CONCURRENT_WS || 20;
    this.batchSize = config.BATCH_WS_SIZE || 20;
  }

  start() {
    logger.info('WS Manager ready (batched)');
  }

  intervalToTopicPart(tf) {
    // tf: numeric string or 'D'
    return tf === 'D' ? 'D' : String(tf);
  }

  _createConnection() {
    if (this.openSockets >= this.maxSockets) {
      logger.warn({ maxSockets: this.maxSockets }, 'Max WS connections reached; cannot create new connection');
      return null;
    }

    const wsUrl = config.BYBIT_WS_PUBLIC;
    const ws = new WebSocket(wsUrl);
    const conn = {
      ws,
      symbols: new Set(),
      id: Date.now() + '-' + Math.random().toString(16).slice(2),
      _topics: new Set(),
      ready: false
    };

    ws.on('open', () => {
      conn.ready = true;
      logger.info({ connId: conn.id }, 'WS connection opened');
      // If there are pending topics, send subscribe for them
      const pending = Array.from(conn._topics || []);
      if (pending.length) {
        try {
          ws.send(JSON.stringify({ op: 'subscribe', args: pending }));
        } catch (err) {
          logger.warn({ err, connId: conn.id }, 'Failed to send pending subscribe');
        }
      }
    });

    ws.on('message', (msg) => {
      try {
        const data = JSON.parse(msg);
        // Handle bybit kline messages with topic and data array
        if (data && data.topic && Array.isArray(data.data) && data.data.length) {
          const topic = data.topic;
          const topicParts = topic.split('.');
          if (topicParts.length >= 3) {
            const tf = topicParts[1];
            const sym = topicParts.slice(2).join('.');
            const d = data.data[0];
            const k = this.normalizeKlinePayload(d, tf, sym);
            // Emit normalized kline event
            this.emit('kline', { symbol: sym, timeframe: tf, data: k, raw: data });
          }
        } else {
          // Some endpoints send ping: { "ping": 12345 } — reply with pong
          if (data && data.ping) {
            try { ws.send(JSON.stringify({ pong: data.ping })); } catch (e) { /* ignore */ }
          }
        }
      } catch (err) {
        logger.debug({ err }, 'Failed to parse WS message');
      }
    });

    ws.on('error', (err) => {
      logger.warn({ err, connId: conn.id }, 'WS error');
    });

    ws.on('close', (code, reason) => {
      logger.info({ connId: conn.id, code, reason }, 'WS connection closed');
      // cleanup
      for (const s of conn.symbols) this.symbolToConn.delete(s);
      this.connections = this.connections.filter(c => c !== conn);
      this.openSockets = Math.max(0, this.openSockets - 1);
    });

    this.connections.push(conn);
    this.openSockets++;
    return conn;
  }

  subscribeSymbolMTF(symbol, tfs = config.MTF_TFS) {
    if (!symbol) return null;
    // If already subscribed, return existing connection
    if (this.symbolToConn.has(symbol)) {
      const idx = this.symbolToConn.get(symbol);
      return this.connections[idx];
    }

    // Find connection with capacity
    let target = this.connections.find(c => c.symbols.size < this.batchSize);
    if (!target) {
      target = this._createConnection();
      if (!target) {
        logger.warn({ symbol }, 'No available WS connection could be created for subscription');
        return null;
      }
    }

    const tfParts = tfs.map(tf => this.intervalToTopicPart(tf));
    const topics = [];
    for (const tfp of tfParts) {
      topics.push(`klineV2.${tfp}.${symbol}`);
      topics.push(`kline.${tfp}.${symbol}`); // fallback
    }

    // Add topics to conn._topics and send subscribe
    for (const t of topics) target._topics.add(t);

    try {
      // send subscribe (ws will buffer until open)
      target.ws.send(JSON.stringify({ op: 'subscribe', args: topics }));
    } catch (err) {
      logger.warn({ err, symbol, connId: target.id }, 'Failed to send subscribe request (will attempt later)');
    }

    target.symbols.add(symbol);
    this.symbolToConn.set(symbol, this.connections.indexOf(target));
    logger.info({ symbol, topics, connId: target.id }, 'Subscribed symbol to WS connection');
    return target;
  }

  unsubscribeSymbol(symbol) {
    if (!this.symbolToConn.has(symbol)) return;
    const idx = this.symbolToConn.get(symbol);
    const conn = this.connections[idx];
    if (!conn) return;

    // Find topics in this connection that match the symbol
    const topicsToUnsub = Array.from(conn._topics || []).filter(t => t.endsWith(`.${symbol}`));
    try {
      if (topicsToUnsub.length) conn.ws.send(JSON.stringify({ op: 'unsubscribe', args: topicsToUnsub }));
    } catch (e) {
      // ignore send errors on unsubscribe
    }
    conn.symbols.delete(symbol);
    for (const t of topicsToUnsub) conn._topics.delete(t);
    this.symbolToConn.delete(symbol);
    logger.info({ symbol, connId: conn.id, unsubscribedTopics: topicsToUnsub.length }, 'Unsubscribed symbol from connection');

    // If connection has no more symbols, close it
    if (conn.symbols.size === 0) {
      try { conn.ws.close(); } catch (e) { /* ignore */ }
    }
  }

  normalizeKlinePayload(d, tf, symbol) {
    // d may have keys: t / start, o / open, h / high, l / low, c / close, v / volume
    const open_time = d.t || d.start || d.open_time || d.k?.t || null;
    const open = Number(d.o || d.open || d.k?.o || d[1] || 0);
    const high = Number(d.h || d.high || d.k?.h || d[2] || 0);
    const low = Number(d.l || d.low || d.k?.l || d[3] || 0);
    const close = Number(d.c || d.close || d.k?.c || d[4] || 0);
    const volume = Number(d.v || d.volume || d.k?.v || d[5] || 0);
    return { open_time, open, high, low, close, volume, timeframe: tf, symbol };
  }

  async closeAll() {
    try {
      for (const conn of this.connections.slice()) {
        try {
          // attempt unsubscribe first
          const topics = Array.from(conn._topics || []);
          if (topics.length && conn.ws && conn.ws.readyState === conn.ws.OPEN) {
            try { conn.ws.send(JSON.stringify({ op: 'unsubscribe', args: topics })); } catch (e) { /* ignore */ }
          }
          if (conn.ws && (conn.ws.readyState === conn.ws.OPEN || conn.ws.readyState === conn.ws.CONNECTING)) {
            try { conn.ws.close(); } catch (e) { /* ignore */ }
          }
        } catch (e) {
          // continue closing others
        }
      }
      this.connections = [];
      this.symbolToConn = new Map();
      this.openSockets = 0;
      logger.info('WSManager: closed all connections');
    } catch (err) {
      logger.warn({ err }, 'WSManager.closeAll error');
    }
  }
}

module.exports = new WSManager();
