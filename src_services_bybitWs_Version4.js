const WebSocket = require('ws');
const config = require('../config');
const EventEmitter = require('events');
const logger = require('pino')();

class WSManager extends EventEmitter {
  constructor() {
    super();
    this.connections = [];
    this.symbolToConn = new Map();
    this.openSockets = 0;
    this.maxSockets = config.MAX_CONCURRENT_WS || 20;
    this.batchSize = config.BATCH_WS_SIZE || 20;
  }

  start() {
    logger.info('WS Manager (v5 batch mode) ready');
  }

  intervalToTopicPart(tf) {
    return tf === 'D' ? 'D' : String(tf);
  }

  _createConnection() {
    if (this.openSockets >= this.maxSockets) return null;
    const wsUrl = config.BYBIT_WS_PUBLIC;
    const ws = new WebSocket(wsUrl);
    const conn = { ws, symbols: new Set(), id: Date.now() + Math.random().toString(16).slice(2), _topics: new Set() };
    ws.on('open', () => {
      logger.info({ id: conn.id }, 'WS connection opened');
    });
    ws.on('message', (msg) => {
      try {
        const data = JSON.parse(msg);
        if (data.topic && data.data && Array.isArray(data.data)) {
          const topic = data.topic;
          const topicParts = topic.split('.');
          if (topicParts.length >= 3) {
            const tf = topicParts[1];
            const sym = topicParts.slice(2).join('.');
            const d = data.data[0];
            const k = this.normalizeKlinePayload(d, tf, sym);
            this.emit('kline', { symbol: sym, timeframe: tf, data: k, raw: data });
          }
        } else {
          if (data.ping) {
            try { ws.send(JSON.stringify({ pong: data.ping })); } catch (e) {}
          }
        }
      } catch (err) {
        logger.debug({ err }, 'ws message parse error');
      }
    });
    ws.on('error', (err) => logger.warn({ err }, 'ws error'));
    ws.on('close', () => {
      logger.info({ id: conn.id }, 'ws closed');
      for (const s of conn.symbols) this.symbolToConn.delete(s);
      this.connections = this.connections.filter(c => c !== conn);
      this.openSockets = Math.max(0, this.openSockets - 1);
    });

    this.connections.push(conn);
    this.openSockets++;
    return conn;
  }

  subscribeSymbolMTF(symbol, tfs = config.MTF_TFS) {
    if (this.symbolToConn.has(symbol)) return this.connections[this.symbolToConn.get(symbol)];
    let target = this.connections.find(c => c.symbols.size < this.batchSize);
    if (!target) {
      target = this._createConnection();
      if (!target) {
        logger.warn('No available WS connections to subscribe new symbol');
        return null;
      }
    }
    const tfParts = tfs.map(tf => this.intervalToTopicPart(tf));
    const topics = [];
    for (const tfp of tfParts) {
      topics.push(`klineV2.${tfp}.${symbol}`);
      topics.push(`kline.${tfp}.${symbol}`);
    }
    try {
      target.ws.send(JSON.stringify({ op: 'subscribe', args: topics }));
      for (const s of [symbol]) target.symbols.add(s);
      for (const t of topics) target._topics.add(t);
      this.symbolToConn.set(symbol, this.connections.indexOf(target));
      logger.info({ symbol, topics, connId: target.id }, 'Subscribed symbol to connection');
    } catch (err) {
      logger.warn({ err }, 'Failed to send subscribe on conn');
    }
    return target;
  }

  unsubscribeSymbol(symbol) {
    const idx = this.symbolToConn.get(symbol);
    if (idx === undefined) return;
    const conn = this.connections[idx];
    if (!conn) return;
    const topics = Array.from(conn._topics || []).filter(t => t.endsWith(`.${symbol}`));
    try { conn.ws.send(JSON.stringify({ op: 'unsubscribe', args: topics })); } catch (e) {}
    conn.symbols.delete(symbol);
    for (const t of topics) conn._topics.delete(t);
    this.symbolToConn.delete(symbol);
    logger.info({ symbol }, 'unsubscribed symbol from conn');
    if (conn.symbols.size === 0) {
      try { conn.ws.close(); } catch (e) {}
    }
  }

  normalizeKlinePayload(d, tf, symbol) {
    const open_time = d.t || d.start || d.open_time || d[0] || null;
    const open = Number(d.o || d.open || d.openPrice || d[1] || 0);
    const high = Number(d.h || d.high || d[2] || 0);
    const low = Number(d.l || d.low || d[3] || 0);
    const close = Number(d.c || d.close || d[4] || 0);
    const volume = Number(d.v || d.volume || d[5] || 0);
    return { open_time, open, high, low, close, volume, timeframe: tf, symbol };
  }
}

module.exports = new WSManager();