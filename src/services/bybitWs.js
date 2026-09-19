const WebSocket = require('ws');
const EventEmitter = require('events');
const config = require('../config');
const logger = require('pino')();

function validateSymbol(symbol) {
  if (!symbol || typeof symbol !== 'string') {
    return false;
  }

  if (!config.SYMBOL_FILTER) {
    return true;
  }

  try {
    return new RegExp(config.SYMBOL_FILTER).test(symbol);
  } catch (err) {
    logger.warn(
      {
        filter: config.SYMBOL_FILTER,
        err: err.message
      },
      'Invalid SYMBOL_FILTER regex'
    );

    return true;
  }
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function getWsUrl() {
  const configured =
    process.env.BYBIT_WS_PUBLIC ||
    config.BYBIT_WS_PUBLIC;

  if (configured) {
    return String(configured);
  }

  const testnet =
    String(process.env.MAINNET || 'true').toLowerCase() === 'false';

  return testnet
    ? 'wss://stream-testnet.bybit.com/v5/public/linear'
    : 'wss://stream.bybit.com/v5/public/linear';
}

class WSManager extends EventEmitter {
  constructor() {
    super();

    this.connections = [];
    this.symbolToConn = new Map();
    this.desiredSymbols = new Map();
    this.klineBuffer = new Map();

    this.started = false;
    this.shuttingDown = false;
    this.connectionSequence = 0;

    this.maxSockets = Math.max(
      1,
      Number(config.MAX_CONCURRENT_WS || 20)
    );

    this.batchSize = Math.max(
      1,
      Number(config.BATCH_WS_SIZE || 10)
    );

    this.subscribeChunk = Math.max(
      1,
      Number(config.WS_SUBSCRIBE_CHUNK || 10)
    );

    this.connectTimeoutMs = Number(
      config.WS_CONNECT_TIMEOUT_MS || 15000
    );

    this.reconnectDelayMs = Number(
      config.WS_RECONNECT_DELAY_MS || 1000
    );

    this.reconnectBackoffFactor = Number(
      config.WS_RECONNECT_BACKOFF_FACTOR || 2
    );

    this.reconnectMaxMs = Number(
      config.WS_RECONNECT_MAX_MS || 30000
    );

    this.maxReconnectAttempts = Number(
      config.WS_MAX_RECONNECT_ATTEMPTS || 10
    );
  }

  start() {
    if (this.started) {
      return;
    }

    this.started = true;
    this.shuttingDown = false;

    logger.info(
      {
        wsUrl: getWsUrl(),
        maxSockets: this.maxSockets,
        batchSize: this.batchSize,
        subscribeChunk: this.subscribeChunk,
        connectTimeoutMs: this.connectTimeoutMs
      },
      'WS Manager starting'
    );

    this._createConnection();
  }

  intervalToTopicPart(timeframe) {
    const value = String(timeframe).toUpperCase();

    if (value === '1H' || value === 'H') {
      return '60';
    }

    if (value === '1D') {
      return 'D';
    }

    return value;
  }

  _createConnection() {
    if (this.shuttingDown) {
      return null;
    }

    if (this.connections.length >= this.maxSockets) {
      logger.warn(
        { maxSockets: this.maxSockets },
        'Maximum WS connection count reached'
      );

      return null;
    }

    const wsUrl = getWsUrl();

    let ws;

    try {
      ws = new WebSocket(wsUrl);
    } catch (err) {
      logger.error(
        { err, wsUrl },
        'Failed to create WebSocket'
      );

      return null;
    }

    const conn = {
      ws,
      id: `${Date.now()}-${++this.connectionSequence}`,
      symbols: new Set(),
      topics: new Set(),
      pendingTopics: new Set(),
      ready: false,
      closed: false,
      reconnectAttempts: 0,
      reconnectTimer: null,
      connectTimer: null,
      flushTimer: null
    };

    this.connections.push(conn);

    conn.connectTimer = setTimeout(() => {
      if (
        !conn.ready &&
        conn.ws.readyState !== WebSocket.OPEN
      ) {
        logger.warn(
          {
            connId: conn.id,
            timeoutMs: this.connectTimeoutMs
          },
          'WS connection timed out before OPEN'
        );

        try {
          conn.ws.terminate();
        } catch (err) {
          logger.debug({ err }, 'Failed to terminate timed-out WS');
        }
      }
    }, this.connectTimeoutMs);

    ws.on('open', () => {
      if (conn.closed) {
        return;
      }

      conn.ready = true;
      conn.reconnectAttempts = 0;

      if (conn.connectTimer) {
        clearTimeout(conn.connectTimer);
        conn.connectTimer = null;
      }

      logger.info(
        {
          connId: conn.id,
          wsUrl
        },
        'WS connection opened'
      );

      this._flushPending(conn);
    });

    ws.on('message', message => {
      this._handleMessage(conn, message);
    });

    ws.on('error', err => {
      logger.error(
        {
          connId: conn.id,
          message: err && err.message,
          code: err && err.code
        },
        'WS connection error'
      );
    });

    ws.on('close', (code, reason) => {
      this._handleClose(
        conn,
        code,
        reason ? reason.toString() : ''
      );
    });

    logger.info(
      {
        connId: conn.id,
        wsUrl
      },
      'WS connection created'
    );

    return conn;
  }

  _handleMessage(conn, rawMessage) {
    let data;

    try {
      data = JSON.parse(rawMessage.toString());
    } catch (err) {
      logger.debug(
        { err: err.message },
        'Unable to parse WS message'
      );

      return;
    }

    // Bybit V5 heartbeat.
    if (data.op === 'ping') {
      try {
        if (conn.ws.readyState === WebSocket.OPEN) {
          conn.ws.send(JSON.stringify({ op: 'pong' }));
        }
      } catch (err) {
        logger.debug({ err }, 'Failed to send WS pong');
      }

      return;
    }

    if (data.success === false || data.retCode !== undefined) {
      if (data.success === false || Number(data.retCode) !== 0) {
        logger.warn(
          {
            connId: conn.id,
            retCode: data.retCode,
            retMsg: data.retMsg || data.msg
          },
          'Bybit WS response error'
        );
      }

      return;
    }

    if (
      typeof data.topic !== 'string' ||
      !data.topic.startsWith('kline.')
    ) {
      return;
    }

    const parts = data.topic.split('.');

    if (parts.length < 3 || !Array.isArray(data.data)) {
      return;
    }

    const timeframe = parts[1];
    const symbol = parts.slice(2).join('.');

    if (!validateSymbol(symbol)) {
      return;
    }

    // Bybit V5 normally sends the newest candle first.
    for (const item of data.data) {
      const normalized = this.normalizeKlinePayload(
        item,
        timeframe,
        symbol
      );

      if (!normalized || !normalized.open_time) {
        continue;
      }

      if (!this.klineBuffer.has(symbol)) {
        this.klineBuffer.set(symbol, {});
      }

      this.klineBuffer.get(symbol)[timeframe] = normalized;

      this.emit('kline', {
        symbol,
        timeframe,
        data: normalized,
        raw: data
      });
    }
  }

  _handleClose(conn, code, reason) {
    if (conn.closed) {
      return;
    }

    conn.closed = true;
    conn.ready = false;

    if (conn.connectTimer) {
      clearTimeout(conn.connectTimer);
      conn.connectTimer = null;
    }

    if (conn.flushTimer) {
      clearTimeout(conn.flushTimer);
      conn.flushTimer = null;
    }

    this.connections = this.connections.filter(item =>
      item !== conn
    );

    const symbols = Array.from(conn.symbols);

    for (const symbol of symbols) {
      if (this.symbolToConn.get(symbol) === conn) {
        this.symbolToConn.delete(symbol);
      }
    }

    logger.warn(
      {
        connId: conn.id,
        code,
        reason,
        symbols: symbols.length
      },
      'WS connection closed'
    );

    if (
      !this.shuttingDown &&
      symbols.length > 0
    ) {
      this._recoverSymbols(
        symbols,
        conn.reconnectAttempts
      );
    }
  }

  _recoverSymbols(symbols, previousAttempts = 0) {
    if (this.shuttingDown) {
      return;
    }

    const attempts = previousAttempts + 1;

    if (attempts > this.maxReconnectAttempts) {
      logger.error(
        {
          symbols: symbols.length,
          attempts
        },
        'WS maximum reconnect attempts reached; symbols abandoned'
      );

      return;
    }

    const delay = Math.min(
      this.reconnectDelayMs *
        Math.pow(this.reconnectBackoffFactor, attempts - 1),
      this.reconnectMaxMs
    );

    logger.warn(
      {
        symbols: symbols.length,
        attempts,
        delayMs: delay
      },
      'Scheduling WS symbol recovery'
    );

    setTimeout(() => {
      if (this.shuttingDown) {
        return;
      }

      for (const symbol of symbols) {
        const timeframes =
          this.desiredSymbols.get(symbol);

        if (timeframes) {
          this.subscribeSymbolMTF(
            symbol,
            timeframes
          );
        }
      }
    }, delay);
  }

  _getAvailableConnection() {
    return this.connections.find(conn =>
      !conn.closed &&
      conn.symbols.size < this.batchSize
    ) || this._createConnection();
  }

  _sendTopics(conn, topics, operation = 'subscribe') {
    if (
      !conn ||
      conn.closed ||
      !conn.ws ||
      conn.ws.readyState !== WebSocket.OPEN
    ) {
      for (const topic of topics) {
        conn && conn.pendingTopics.add(topic);
      }

      return Promise.reject(
        new Error('WS socket is not OPEN')
      );
    }

    return new Promise((resolve, reject) => {
      const payload = JSON.stringify({
        op: operation,
        args: topics
      });

      conn.ws.send(payload, err => {
        if (err) {
          for (const topic of topics) {
            conn.pendingTopics.add(topic);
          }

          reject(err);
          return;
        }

        for (const topic of topics) {
          conn.pendingTopics.delete(topic);

          if (operation === 'subscribe') {
            conn.topics.add(topic);
          } else {
            conn.topics.delete(topic);
          }
        }

        resolve();
      });
    });
  }

  _flushPending(conn) {
    if (
      !conn ||
      conn.closed ||
      !conn.ready ||
      conn.ws.readyState !== WebSocket.OPEN ||
      conn.pendingTopics.size === 0
    ) {
      return;
    }

    const topics = Array.from(conn.pendingTopics);

    const batches = [];

    for (
      let index = 0;
      index < topics.length;
      index += this.subscribeChunk
    ) {
      batches.push(
        topics.slice(index, index + this.subscribeChunk)
      );
    }

    const sendBatch = async () => {
      for (const batch of batches) {
        if (
          conn.closed ||
          conn.ws.readyState !== WebSocket.OPEN
        ) {
          return;
        }

        try {
          await this._sendTopics(
            conn,
            batch,
            'subscribe'
          );

          // Avoid sending a large burst of subscriptions.
          await sleep(100);
        } catch (err) {
          logger.warn(
            {
              connId: conn.id,
              err: err.message
            },
            'Failed to send WS subscription batch'
          );

          return;
        }
      }

      logger.info(
        {
          connId: conn.id,
          topics: topics.length
        },
        'WS pending subscriptions flushed'
      );
    };

    sendBatch().catch(err => {
      logger.warn(
        { connId: conn.id, err },
        'WS subscription flush failed'
      );
    });
  }

  subscribeSymbolMTF(symbol, timeframes = null) {
    if (!validateSymbol(symbol)) {
      logger.debug(
        { symbol },
        'Rejected symbol from WS subscription'
      );

      return null;
    }

    const requestedTimeframes = (
      timeframes ||
      config.MTF_TFS ||
      ['5', '15', '60', '240', 'D']
    ).map(String);

    this.desiredSymbols.set(
      symbol,
      requestedTimeframes
    );

    const existing = this.symbolToConn.get(symbol);

    if (existing && !existing.closed) {
      return existing;
    }

    const conn = this._getAvailableConnection();

    if (!conn) {
      logger.warn(
        { symbol },
        'No available WS connection for symbol'
      );

      return null;
    }

    const topics = requestedTimeframes.map(timeframe => {
      const interval = this.intervalToTopicPart(timeframe);

      // Bybit V5 topic format.
      return `kline.${interval}.${symbol}`;
    });

    for (const topic of topics) {
      conn.pendingTopics.add(topic);
    }

    conn.symbols.add(symbol);
    this.symbolToConn.set(symbol, conn);

    logger.info(
      {
        symbol,
        connId: conn.id,
        topicCount: topics.length,
        socketState: conn.ws.readyState
      },
      'WS symbol queued'
    );

    if (conn.ws.readyState === WebSocket.OPEN) {
      conn.ready = true;
      this._flushPending(conn);
    }

    return conn;
  }

  unsubscribeSymbol(symbol) {
    const conn = this.symbolToConn.get(symbol);

    this.desiredSymbols.delete(symbol);

    if (!conn) {
      return;
    }

    const topics = Array.from(conn.topics).filter(topic =>
      topic.endsWith(`.${symbol}`)
    );

    conn.symbols.delete(symbol);
    this.symbolToConn.delete(symbol);

    for (const topic of topics) {
      conn.topics.delete(topic);
      conn.pendingTopics.delete(topic);
    }

    if (
      topics.length > 0 &&
      conn.ws.readyState === WebSocket.OPEN
    ) {
      this._sendTopics(
        conn,
        topics,
        'unsubscribe'
      ).catch(err => {
        logger.debug(
          { err, symbol },
          'WS unsubscribe failed'
        );
      });
    }

    this.klineBuffer.delete(symbol);

    if (conn.symbols.size === 0) {
      conn.closed = true;

      try {
        conn.ws.close();
      } catch (err) {
        logger.debug({ err }, 'Failed to close empty WS connection');
      }
    }
  }

  normalizeKlinePayload(item, timeframe, symbol) {
    if (Array.isArray(item)) {
      const start = Number(item[0]);

      return {
        open_time: start < 100000000000
          ? start * 1000
          : start,
        open: Number(item[1] || 0),
        high: Number(item[2] || 0),
        low: Number(item[3] || 0),
        close: Number(item[4] || 0),
        volume: Number(item[5] || 0),
        timeframe,
        symbol
      };
    }

    if (!item || typeof item !== 'object') {
      return null;
    }

    const rawOpenTime =
      item.start ??
      item.startTime ??
      item.t ??
      item.open_time;

    const numericOpenTime = Number(rawOpenTime);

    if (!Number.isFinite(numericOpenTime)) {
      return null;
    }

    return {
      open_time: numericOpenTime < 100000000000
        ? numericOpenTime * 1000
        : numericOpenTime,
      open: Number(item.open ?? item.o ?? 0),
      high: Number(item.high ?? item.h ?? 0),
      low: Number(item.low ?? item.l ?? 0),
      close: Number(item.close ?? item.c ?? 0),
      volume: Number(item.volume ?? item.v ?? 0),
      timeframe,
      symbol
    };
  }

  async performInitialScan() {
    const symbols = [];

    for (const symbol of this.desiredSymbols.keys()) {
      if (validateSymbol(symbol)) {
        symbols.push({
          symbol,
          base: symbol.replace(/USDT(\.P)?$/i, ''),
          quote: 'USDT'
        });
      }
    }

    logger.info(
      { count: symbols.length },
      'WS initial scan completed'
    );

    return symbols;
  }

  async closeAll() {
    this.shuttingDown = true;
    this.started = false;

    for (const conn of this.connections.slice()) {
      conn.closed = true;

      if (conn.connectTimer) {
        clearTimeout(conn.connectTimer);
      }

      if (conn.flushTimer) {
        clearTimeout(conn.flushTimer);
      }

      try {
        if (
          conn.ws.readyState === WebSocket.OPEN ||
          conn.ws.readyState === WebSocket.CONNECTING
        ) {
          conn.ws.close();
        }
      } catch (err) {
        logger.debug(
          { err, connId: conn.id },
          'Failed to close WS connection'
        );
      }
    }

    this.connections = [];
    this.symbolToConn.clear();
    this.desiredSymbols.clear();
    this.klineBuffer.clear();

    logger.info('WS Manager closed all connections');
  }
}

module.exports = new WSManager();
