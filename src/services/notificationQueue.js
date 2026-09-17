// src/services/notificationQueue.js
const logger = require('pino')();

const QUEUE_STATE = {
  IDLE: 'idle',
  STARTUP_SUMMARY: 'startup_summary',
  PROCESSING: 'processing'
};

function signalKey(signal) {
  if (!signal || !signal.symbol || !signal.root_tf) return null;
  if (signal.eventId) return String(signal.eventId);
  return `${String(signal.symbol)}_${String(signal.root_tf)}_${Number(signal.candleTime || Date.now())}`;
}

class NotificationQueue {
  constructor() {
    this.queue = [];
    this.state = QUEUE_STATE.IDLE;
    this.processing = false;
    this.startupSummaryInProgress = false;
    this.sentSignalIds = new Set();
  }

  enqueueSignal(signal, type = 'realtime') {
    if (!signal || !signal.symbol || !signal.root_tf) {
      logger.warn({ signal }, 'NotificationQueue: invalid signal, skipping');
      return false;
    }

    const sigId = signalKey(signal);

    if (this.sentSignalIds.has(sigId)) {
      logger.debug({ sigId, type }, 'NotificationQueue: signal already sent or queued, skipping duplicate');
      return false;
    }

    this.queue.push({
      type,
      signal,
      timestamp: Date.now(),
      id: sigId
    });

    logger.debug({ sigId, type, queueLength: this.queue.length }, 'NotificationQueue: signal enqueued');

    if (!this.processing) {
      this.processQueue();
    }

    return true;
  }

  enqueueStartupBatch(signals) {
    if (!Array.isArray(signals)) {
      logger.warn('NotificationQueue: invalid startup batch');
      return;
    }

    if (this.startupSummaryInProgress) {
      logger.info('NotificationQueue: startup summary already in progress, skipping duplicate batch');
      return;
    }

    this.startupSummaryInProgress = true;
    this.state = QUEUE_STATE.STARTUP_SUMMARY;

    const uniqueSignals = signals.filter(sig => {
      const sigId = signalKey(sig);
      if (!sigId) return false;
      if (this.sentSignalIds.has(sigId)) {
        logger.debug({ sigId }, 'NotificationQueue: filtering duplicate startup signal');
        return false;
      }
      return true;
    });

    logger.info(
      { total: signals.length, unique: uniqueSignals.length },
      'NotificationQueue: enqueuing startup batch'
    );

    this.queue.push({
      type: 'startup_batch',
      signals: uniqueSignals,
      timestamp: Date.now(),
      id: `startup_batch_${Date.now()}`
    });

    if (!this.processing) {
      this.processQueue();
    }
  }

  async processQueue() {
    if (this.processing) {
      logger.debug('NotificationQueue: already processing');
      return;
    }

    this.processing = true;

    try {
      while (this.queue.length > 0) {
        const item = this.queue.shift();
        logger.debug({ type: item.type, queueRemaining: this.queue.length }, 'NotificationQueue: processing item');

        try {
          if (item.type === 'startup_batch') {
            await this._processStartupBatch(item.signals);
          } else if (item.type === 'realtime') {
            await this._processRealtimeSignal(item.signal);
          } else if (item.type === 'candle_update') {
            await this._processCandleUpdate(item.signal);
          }

          if (item.type === 'startup_batch' && Array.isArray(item.signals)) {
            for (const sig of item.signals) {
              const key = signalKey(sig);
              if (key) this.sentSignalIds.add(key);
            }
          } else if (item.id) {
            this.sentSignalIds.add(item.id);
          }
        } catch (err) {
          logger.error({ err, itemType: item.type }, 'NotificationQueue: item processing failed (continuing)');
        }
      }

      this.state = QUEUE_STATE.IDLE;
      logger.info('NotificationQueue: queue processing completed');
    } catch (err) {
      logger.error({ err }, 'NotificationQueue: fatal error during processing');
    } finally {
      this.processing = false;
      this.startupSummaryInProgress = false;
    }
  }

  async _processStartupBatch(signals) {
    try {
      const telegram = require('./telegram');

      logger.info({ count: signals.length }, 'NotificationQueue: starting startup batch flow');
      await telegram.sendStartupSummary({ snapshot: signals });
      logger.info('NotificationQueue: startup batch flow completed');
    } catch (err) {
      logger.error({ err }, 'NotificationQueue: startup batch processing failed');
      throw err;
    }
  }

  async _processRealtimeSignal(signal) {
    try {
      const telegram = require('./telegram');

      logger.debug({ symbol: signal.symbol, root_tf: signal.root_tf }, 'NotificationQueue: sending realtime signal block');

      await telegram.sendNewSignalSingleBlock(signal);

      logger.info(
        { symbol: signal.symbol, root_tf: signal.root_tf },
        'NotificationQueue: realtime signal block sent'
      );
    } catch (err) {
      logger.warn({ err, symbol: signal.symbol }, 'NotificationQueue: realtime signal send failed');
      throw err;
    }
  }

  async _processCandleUpdate(signal) {
    try {
      const telegram = require('./telegram');

      logger.debug({ symbol: signal.symbol }, 'NotificationQueue: sending candle update');

      logger.info(
        { symbol: signal.symbol },
        'NotificationQueue: candle update queued (not yet implemented)'
      );
    } catch (err) {
      logger.warn({ err }, 'NotificationQueue: candle update send failed');
      throw err;
    }
  }

  getStatus() {
    return {
      state: this.state,
      processing: this.processing,
      queueLength: this.queue.length,
      startupInProgress: this.startupSummaryInProgress,
      sentSignalCount: this.sentSignalIds.size
    };
  }

  resetSentSignals() {
    this.sentSignalIds.clear();
    logger.info('NotificationQueue: sent signals cache cleared');
  }
}

const notificationQueue = new NotificationQueue();

module.exports = notificationQueue;
