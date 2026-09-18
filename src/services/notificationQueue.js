// src/services/notificationQueue.js
const dbModule = require('../db');
const logger = require('pino')();

const QUEUE_STATE = {
  IDLE: 'idle',
  STARTUP_SUMMARY: 'startup_summary',
  PROCESSING: 'processing'
};

function signalKey(signal) {
  if (!signal || !signal.symbol || !signal.root_tf) {
    return null;
  }

  if (signal.eventId) {
    return String(signal.eventId);
  }

  if (
    signal.candleTime !== undefined &&
    signal.candleTime !== null
  ) {
    return [
      String(signal.symbol),
      String(signal.root_tf),
      String(Number(signal.candleTime))
    ].join('_');
  }

  return [
    String(signal.symbol),
    String(signal.root_tf)
  ].join('_');
}

function persistentStateKey(signalId) {
  if (!signalId) return null;
  return `telegram_signal_sent:${signalId}`;
}

class NotificationQueue {
  constructor() {
    this.queue = [];
    this.state = QUEUE_STATE.IDLE;
    this.processing = false;
    this.startupSummaryInProgress = false;

    /*
     * reservedSignalIds prevents realtime/startup races within the
     * current process.
     */
    this.reservedSignalIds = new Set();

    /*
     * sentSignalIds is only an in-memory optimization. Persistent
     * deduplication is performed through db notification_state.
     */
    this.sentSignalIds = new Set();
  }

  hasPersistentSentState(signalId) {
    const stateKey = persistentStateKey(signalId);

    if (!stateKey) {
      return false;
    }

    return Boolean(dbModule.getState(stateKey));
  }

  isDuplicate(signalId) {
    if (!signalId) {
      return true;
    }

    if (this.reservedSignalIds.has(signalId)) {
      return true;
    }

    if (this.sentSignalIds.has(signalId)) {
      return true;
    }

    return this.hasPersistentSentState(signalId);
  }

  reserve(signalId) {
    if (signalId) {
      this.reservedSignalIds.add(signalId);
    }
  }

  release(signalId) {
    if (signalId) {
      this.reservedSignalIds.delete(signalId);
    }
  }

  markSent(signalId) {
    if (!signalId) {
      return;
    }

    this.reservedSignalIds.delete(signalId);
    this.sentSignalIds.add(signalId);

    const stateKey = persistentStateKey(signalId);

    if (stateKey) {
      dbModule.setState(stateKey, true);
    }
  }

  enqueueSignal(signal, type = 'realtime') {
    if (!signal || !signal.symbol || !signal.root_tf) {
      logger.warn(
        { signal },
        'NotificationQueue: invalid signal, skipping'
      );
      return false;
    }

    const signalId = signalKey(signal);

    if (!signalId) {
      logger.warn(
        { signal },
        'NotificationQueue: unable to create signal ID, skipping'
      );
      return false;
    }

    if (this.isDuplicate(signalId)) {
      logger.debug(
        { signalId, type },
        'NotificationQueue: signal already queued or sent'
      );
      return false;
    }

    this.reserve(signalId);

    this.queue.push({
      type,
      signal,
      signalId,
      timestamp: Date.now()
    });

    logger.debug(
      {
        signalId,
        type,
        queueLength: this.queue.length
      },
      'NotificationQueue: signal reserved and enqueued'
    );

    this.startProcessing();

    return true;
  }

  enqueueStartupBatch(signals) {
    if (!Array.isArray(signals)) {
      logger.warn('NotificationQueue: invalid startup batch');
      return false;
    }

    if (this.startupSummaryInProgress) {
      logger.info(
        'NotificationQueue: startup summary already in progress'
      );
      return false;
    }

    const uniqueSignals = [];
    const batchIds = new Set();

    for (const signal of signals) {
      const signalId = signalKey(signal);

      if (!signalId) {
        continue;
      }

      if (batchIds.has(signalId)) {
        logger.debug(
          { signalId },
          'NotificationQueue: duplicate inside startup batch'
        );
        continue;
      }

      if (this.isDuplicate(signalId)) {
        logger.debug(
          { signalId },
          'NotificationQueue: startup signal already queued or sent'
        );
        continue;
      }

      batchIds.add(signalId);
      this.reserve(signalId);
      uniqueSignals.push(signal);
    }

    if (uniqueSignals.length === 0) {
      logger.info(
        { inputCount: signals.length },
        'NotificationQueue: startup batch has no new signals'
      );
      return false;
    }

    this.startupSummaryInProgress = true;
    this.state = QUEUE_STATE.STARTUP_SUMMARY;

    const batchId = [
      'startup_batch',
      Date.now(),
      Math.random().toString(36).slice(2, 8)
    ].join('_');

    this.queue.push({
      type: 'startup_batch',
      signals: uniqueSignals,
      signalIds: Array.from(batchIds),
      batchId,
      timestamp: Date.now()
    });

    logger.info(
      {
        inputCount: signals.length,
        uniqueCount: uniqueSignals.length,
        batchId
      },
      'NotificationQueue: startup batch reserved and enqueued'
    );

    this.startProcessing();

    return true;
  }

  startProcessing() {
    if (this.processing) {
      return;
    }

    this.processQueue().catch((err) => {
      logger.error(
        { err },
        'NotificationQueue: processQueue failed'
      );
    });
  }

  async processQueue() {
    if (this.processing) {
      return;
    }

    this.processing = true;
    this.state = QUEUE_STATE.PROCESSING;

    try {
      while (this.queue.length > 0) {
        const item = this.queue.shift();

        if (!item) {
          continue;
        }

        logger.debug(
          {
            type: item.type,
            signalId: item.signalId,
            batchId: item.batchId,
            queueRemaining: this.queue.length
          },
          'NotificationQueue: processing item'
        );

        try {
          if (item.type === 'startup_batch') {
            await this._processStartupBatch(item.signals);

            for (const signalId of item.signalIds || []) {
              this.markSent(signalId);
            }
          } else if (item.type === 'realtime') {
            await this._processRealtimeSignal(item.signal);
            this.markSent(item.signalId);
          } else if (item.type === 'candle_update') {
            await this._processCandleUpdate(item.signal);
            this.markSent(item.signalId);
          }
        } catch (err) {
          /*
           * Failed sends are released so they can be retried.
           * They are not marked as sent.
           */
          if (item.type === 'startup_batch') {
            for (const signalId of item.signalIds || []) {
              this.release(signalId);
            }
          } else {
            this.release(item.signalId);
          }

          logger.error(
            {
              err,
              type: item.type,
              signalId: item.signalId,
              batchId: item.batchId
            },
            'NotificationQueue: item processing failed'
          );
        }
      }
    } finally {
      this.processing = false;
      this.state = QUEUE_STATE.IDLE;
      this.startupSummaryInProgress = false;

      logger.info(
        'NotificationQueue: queue processing completed'
      );
    }
  }

  async _processStartupBatch(signals) {
    const telegram = require('./telegram');

    logger.info(
      { count: signals.length },
      'NotificationQueue: starting startup batch flow'
    );

    await telegram.sendStartupSummary({
      snapshot: signals
    });

    logger.info(
      'NotificationQueue: startup batch flow completed'
    );
  }

  async _processRealtimeSignal(signal) {
    const telegram = require('./telegram');

    logger.info(
      {
        symbol: signal.symbol,
        root_tf: signal.root_tf,
        eventId: signal.eventId
      },
      'NotificationQueue: sending realtime signal block'
    );

    await telegram.sendNewSignalSingleBlock(signal);

    logger.info(
      {
        symbol: signal.symbol,
        root_tf: signal.root_tf,
        eventId: signal.eventId
      },
      'NotificationQueue: realtime signal block sent'
    );
  }

  async _processCandleUpdate(signal) {
    logger.debug(
      {
        symbol: signal && signal.symbol,
        root_tf: signal && signal.root_tf,
        eventId: signal && signal.eventId
      },
      'NotificationQueue: candle update received'
    );
  }

  getStatus() {
    return {
      state: this.state,
      processing: this.processing,
      queueLength: this.queue.length,
      startupInProgress: this.startupSummaryInProgress,
      reservedSignalCount: this.reservedSignalIds.size,
      sentSignalCount: this.sentSignalIds.size
    };
  }

  resetSentSignals() {
    this.sentSignalIds.clear();
    logger.info(
      'NotificationQueue: in-memory sent cache cleared'
    );
  }

  resetAllSignalTracking() {
    this.reservedSignalIds.clear();
    this.sentSignalIds.clear();

    logger.info(
      'NotificationQueue: all in-memory signal tracking cleared'
    );
  }
}

module.exports = new NotificationQueue();
