const logger = require('pino')();

const QUEUE_STATE = {
  IDLE: 'idle',
  STARTUP_SUMMARY: 'startup_summary',
  PROCESSING: 'processing'
};

function getSignalId(signal) {
  if (!signal || !signal.symbol || !signal.root_tf) {
    return null;
  }

  /*
   * Prefer a stable signal/candle identifier when one exists.
   * The fallback preserves the existing symbol/timeframe behavior.
   */
  if (signal.id !== undefined && signal.id !== null) {
    return String(signal.id);
  }

  if (
    signal.signal_id !== undefined &&
    signal.signal_id !== null
  ) {
    return String(signal.signal_id);
  }

  if (
    signal.candle_open_time !== undefined &&
    signal.candle_open_time !== null
  ) {
    return [
      signal.symbol,
      signal.root_tf,
      signal.candle_open_time
    ].join('_');
  }

  return [
    signal.symbol,
    signal.root_tf
  ].join('_');
}

class NotificationQueue {
  constructor() {
    this.queue = [];
    this.state = QUEUE_STATE.IDLE;
    this.processing = false;
    this.startupSummaryInProgress = false;

    // Signals successfully delivered or currently reserved.
    this.sentSignalIds = new Set();
    this.pendingSignalIds = new Set();
  }

  isKnownSignal(signal) {
    const signalId = getSignalId(signal);

    if (!signalId) {
      return true;
    }

    return (
      this.sentSignalIds.has(signalId) ||
      this.pendingSignalIds.has(signalId)
    );
  }

  reserveSignal(signal) {
    const signalId = getSignalId(signal);

    if (signalId) {
      this.pendingSignalIds.add(signalId);
    }

    return signalId;
  }

  markSignalSent(signalId) {
    if (!signalId) {
      return;
    }

    this.pendingSignalIds.delete(signalId);
    this.sentSignalIds.add(signalId);
  }

  releaseSignal(signalId) {
    if (!signalId) {
      return;
    }

    this.pendingSignalIds.delete(signalId);
  }

  enqueueSignal(signal, type = 'realtime') {
    if (
      !signal ||
      !signal.symbol ||
      !signal.root_tf
    ) {
      logger.warn(
        { signal },
        'NotificationQueue: invalid signal, skipping'
      );

      return false;
    }

    const signalId = getSignalId(signal);

    if (this.isKnownSignal(signal)) {
      logger.debug(
        {
          signalId,
          symbol: signal.symbol,
          root_tf: signal.root_tf,
          type
        },
        'NotificationQueue: duplicate signal, skipping'
      );

      return false;
    }

    this.reserveSignal(signal);

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
      'NotificationQueue: signal enqueued'
    );

    this.startProcessing();

    return true;
  }

  enqueueStartupBatch(signals) {
    if (!Array.isArray(signals)) {
      logger.warn(
        'NotificationQueue: invalid startup batch'
      );

      return false;
    }

    if (this.startupSummaryInProgress) {
      logger.info(
        'NotificationQueue: startup summary already in progress, skipping duplicate batch'
      );

      return false;
    }

    const uniqueSignals = [];
    const reservedIds = new Set();

    for (const signal of signals) {
      if (
        !signal ||
        !signal.symbol ||
        !signal.root_tf
      ) {
        continue;
      }

      const signalId = getSignalId(signal);

      if (
        reservedIds.has(signalId) ||
        this.isKnownSignal(signal)
      ) {
        logger.debug(
          {
            signalId
          },
          'NotificationQueue: filtering duplicate startup signal'
        );

        continue;
      }

      reservedIds.add(signalId);
      this.reserveSignal(signal);
      uniqueSignals.push(signal);
    }

    if (uniqueSignals.length === 0) {
      logger.info(
        'NotificationQueue: startup batch contained no new signals'
      );

      return false;
    }

    this.startupSummaryInProgress = true;
    this.state = QUEUE_STATE.STARTUP_SUMMARY;

    this.queue.push({
      type: 'startup_batch',
      signals: uniqueSignals,
      signalIds: uniqueSignals.map(getSignalId),
      timestamp: Date.now()
    });

    logger.info(
      {
        total: signals.length,
        unique: uniqueSignals.length,
        queueLength: this.queue.length
      },
      'NotificationQueue: startup batch enqueued'
    );

    this.startProcessing();

    return true;
  }

  startProcessing() {
    if (this.processing) {
      return;
    }

    void this.processQueue();
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

        logger.debug(
          {
            type: item.type,
            queueRemaining: this.queue.length
          },
          'NotificationQueue: processing item'
        );

        try {
          if (item.type === 'startup_batch') {
            await this._processStartupBatch(item.signals);

            for (const signalId of item.signalIds || []) {
              this.markSignalSent(signalId);
            }
          } else if (item.type === 'realtime') {
            await this._processRealtimeSignal(item.signal);
            this.markSignalSent(item.signalId);
          } else if (item.type === 'candle_update') {
            await this._processCandleUpdate(item.signal);
            this.markSignalSent(item.signalId);
          }
        } catch (err) {
          /*
           * Do not permanently suppress a notification that failed to
           * send. It can be retried by a later scan.
           */
          if (item.type === 'startup_batch') {
            for (const signalId of item.signalIds || []) {
              this.releaseSignal(signalId);
            }
          } else {
            this.releaseSignal(item.signalId);
          }

          logger.error(
            {
              err,
              itemType: item.type
            },
            'NotificationQueue: item processing failed'
          );
        }
      }

      this.state = QUEUE_STATE.IDLE;

      logger.info(
        'NotificationQueue: queue processing completed'
      );
    } catch (err) {
      logger.error(
        { err },
        'NotificationQueue: fatal processing error'
      );
    } finally {
      this.processing = false;
      this.startupSummaryInProgress = false;
      this.state = QUEUE_STATE.IDLE;

      /*
       * A producer may have enqueued an item during the final loop
       * iteration. Ensure it is not stranded.
       */
      if (this.queue.length > 0) {
        this.startProcessing();
      }
    }
  }

  async _processStartupBatch(signals) {
    const telegram = require('./telegram');

    logger.info(
      {
        count: signals.length
      },
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

    logger.debug(
      {
        symbol: signal.symbol,
        root_tf: signal.root_tf
      },
      'NotificationQueue: sending realtime signal block'
    );

    await telegram.sendNewSignalSingleBlock(signal);

    logger.info(
      {
        symbol: signal.symbol,
        root_tf: signal.root_tf
      },
      'NotificationQueue: realtime signal block sent'
    );
  }

  async _processCandleUpdate(signal) {
    /*
     * Keep this method available for future candle-update messages.
     * Do not silently mark an update as delivered unless it was actually
     * sent through Telegram.
     */
    logger.info(
      {
        symbol: signal.symbol,
        root_tf: signal.root_tf
      },
      'NotificationQueue: candle update not implemented'
    );
  }

  getStatus() {
    return {
      state: this.state,
      processing: this.processing,
      queueLength: this.queue.length,
      startupInProgress: this.startupSummaryInProgress,
      sentSignalCount: this.sentSignalIds.size,
      pendingSignalCount: this.pendingSignalIds.size
    };
  }

  resetSentSignals() {
    this.sentSignalIds.clear();
    this.pendingSignalIds.clear();

    logger.info(
      'NotificationQueue: signal caches cleared'
    );
  }
}

module.exports = new NotificationQueue();
