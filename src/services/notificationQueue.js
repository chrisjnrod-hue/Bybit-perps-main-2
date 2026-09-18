// src/services/notificationQueue.js
const logger = require('pino')();

const QUEUE_STATE = {
  IDLE: 'idle',
  STARTUP_SUMMARY: 'startup_summary',
  PROCESSING: 'processing'
};

class NotificationQueue {
  constructor() {
    this.queue = [];
    this.state = QUEUE_STATE.IDLE;
    this.processing = false;
    this.startupSummaryInProgress = false;
    this.sentSignalIds = new Set(); // Global deduplication
  }

  /**
   * Add a signal to the queue for notification
   * @param {object} signal - Signal object with symbol, root_tf, etc.
   * @param {string} type - 'startup', 'realtime', or 'candle_update'
   */
  enqueueSignal(signal, type = 'realtime') {
    if (!signal || !signal.symbol || !signal.root_tf) {
      logger.warn({ signal }, 'NotificationQueue: invalid signal, skipping');
      return false;
    }

    const sigId = `${signal.symbol}_${signal.root_tf}`;
    if (this.sentSignalIds.has(sigId) && type === 'startup') {
      logger.debug({ sigId }, 'NotificationQueue: signal already sent during startup, skipping duplicate');
      return false;
    }

    this.queue.push({
      type,
      signal,
      timestamp: Date.now(),
      id: sigId
    });

    logger.debug(
      { sigId, type, queueLength: this.queue.length },
      'NotificationQueue: signal enqueued'
    );

    // Trigger processing if idle
    if (!this.processing) {
      this.processQueue();
    }

    return true;
  }

  /**
   * Enqueue an entire startup batch
   * @param {array} signals - Array of signal objects
   */
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

    // Deduplicate against already-sent signals
    const uniqueSignals = signals.filter(sig => {
      const sigId = `${sig.symbol}_${sig.root_tf}`;
      if (this.sentSignalIds.has(sigId)) {
        logger.debug({ sigId }, 'NotificationQueue: filtering duplicate from startup batch');
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
      timestamp: Date.now()
    });

    if (!this.processing) {
      this.processQueue();
    }
  }

  /**
   * Process the queue sequentially
   */
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

          // Mark signal as sent
          if (item.id) {
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

  /**
   * Process startup batch: send summary header + individual signals + recommendations
   */
  async _processStartupBatch(signals) {
    try {
      const telegram = require('./telegram');

      logger.info({ count: signals.length }, 'NotificationQueue: starting startup batch flow');

      // Use telegram's sendStartupSummary which handles the full flow
      await telegram.sendStartupSummary({ snapshot: signals });

      logger.info('NotificationQueue: startup batch flow completed');
    } catch (err) {
      logger.error({ err }, 'NotificationQueue: startup batch processing failed');
      throw err;
    }
  }

  /**
   * Process realtime signal: send individual signal block immediately
   */
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

  /**
   * Process candle update: send root candle update summary
   */
  async _processCandleUpdate(signal) {
    try {
      const telegram = require('./telegram');

      logger.debug({ symbol: signal.symbol }, 'NotificationQueue: sending candle update');

      // TODO: Implement candle update message format in telegram.js
      // For now, log it
      logger.info(
        { symbol: signal.symbol },
        'NotificationQueue: candle update queued (not yet implemented)'
      );
    } catch (err) {
      logger.warn({ err }, 'NotificationQueue: candle update send failed');
      throw err;
    }
  }

  /**
   * Get queue status
   */
  getStatus() {
    return {
      state: this.state,
      processing: this.processing,
      queueLength: this.queue.length,
      startupInProgress: this.startupSummaryInProgress,
      sentSignalCount: this.sentSignalIds.size
    };
  }

  /**
   * Clear all sent signal IDs (for testing/reset)
   */
  resetSentSignals() {
    this.sentSignalIds.clear();
    logger.info('NotificationQueue: sent signals cache cleared');
  }
}

// Singleton instance
const notificationQueue = new NotificationQueue();

module.exports = notificationQueue;
