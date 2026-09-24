const logger = require('pino')();

const QUEUE_STATE = {
  IDLE: 'idle',
  STARTUP_SUMMARY: 'startup_summary',
  ROOT_CANDLE_SUMMARY: 'root_candle_summary',
  PROCESSING: 'processing'
};

function getNotificationType(signal) {
  if (!signal) {
    return 'signal';
  }

  if (
    signal.notificationType &&
    typeof signal.notificationType === 'string' &&
    signal.notificationType.trim()
  ) {
    return signal.notificationType.trim();
  }

  return 'signal';
}

function getSignalId(signal) {
  if (!signal || !signal.symbol || !signal.root_tf) {
    return null;
  }

  const candleValue =
    signal.candle_open_time !== undefined && signal.candle_open_time !== null
      ? Number(signal.candle_open_time)
      : null;

  if (Number.isFinite(candleValue)) {
    return [
      signal.symbol,
      signal.root_tf,
      candleValue
    ].join('_');
  }

  return [
    signal.symbol,
    signal.root_tf,
    signal.detected_at !== undefined && signal.detected_at !== null
      ? Number(signal.detected_at)
      : Date.now()
  ].join('_');
}

class NotificationQueue {
  constructor() {
    this.queue = [];
    this.state = QUEUE_STATE.IDLE;
    this.processing = false;
    this.startupSummaryInProgress = false;
    this.rootCandleSummaryInProgress = false;

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

  normalizeSignal(signal, fallbackType = null) {
    if (!signal || !signal.symbol || !signal.root_tf) {
      return null;
    }

    const nextSignal = {
      ...signal
    };

    const resolvedType =
      signal.notificationType ||
      fallbackType ||
      getNotificationType(signal);

    nextSignal.notificationType = resolvedType;

    return nextSignal;
  }

  enqueueSignal(signal, type = 'realtime') {
    if (!signal || !signal.symbol || !signal.root_tf) {
      logger.warn(
        { signal },
        'NotificationQueue: invalid signal, skipping'
      );

      return false;
    }

    const normalized = this.normalizeSignal(
      signal,
      type === 'realtime' ? signal.notificationType || null : null
    );

    const signalId = getSignalId(normalized);

    if (this.isKnownSignal(normalized)) {
      logger.debug(
        {
          signalId,
          symbol: normalized.symbol,
          root_tf: normalized.root_tf,
          type,
          notificationType: normalized.notificationType
        },
        'NotificationQueue: duplicate signal, skipping'
      );

      return false;
    }

    this.reserveSignal(normalized);

    this.queue.push({
      type,
      signal: normalized,
      signalId,
      timestamp: Date.now()
    });

    logger.debug(
      {
        signalId,
        type,
        notificationType: normalized.notificationType,
        queueLength: this.queue.length
      },
      'NotificationQueue: signal enqueued'
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
      logger.info('NotificationQueue: startup summary already in progress, skipping duplicate batch');
      return false;
    }

    return this.enqueueSummaryBatch({
      signals,
      batchType: 'startup_batch',
      fallbackType: 'startup',
      summaryState: QUEUE_STATE.STARTUP_SUMMARY,
      summaryFlag: 'startupSummaryInProgress',
      logLabel: 'startup'
    });
  }

  enqueueRootCandleBatch(signals) {
    if (!Array.isArray(signals)) {
      logger.warn('NotificationQueue: invalid root candle batch');
      return false;
    }

    if (this.rootCandleSummaryInProgress) {
      logger.info('NotificationQueue: root candle summary already in progress, skipping duplicate batch');
      return false;
    }

    return this.enqueueSummaryBatch({
      signals,
      batchType: 'root_candle_batch',
      fallbackType: 'new_root_candle',
      summaryState: QUEUE_STATE.ROOT_CANDLE_SUMMARY,
      summaryFlag: 'rootCandleSummaryInProgress',
      logLabel: 'root_candle'
    });
  }

  enqueueSummaryBatch({
    signals,
    batchType,
    fallbackType,
    summaryState,
    summaryFlag,
    logLabel
  }) {
    const uniqueSignals = [];
    const reservedIds = new Set();

    for (const signal of signals) {
      const normalized = this.normalizeSignal(signal, fallbackType);

      if (!normalized) {
        continue;
      }

      const signalId = getSignalId(normalized);

      if (
        reservedIds.has(signalId) ||
        this.isKnownSignal(normalized)
      ) {
        logger.debug(
          {
            signalId,
            notificationType: normalized.notificationType
          },
          `NotificationQueue: filtering duplicate ${logLabel} signal`
        );

        continue;
      }

      reservedIds.add(signalId);
      this.reserveSignal(normalized);

      uniqueSignals.push(normalized);
    }

    if (uniqueSignals.length === 0) {
      logger.info(
        {
          batchType
        },
        `NotificationQueue: ${logLabel} batch contained no new signals`
      );

      return false;
    }

    this[summaryFlag] = true;
    this.state = summaryState;

    this.queue.push({
      type: batchType,
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
      `NotificationQueue: ${logLabel} batch enqueued`
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
          } else if (item.type === 'root_candle_batch') {
            await this._processRootCandleBatch(item.signals);

            for (const signalId of item.signalIds || []) {
              this.markSignalSent(signalId);
            }
          } else if (item.type === 'realtime') {
            await this._processRealtimeSignal(item.signal);
            this.markSignalSent(item.signalId);
          } else if (item.type === 'midcandle_update') {
            await this._processMidCandleUpdate(item.signal);
            this.markSignalSent(item.signalId);
          } else if (item.type === 'mtf_alignment') {
            await this._processMtfAlignment(item.signal);
            this.markSignalSent(item.signalId);
          } else if (item.type === 'candle_update') {
            await this._processCandleUpdate(item.signal);
            this.markSignalSent(item.signalId);
          }
        } catch (err) {
          if (item.type === 'startup_batch' || item.type === 'root_candle_batch') {
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

      logger.info('NotificationQueue: queue processing completed');
    } catch (err) {
      logger.error(
        { err },
        'NotificationQueue: fatal processing error'
      );
    } finally {
      this.processing = false;
      this.startupSummaryInProgress = false;
      this.rootCandleSummaryInProgress = false;
      this.state = QUEUE_STATE.IDLE;

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

    logger.info('NotificationQueue: startup batch flow completed');
  }

  async _processRootCandleBatch(signals) {
    const telegram = require('./telegram');

    logger.info(
      {
        count: signals.length
      },
      'NotificationQueue: starting root candle batch flow'
    );

    await telegram.sendRootCandleSummary({
      snapshot: signals
    });

    logger.info('NotificationQueue: root candle batch flow completed');
  }

  async _processRealtimeSignal(signal) {
    const telegram = require('./telegram');

    logger.debug(
      {
        symbol: signal.symbol,
        root_tf: signal.root_tf,
        notificationType: signal.notificationType
      },
      'NotificationQueue: sending realtime signal block'
    );

    await telegram.sendNewSignalSingleBlock(signal);

    logger.info(
      {
        symbol: signal.symbol,
        root_tf: signal.root_tf,
        notificationType: signal.notificationType
      },
      'NotificationQueue: realtime signal block sent'
    );
  }

  async _processMidCandleUpdate(signal) {
    const telegram = require('./telegram');

    logger.debug(
      {
        symbol: signal.symbol,
        root_tf: signal.root_tf,
        notificationType: signal.notificationType
      },
      'NotificationQueue: sending mid-candle update block'
    );

    await telegram.sendMidCandleUpdateBlock(signal);
  }

  async _processMtfAlignment(signal) {
    const telegram = require('./telegram');

    logger.debug(
      {
        symbol: signal.symbol,
        root_tf: signal.root_tf,
        notificationType: signal.notificationType
      },
      'NotificationQueue: sending MTF alignment alert'
    );

    await telegram.sendMtfAlignmentAlert(signal);
  }

  async _processCandleUpdate(signal) {
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
      rootCandleInProgress: this.rootCandleSummaryInProgress,
      sentSignalCount: this.sentSignalIds.size,
      pendingSignalCount: this.pendingSignalIds.size
    };
  }

  resetSentSignals() {
    this.sentSignalIds.clear();
    this.pendingSignalIds.clear();

    logger.info('NotificationQueue: signal caches cleared');
  }
}

module.exports = new NotificationQueue();
