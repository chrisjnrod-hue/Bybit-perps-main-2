// telegram.js (FULLY UPDATED WITH SIGNAL VALIDATION)
const TelegramBot = require('node-telegram-bot-api');
const config = require('../config');
const logger = require('pino')();
const dbModule = require('../db');

let bot = null;

const SUMMARY_TITLE_MAP = {
  startup: '📊 Startup Summary',
  new_root_candle: '🕔 New Root Candle Open',
  new_root_candle_open: '🕔 New Root Candle Open',
  root_tf_candle_open: '🕔 Root TF Candle Open',
  newroottfcandleopen: '🕔 New Root Candle Open',
  mtf_alignment: '⏱️ MTF Alignment Alert',
  midcandle_update: '⏳ Mid-Candle Update'
};

function getNotificationType(signal) {
  if (!signal) {
    return null;
  }

  const type =
    signal.notificationType ||
    signal.signalType;

  if (
    typeof type === 'string' &&
    type.trim()
  ) {
    return type.trim();
  }

  return null;
}

function normalizeNotificationType(signal) {
  const type = getNotificationType(signal);

  if (!type) {
    return null;
  }

  const normalized = String(type).trim().toLowerCase();

  const aliases = {
    'new_root_candle_open': 'new_root_candle',
    'root_tf_candle_open': 'new_root_candle',
    'newroottfcandleopen': 'new_root_candle'
  };

  return aliases[normalized] || normalized;
}

module.exports = {
  init() {
    if (!config.TELEGRAM_BOT_TOKEN) {
      logger.warn(
        'Telegram token not configured; telegram disabled'
      );
      return;
    }

    if (!config.TELEGRAM_CHAT_ID) {
      logger.warn(
        'Telegram chat ID not configured; telegram disabled'
      );
      return;
    }

    if (!bot) {
      bot = new TelegramBot(
        config.TELEGRAM_BOT_TOKEN,
        {
          polling: false
        }
      );

      logger.info(
        'Telegram bot initialized'
      );
    }
  },

  ensureInitialized() {
    if (!bot) {
      this.init();
    }

    if (!bot) {
      throw new Error(
        'Telegram bot is not initialized or Telegram is disabled'
      );
    }

    return bot;
  },

  getLabel(
    index,
    { lowercase = true } = {}
  ) {
    if (
      typeof index !== 'number' ||
      index < 0
    ) {
      return '';
    }

    let i = index + 1;
    const chars = [];

    while (i > 0) {
      i -= 1;

      chars.unshift(
        String.fromCharCode(
          (i % 26) + 65
        )
      );

      i = Math.floor(i / 26);
    }

    const label = chars.join('');

    return lowercase
      ? label.toLowerCase()
      : label;
  },

  _sleep(ms) {
    return new Promise((resolve) => {
      setTimeout(resolve, ms || 0);
    });
  },

  async _sendMessage(
    text,
    options = {}
  ) {
    this.ensureInitialized();

    const delayMs = Math.max(
      500,
      Number(
        config.TELEGRAM_SEND_DELAY_MS
      ) || 1000
    );

    try {
      return await bot.sendMessage(
        config.TELEGRAM_CHAT_ID,
        text,
        options
      );
    } catch (err) {
      const retryAfter =
        err?.response?.body?.parameters?.retry_after ??
        err?.response?.parameters?.retry_after ??
        0;

      const errorMessage =
        String(
          err?.message || ''
        ).toLowerCase();

      const isRateLimit =
        err?.response?.statusCode === 429 ||
        retryAfter > 0 ||
        errorMessage.includes('429') ||
        errorMessage.includes(
          'too many requests'
        );

      if (!isRateLimit) {
        throw err;
      }

      const retryMs = Math.max(
        delayMs,
        Number(retryAfter) > 0
          ? Number(retryAfter) * 1000
          : delayMs
      );

      logger.warn(
        {
          retryMs,
          retryAfter,
          symbol: options?.symbol || null,
          root_tf: options?.root_tf || null,
          notificationType:
            options?.notificationType || null
        },
        'Telegram: rate limited; backing off before retry'
      );

      await this._sleep(retryMs);

      return await bot.sendMessage(
        config.TELEGRAM_CHAT_ID,
        text,
        options
      );
    }
  },

  buildAlignmentLines(alignment) {
    const lines = [];
    let positiveCount = 0;
    let total = 0;

    for (
      const tf of Object.keys(alignment || {})
    ) {
      const info = alignment[tf];

      total += 1;

      const ok =
        info &&
        typeof info.histogram !== 'undefined';

      const positiveSymbol = ok
        ? (
            info.positive
              ? '🟢'
              : '🔴'
          )
        : '⚪';

      if (
        info &&
        info.positive
      ) {
        positiveCount += 1;
      }

      const histogram =
        ok
          ? `hist=${Number(
              info.histogram
            ).toFixed(6)}`
          : '';

      const rising =
        ok
          ? (
              info.rising
                ? '↑'
                : '↓'
            )
          : '';

      lines.push(
        `${tf}: ${positiveSymbol} ${
          ok
            ? (
                info.positive
                  ? 'POS'
                  : 'NEG'
              )
            : 'unknown'
        } ${histogram} ${rising}`.trim()
      );
    }

    const mtfScore =
      total > 0
        ? positiveCount / total
        : 0;

    return {
      lines: lines.join('\n'),
      mtfScore,
      positiveCount,
      total
    };
  },

  formatMarketData(md = {}) {
    const price =
      typeof md.price === 'number'
        ? md.price
        : md.price
          ? Number(md.price)
          : null;

    const volume24 =
      typeof md.volume_24h_usdt === 'number'
        ? md.volume_24h_usdt
        : md.volume_24h_usdt
          ? Number(md.volume_24h_usdt)
          : null;

    const volumeChange =
      typeof md.volume_change_pct === 'number'
        ? md.volume_change_pct
        : md.volume_change_pct
          ? Number(md.volume_change_pct)
          : null;

    const marketCap =
      typeof md.market_cap === 'number'
        ? md.market_cap
        : md.market_cap
          ? Number(md.market_cap)
          : null;

    const lines = [
      `💰 Price: ${
        price !== null && price > 0
          ? '$' + price.toLocaleString(
              'en-US',
              {
                maximumFractionDigits: 8
              }
            )
          : '0'
      }`,
      `💵 24h Volume: ${
        volume24 !== null && volume24 > 0
          ? '$' + volume24.toLocaleString(
              'en-US',
              {
                maximumFractionDigits: 2
              }
            ) + ' USDT'
          : '0 USDT'
      }`,
      `📈 Volume Change: ${
        volumeChange !== null
          ? volumeChange.toFixed(2) + '%'
          : 'n/a'
      }`,
      `💎 Market Cap: ${
        marketCap && marketCap > 0
          ? '$' + marketCap.toLocaleString(
              'en-US',
              {
                maximumFractionDigits: 0
              }
            )
          : 'n/a'
      }`
    ];

    return lines.join('\n');
  },

  /**
   * ENHANCED: Validate signal structure before building message
   */
  buildSignalMessage(signal) {
    // CRITICAL FIX: Validate signal shape
    if (!signal || !signal.symbol || !signal.root_tf) {
      logger.error(
        { signal },
        'Telegram: ⚠️  CRITICAL - signal missing required fields (symbol, root_tf); cannot build message'
      );
      throw new Error('Incomplete signal object: missing symbol or root_tf');
    }

    const {
      symbol,
      root_tf,
      detected_at,
      meta = {}
    } = signal;

    const timeStr =
      detected_at
        ? new Date(
            detected_at
          ).toISOString()
        : new Date().toISOString();

    const alignment =
      meta.alignment || {};

    const tvScore =
      typeof meta.tvScore === 'number'
        ? meta.tvScore
        : (
            meta.tvScore
              ? Number(meta.tvScore)
              : 0
          );

    const tvSource =
      meta.tvSource || 'error';

    const mtfScore =
      typeof meta.mtfScore === 'number'
        ? meta.mtfScore
        : null;

    const decision =
      meta.decision || 'monitor';

    const reason =
      meta.acceptReason ||
      meta.reason ||
      'n/a';

    const {
      lines: alignmentLines,
      mtfScore: computedMtfScore
    } = this.buildAlignmentLines(
      alignment
    );

    const usedMtfScore =
      mtfScore !== null
        ? mtfScore
        : computedMtfScore;

    const tvPercent =
      Math.round(
        (tvScore || 0) * 100
      );

    const mtfPercent =
      Math.round(
        (usedMtfScore || 0) * 100
      );

    const scoringLine =
      `📊 Scoring:\nTV: ${tvPercent}% (${tvSource}) • MTF: ${mtfPercent}%`;

    const mtfHeader =
      '🛰️ MTF Status:';

    const marketBlock =
      `💱 Market Data:\n${
        this.formatMarketData(
          meta.marketData || {}
        )
      }`;

    const notificationType =
      normalizeNotificationType(signal);

    const eventTitle =
      notificationType
        ? (
            SUMMARY_TITLE_MAP[
              notificationType
            ] || null
          )
        : null;

    const msgParts = [
      ...(eventTitle
        ? [
            eventTitle,
            ''
          ]
        : []),
      `🎯 Signal: ${symbol} (${root_tf})`,
      `⏰ Time: ${timeStr}`,
      `${
        decision === 'accept'
          ? '✅ Decision'
          : '⚠️ Decision'
      }: ${decision} (reason: ${reason})`,
      '',
      scoringLine,
      '',
      mtfHeader,
      alignmentLines || 'No MTF data',
      '',
      marketBlock
    ];

    return msgParts.join('\n');
  },

  async sendNewSignalSingleBlock(
    signal,
    forcedType = null
  ) {
    const normalizedSignal = signal
      ? {
          ...signal,
          notificationType:
            forcedType ||
            signal.notificationType ||
            signal.signalType ||
            null
        }
      : null;

    if (!normalizedSignal) {
      throw new Error(
        'Telegram: cannot send an empty signal'
      );
    }

    // ENHANCED: Validate before sending
    if (!normalizedSignal.symbol || !normalizedSignal.root_tf) {
      logger.error(
        { normalizedSignal },
        'Telegram
