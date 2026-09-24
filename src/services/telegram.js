const TelegramBot = require('node-telegram-bot-api');
const config = require('../config');
const logger = require('pino')();
const dbModule = require('../db');

let bot = null;

const SUMMARY_TITLE_MAP = {
  startup: '📊 Startup Summary',
  new_root_candle: '🕔 New Root Candle Open',
  mtf_alignment: '⏱️ MTF Alignment Alert',
  midcandle_update: '⏳ Mid-Candle Update'
};

module.exports = {
  init() {
    if (!config.TELEGRAM_BOT_TOKEN) {
      logger.warn('Telegram token not configured; telegram disabled');
      return;
    }

    if (!bot) {
      bot = new TelegramBot(config.TELEGRAM_BOT_TOKEN, { polling: false });
    }
  },

  getLabel(index, { lowercase = true } = {}) {
    if (typeof index !== 'number' || index < 0) return '';

    let i = index + 1;
    const chars = [];

    while (i > 0) {
      i -= 1;
      chars.unshift(String.fromCharCode((i % 26) + 65));
      i = Math.floor(i / 26);
    }

    const label = chars.join('');

    return lowercase ? label.toLowerCase() : label;
  },

  _sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms || 0));
  },

  async _sendMessage(text, options = {}) {
    if (!bot) return null;

    const delayMs = Math.max(
      500,
      Number(config.TELEGRAM_SEND_DELAY_MS) || 1000
    );

    try {
      return await bot.sendMessage(config.TELEGRAM_CHAT_ID, text, options);
    } catch (err) {
      const retryAfter =
        err?.response?.body?.parameters?.retry_after ??
        err?.response?.parameters?.retry_after ??
        0;

      const isRateLimit =
        err?.response?.statusCode === 429 ||
        retryAfter > 0 ||
        String(err?.message || '').toLowerCase().includes('429') ||
        String(err?.message || '').toLowerCase().includes('too many requests');

      if (isRateLimit) {
        const retryMs = Math.max(
          delayMs,
          Number(retryAfter) > 0 ? Number(retryAfter) * 1000 : delayMs
        );

        logger.warn(
          { retryMs, retryAfter, symbol: options?.symbol || null },
          'Telegram: rate limited; backing off before retry'
        );

        await this._sleep(retryMs);

        return bot.sendMessage(config.TELEGRAM_CHAT_ID, text, options);
      }

      throw err;
    }
  },

  buildAlignmentLines(alignment) {
    const lines = [];
    let positiveCount = 0;
    let total = 0;

    for (const tf of Object.keys(alignment || {})) {
      const info = alignment[tf];
      total++;

      const ok = info && typeof info.histogram !== 'undefined';
      const posSym = ok ? (info.positive ? '🟢' : '🔴') : '⚪';
      if (info && info.positive) positiveCount++;

      const hist = ok ? `hist=${Number(info.histogram).toFixed(6)}` : '';
      const rise = ok ? (info.rising ? '↑' : '↓') : '';

      lines.push(
        `${tf}: ${posSym} ${ok ? (info.positive ? 'POS' : 'NEG') : 'unknown'} ${hist} ${rise}`.trim()
      );
    }

    const mtfScore = total ? (positiveCount / total) : 0;

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

    const vol24 =
      typeof md.volume_24h_usdt === 'number'
        ? md.volume_24h_usdt
        : md.volume_24h_usdt
          ? Number(md.volume_24h_usdt)
          : null;

    const volChange =
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
      `💰 Price: ${price !== null && price > 0 ? '$' + price.toLocaleString('en-US', { maximumFractionDigits: 8 }) : '0'}`,
      `💵 24h Volume: ${vol24 !== null && vol24 > 0 ? '$' + vol24.toLocaleString('en-US', { maximumFractionDigits: 2 }) + ' USDT' : '0 USDT'}`,
      `📈 Volume Change: ${volChange !== null ? volChange.toFixed(2) + '%' : 'n/a'}`,
      `💎 Market Cap: ${marketCap && marketCap > 0 ? '$' + marketCap.toLocaleString('en-US', { maximumFractionDigits: 0 }) : 'n/a'}`
    ];

    return lines.join('\n');
  },

  buildSignalMessage(signal) {
    const { symbol, root_tf, detected_at, meta = {} } = signal || {};
    const timeStr = detected_at
      ? new Date(detected_at).toISOString()
      : new Date().toISOString();

    const alignment = meta.alignment || {};
    const tvScore =
      typeof meta.tvScore === 'number'
        ? meta.tvScore
        : (meta.tvScore ? Number(meta.tvScore) : 0);

    const tvSource = meta.tvSource || 'error';
    const mtfScore =
      typeof meta.mtfScore === 'number'
        ? meta.mtfScore
        : null;

    const decision = meta.decision || 'monitor';
    const reason = meta.acceptReason || meta.reason || 'n/a';

    const {
      lines: alignmentLines,
      mtfScore: computedMtfScore
    } = this.buildAlignmentLines(alignment);

    const usedMtfScore =
      mtfScore !== null ? mtfScore : computedMtfScore;

    const tvPercent = Math.round((tvScore || 0) * 100);
    const mtfPercent = Math.round((usedMtfScore || 0) * 100);

    const scoringLine = `📊 Scoring:\nTV: ${tvPercent}% (${tvSource}) • MTF: ${mtfPercent}%`;
    const mtfHeader = '🛰️ MTF Status:';
    const marketBlock = `💱 Market Data:\n${this.formatMarketData(meta.marketData || {})}`;

    const eventTitle =
      signal && signal.notificationType
        ? SUMMARY_TITLE_MAP[signal.notificationType] || null
        : null;

    const msgParts = [
      ...(eventTitle ? [eventTitle, ''] : []),
      `🎯 Signal: ${symbol} (${root_tf})`,
      `⏰ Time: ${timeStr}`,
      `${decision === 'accept' ? '✅ Decision' : '⚠️ Decision'}: ${decision} (reason: ${reason})`,
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

  async sendNewSignalSingleBlock(signal, forcedType = null) {
    if (!bot) return;

    try {
      const normalizedSignal = signal
        ? {
            ...signal,
            notificationType:
              forcedType ||
              signal.notificationType ||
              null
          }
        : null;

      const baseMsg = this.buildSignalMessage(normalizedSignal);

      await this._sendMessage(baseMsg, {
        symbol: normalizedSignal?.symbol,
        root_tf: normalizedSignal?.root_tf,
        notificationType: normalizedSignal?.notificationType || null
      });

      logger.debug(
        {
          symbol: normalizedSignal?.symbol,
          root_tf: normalizedSignal?.root_tf,
          notificationType: normalizedSignal?.notificationType || null
        },
        'Telegram: signal detail block sent'
      );
    } catch (err) {
      logger.warn(
        {
          err,
          symbol: signal?.symbol,
          root_tf: signal?.root_tf
        },
        'Telegram: failed to send signal detail block'
      );
    }
  },

  async sendMidCandleUpdateBlock(signal) {
    if (!signal) {
      return;
    }

    const title = SUMMARY_TITLE_MAP.midcandle_update || '⏳ Mid-Candle Update';

    const msg = [
      title,
      '',
      this.buildSignalMessage({
        ...signal,
        notificationType: 'midcandle_update'
      })
    ].join('\n');

    await this._sendMessage(msg, {
      symbol: signal.symbol,
      root_tf: signal.root_tf,
      notificationType: 'midcandle_update'
    });
  },

  async sendMtfAlignmentAlert(signal) {
    if (!signal) {
      return;
    }

    const title = SUMMARY_TITLE_MAP.mtf_alignment || '⏱️ MTF Alignment Alert';

    const msg = [
      title,
      '',
      this.buildSignalMessage({
        ...signal,
        notificationType: 'mtf_alignment'
      })
    ].join('\n');

    await this._sendMessage(msg, {
      symbol: signal.symbol,
      root_tf: signal.root_tf,
      notificationType: 'mtf_alignment'
    });
  },

  async sendSummaryBlock({
    snapshot = [],
    title = '📊 Startup Summary',
    signalType = null
  } = {}) {
    if (!bot) return;

    try {
      const signals = Array.isArray(snapshot) ? snapshot : [];

      if (signals.length === 0) {
        logger.warn('Telegram: no signals provided to summary block');
        return;
      }

      const delayMs = Math.max(
        500,
        Number(config.TELEGRAM_SEND_DELAY_MS) || 1000
      );

      const tfCounts = {};
      const symbolSet = new Set();

      for (const s of signals) {
        const tf = String(s.root_tf || 'unknown');
        tfCounts[tf] = (tfCounts[tf] || 0) + 1;
        if (s.symbol) symbolSet.add(s.symbol);
      }

      const orderedRootTfs = Array.isArray(config.ROOT_TFS) && config.ROOT_TFS.length
        ? config.ROOT_TFS.map(String)
        : Object.keys(tfCounts);

      for (const tf of Object.keys(tfCounts)) {
        if (!orderedRootTfs.includes(tf)) orderedRootTfs.push(tf);
      }

      const summaryParts = orderedRootTfs.map((tf) => `${tf}: ${tfCounts[tf] || 0}`);
      const allSymbols = Array.from(symbolSet).sort((a, b) =>
        a.localeCompare(b, undefined, { sensitivity: 'base' })
      );

      const symbolLines = allSymbols.length ? allSymbols.join('\n') : 'n/a';
      const header = `${title} (${signals.length} signals):\n${summaryParts.join(' • ')}\n\n${symbolLines}`;

      await this._sendMessage(header);
      logger.info('Telegram: summary header sent');
      await this._sleep(delayMs);

      const sortedSignals = [...signals].sort((a, b) => {
        const s = (a.symbol || '').localeCompare(b.symbol || '', undefined, { sensitivity: 'base' });
        if (s !== 0) return s;
        return String(a.root_tf || '').localeCompare(String(b.root_tf || ''), undefined, { numeric: true });
      });

      for (let i = 0; i < sortedSignals.length; i++) {
        try {
          await this.sendNewSignalSingleBlock(sortedSignals[i], signalType);
          logger.debug(
            {
              symbol: sortedSignals[i].symbol,
              index: i + 1,
              total: sortedSignals.length
            },
            'Telegram: signal block sent'
          );
        } catch (e) {
          logger.warn(
            { err: e, symbol: sortedSignals[i].symbol },
            'Telegram: failed to send signal block'
          );
        }

        await this._sleep(delayMs);
      }

      let openCount = 0;
      try {
        const row = dbModule.get().prepare("SELECT COUNT(*) as cnt FROM trades WHERE status = 'open'").get();
        openCount = row ? Number(row.cnt || 0) : 0;
      } catch (e) {
        logger.debug({ e }, 'Telegram: failed to read open trades count');
        openCount = 0;
      }

      const maxOpenTrades = Number(config.MAX_OPEN_TRADES) || 0;
      const maxSlots = Math.max(0, maxOpenTrades - openCount);
      const recHeader = `📈 Recommended to Open (${maxSlots} slots available):`;

      await this._sendMessage(recHeader);
      await this._sleep(delayMs);

      const candidates = signals
        .map((s) => ({
          symbol: s.symbol,
          root_tf: s.root_tf,
          tvScore: Number(s.meta?.tvScore || 0),
          mtfScore: Number(s.meta?.mtfScore || 0),
          acceptDecision: s.meta?.decision || 'monitor',
          reason: s.meta?.acceptReason || 'n/a'
        }))
        .filter((c) => c.acceptDecision === 'accept')
        .sort((a, b) => {
          if (b.tvScore !== a.tvScore) return b.tvScore - a.tvScore;
          return b.mtfScore - a.mtfScore;
        });

      const recommended = candidates.slice(0, Math.max(0, maxSlots));

      if (recommended.length === 0) {
        await this._sendMessage('No recommended signals (all rejections or filtered)');
      } else {
        for (let i = 0; i < recommended.length; i++) {
          const r = recommended[i];
          const label = this.getLabel(i, { lowercase: true });
          const tvPercent = Math.round((r.tvScore || 0) * 100);
          const mtfPercent = Math.round((r.mtfScore || 0) * 100);
          const simNote = config.OPENTRADE ? '' : ' [SIMULATED]';
          const line = `${label}) ${r.symbol} ${r.root_tf} - TV:${tvPercent}% MTF:${mtfPercent}% - ${r.reason}${simNote}`;

          await this._sendMessage(line);

          logger.debug(
            {
              symbol: r.symbol,
              index: i + 1,
              total: recommended.length
            },
            'Telegram: recommended block sent'
          );

          await this._sleep(delayMs);
        }
      }

      logger.info('Telegram: summary flow completed');
    } catch (err) {
      logger.error({ err }, 'Telegram: summary flow failed');
    }
  },

  async sendStartupSummary({ snapshot = [] } = {}) {
    if (!bot) return;

    await this.sendSummaryBlock({
      snapshot,
      title: '📊 Startup Summary',
      signalType: null
    });
  },

  async sendRootCandleSummary({ snapshot = [] } = {}) {
    if (!bot) return;

    await this.sendSummaryBlock({
      snapshot,
      title: '🕔 New Root Candle Open',
      signalType: 'new_root_candle'
    });
  }
};
