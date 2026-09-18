// src/services/telegram.js
const TelegramBot = require('node-telegram-bot-api');
const config = require('../config');
const logger = require('pino')();
const dbModule = require('../db');

let bot = null;

module.exports = {
  init() {
    if (!config.TELEGRAM_BOT_TOKEN) {
      logger.warn('Telegram token not configured; telegram disabled');
      return;
    }
    if (!bot) bot = new TelegramBot(config.TELEGRAM_BOT_TOKEN, { polling: false });
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
      lines.push(`${tf}: ${posSym} ${ok ? (info.positive ? 'POS' : 'NEG') : 'unknown'} ${hist} ${rise}`.trim());
    }

    const mtfScore = total ? (positiveCount / total) : 0;
    return { lines: lines.join('\n'), mtfScore, positiveCount, total };
  },

  formatMarketData(md = {}) {
    const price = (typeof md.price === 'number') ? md.price : (md.price ? Number(md.price) : null);
    const vol24 = (typeof md.volume_24h_usdt === 'number') ? md.volume_24h_usdt : (md.volume_24h_usdt ? Number(md.volume_24h_usdt) : null);
    const volChange = (typeof md.volume_change_pct === 'number') ? md.volume_change_pct : (md.volume_change_pct ? Number(md.volume_change_pct) : null);
    const marketCap = (typeof md.market_cap === 'number') ? md.market_cap : (md.market_cap ? Number(md.market_cap) : null);

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
    const timeStr = detected_at ? new Date(detected_at).toISOString() : new Date().toISOString();
    const alignment = meta.alignment || {};
    const tvScore = (typeof meta.tvScore === 'number') ? meta.tvScore : (meta.tvScore ? Number(meta.tvScore) : 0);
    const tvSource = meta.tvSource || 'error';
    const mtfScore = (typeof meta.mtfScore === 'number') ? meta.mtfScore : null;
    const decision = meta.decision || 'monitor';
    const reason = meta.acceptReason || meta.reason || 'n/a';

    const { lines: alignmentLines, mtfScore: computedMtfScore } = this.buildAlignmentLines(alignment);
    const usedMtfScore = (mtfScore !== null) ? mtfScore : computedMtfScore;

    const tvPercent = Math.round((tvScore || 0) * 100);
    const mtfPercent = Math.round((usedMtfScore || 0) * 100);
    const scoringLine = `📊 Scoring:\nTV: ${tvPercent}% (${tvSource}) • MTF: ${mtfPercent}%`;
    const mtfHeader = `🛰️ MTF Status:`;
    const marketBlock = `💱 Market Data:\n${this.formatMarketData(meta.marketData || {})}`;

    const msgParts = [
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

  async sendNewSignalSingleBlock(signal) {
    if (!bot) return;

    try {
      const baseMsg = this.buildSignalMessage(signal);

      logger.debug(
        {
          symbol: signal?.symbol,
          root_tf: signal?.root_tf,
          eventId: signal?.eventId
        },
        'Telegram: preparing to send single signal block'
      );

      await bot.sendMessage(config.TELEGRAM_CHAT_ID, baseMsg);

      logger.info(
        {
          symbol: signal?.symbol,
          root_tf: signal?.root_tf,
          eventId: signal?.eventId
        },
        'Telegram: signal detail block sent'
      );
    } catch (err) {
      logger.warn(
        { err, signal },
        'Telegram: failed to send signal detail block'
      );
    }
  },

  async sendStartupSummary({ snapshot = [] } = {}) {
    if (!bot) return;

    try {
      const signals = Array.isArray(snapshot) && snapshot.length > 0 ? snapshot : [];

      if (signals.length === 0) {
        logger.warn('Telegram: no signals provided to startup summary');
        return;
      }

      logger.info(
        { signalCount: signals.length },
        'Telegram: starting startup summary flow'
      );

      // STEP 1: Send startup header with summary counts
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

      const summaryParts = orderedRootTfs.map(tf => `${tf}: ${tfCounts[tf] || 0}`);
      const allSymbols = Array.from(symbolSet).sort((a, b) => a.localeCompare(b, undefined, { sensitivity: 'base' }));
      const symbolLines = allSymbols.length ? allSymbols.join('\n') : 'n/a';

      const header = `📊 Startup Summary (${signals.length} signals):\n${summaryParts.join(' • ')}\n\n${symbolLines}`;
      await bot.sendMessage(config.TELEGRAM_CHAT_ID, header);
      logger.info('Telegram: startup header sent');
      await this._sleep(config.TELEGRAM_SEND_DELAY_MS || 100);

      // STEP 2: Send individual signal detail blocks (one per signal, sorted A-Z)
      signals.sort((a, b) => {
        const s = (a.symbol || '').localeCompare(b.symbol || '', undefined, { sensitivity: 'base' });
        if (s !== 0) return s;
        return String(a.root_tf || '').localeCompare(String(b.root_tf || ''), undefined, { numeric: true });
      });

      logger.info(
        {
          totalSignals: signals.length,
          firstSignal: signals[0]?.symbol || null,
          lastSignal: signals[signals.length - 1]?.symbol || null
        },
        'Telegram: about to send per-signal blocks'
      );

      for (let i = 0; i < signals.length; i++) {
        const signal = signals[i];

        try {
          logger.debug(
            {
              index: i + 1,
              total: signals.length,
              symbol: signal?.symbol,
              root_tf: signal?.root_tf,
              eventId: signal?.eventId
            },
            'Telegram: sending signal block'
          );

          await this.sendNewSignalSingleBlock(signal);

          logger.info(
            {
              index: i + 1,
              total: signals.length,
              symbol: signal?.symbol,
              root_tf: signal?.root_tf,
              eventId: signal?.eventId
            },
            'Telegram: signal block completed'
          );
        } catch (e) {
          logger.warn(
            {
              err: e,
              index: i + 1,
              total: signals.length,
              symbol: signal?.symbol,
              root_tf: signal?.root_tf
            },
            'Telegram: failed to send signal block'
          );
        }

        await this._sleep(config.TELEGRAM_SEND_DELAY_MS || 100);
      }

      logger.info(
        { totalSignals: signals.length },
        'Telegram: completed per-signal block loop'
      );

      // STEP 3: Send recommended trades block
      let openCount = 0;
      try {
        const row = dbModule.get().prepare("SELECT COUNT(*) as cnt FROM trades WHERE status = 'open'").get();
        openCount = row ? Number(row.cnt || 0) : 0;
      } catch (e) {
        logger.debug({ e }, 'Telegram: failed to read open trades count');
        openCount = 0;
      }

      const maxSlots = Math.max(0, config.MAX_OPEN_TRADES - openCount);
      const recHeader = `📈 Recommended to Open (${maxSlots} slots available):`;
      await bot.sendMessage(config.TELEGRAM_CHAT_ID, recHeader);
      await this._sleep(config.TELEGRAM_SEND_DELAY_MS || 100);

      const candidates = signals
        .map(s => ({
          symbol: s.symbol,
          root_tf: s.root_tf,
          tvScore: s.meta?.tvScore || 0,
          mtfScore: s.meta?.mtfScore || 0,
          acceptDecision: s.meta?.decision || 'monitor',
          reason: s.meta?.acceptReason || 'n/a'
        }))
        .filter(c => c.acceptDecision === 'accept')
        .sort((a, b) => {
          if (b.tvScore !== a.tvScore) return b.tvScore - a.tvScore;
          return b.mtfScore - a.mtfScore;
        });

      const recommended = candidates.slice(0, maxSlots);

      if (recommended.length === 0) {
        await bot.sendMessage(config.TELEGRAM_CHAT_ID, 'No recommended signals (all rejections or filtered)');
      } else {
        for (let i = 0; i < recommended.length; i++) {
          const r = recommended[i];
          const label = this.getLabel(i, { lowercase: true });
          const tvPercent = Math.round((r.tvScore || 0) * 100);
          const mtfPercent = Math.round((r.mtfScore || 0) * 100);
          const simNote = config.OPENTRADE ? '' : ' [SIMULATED]';
          const line = `${label}) ${r.symbol} ${r.root_tf} - TV:${tvPercent}% MTF:${mtfPercent}% - ${r.reason}${simNote}`;
          await bot.sendMessage(config.TELEGRAM_CHAT_ID, line);
          logger.debug({ symbol: r.symbol, index: i + 1, total: recommended.length }, 'Telegram: recommended block sent');
          await this._sleep(config.TELEGRAM_SEND_DELAY_MS || 100);
        }
      }

      logger.info('Telegram: startup summary flow completed');
    } catch (err) {
      logger.error({ err }, 'Telegram: startup summary flow failed');
    }
  }
};
