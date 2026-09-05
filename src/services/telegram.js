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
    bot = new TelegramBot(config.TELEGRAM_BOT_TOKEN, { polling: false });
  },

  // Alphabetic label generator: 0 -> 'a', 25 -> 'z', 26 -> 'aa', 27 -> 'ab', ...
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

  // small helper sleep
  _sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms || 0));
  },

  // Low-level builder for alignment lines and mtf score
  buildAlignmentLines(alignment) {
    const lines = [];
    let positiveCount = 0;
    let total = 0;
    for (const [tf, info] of Object.entries(alignment || {})) {
      total++;
      const ok = info && typeof info.histogram !== 'undefined';
      const pos = ok ? (info.positive ? 'POS' : 'NEG') : 'unknown';
      if (info && info.positive) positiveCount++;
      const hist = ok ? `hist=${Number(info.histogram).toFixed(6)}` : '';
      const rise = ok ? (info.rising ? '↑' : '↓') : '';
      lines.push(`${tf}: ${pos} ${hist} ${rise}`.trim());
    }
    const mtfScore = total ? (positiveCount / total) : 0;
    return { lines: lines.join('\n'), mtfScore, positiveCount, total };
  },

  // Sends a single-block message for a newly discovered signal (scan boundary)
  // label (optional) - a, b, c, ... will be prefixed if provided
  async sendNewSignalSingleBlock(signal, label = null) {
    if (!bot) return;
    try {
      const { symbol, root_tf, detected_at, meta } = signal;
      const timeStr = new Date(detected_at).toISOString();
      const alignment = meta.alignment || {};
      const { lines, mtfScore } = this.buildAlignmentLines(alignment);

      const tvScore = (meta && typeof meta.tvScore === 'number') ? meta.tvScore : 0;
      const decision = meta.decision || 'monitor';
      const reason = meta.acceptReason || meta.decision || 'n/a';
      const price = meta.marketData && meta.marketData.price ? Number(meta.marketData.price) : null;

      const labelPrefix = label ? `${label}) ` : '';

      const msg = [
        `${labelPrefix}New signal: ${symbol} (${root_tf})`,
        `Time: ${timeStr}`,
        `Decision: ${decision} (reason: ${reason})`,
        '',
        `TV score: ${(tvScore * 100).toFixed(0)}% • MTF score: ${(mtfScore * 100).toFixed(0)}%`,
        '',
        `MTF status:\n${lines}`,
        '',
        `Price: ${price ? price.toLocaleString('en-US', { maximumFractionDigits: 8 }) : 'n/a'}`,
      ].join('\n');

      await bot.sendMessage(config.TELEGRAM_CHAT_ID, msg);
      logger.info({ symbol, root_tf, label }, 'Telegram new-signal message sent');
    } catch (err) {
      logger.warn({ err }, 'Failed to send telegram new-signal block');
    }
  },

  // Similar to earlier sendRootSignalBlock but accepts tvScore and mtfScore
  async sendRootSignalBlock({ symbol, root_tf, alignment, detected_at, accept, marketData, tvScore = 0, mtfScore = 0 }) {
    if (!bot) return;
    const timeStr = new Date(detected_at).toISOString();
    let alignmentLines = Object.entries(alignment).map(([tf, info]) => {
      if (!info || !info.hasOwnProperty('histogram')) return `${tf}: unknown`;
      return `${tf}: ${info.positive ? 'POS' : 'NEG'} hist=${Number(info.histogram).toFixed(6)} ${info.rising ? '↑' : '↓'}`;
    }).join('\n');

    const decision = accept && accept.decision ? accept.decision : 'monitor';

    const price = marketData?.price ? Number(marketData.price) : null;
    const vol24 = marketData?.volume_24h_usdt ? Number(marketData.volume_24h_usdt) : null;
    const prevVol = marketData?.prev_volume_24h ? Number(marketData.prev_volume_24h) : null;
    const volChange = (typeof marketData?.volume_change_pct === 'number') ? Number(marketData.volume_change_pct) : null;
    const marketCap = marketData?.market_cap ? Number(marketData.market_cap) : null;

    const marketLines = [
      `Price: ${price ? price.toLocaleString('en-US', { maximumFractionDigits: 8 }) : 'n/a'}`,
      `24h USDT Volume: ${vol24 ? vol24.toLocaleString('en-US', { maximumFractionDigits: 2 }) : 'n/a'}`,
      `Prev 24h USDT Volume: ${prevVol ? prevVol.toLocaleString('en-US', { maximumFractionDigits: 2 }) : 'n/a'}`,
      `24h Volume Change: ${volChange !== null ? volChange.toFixed(2) + '%' : 'n/a'}`,
      `Market Cap: ${marketCap ? '$' + marketCap.toLocaleString('en-US', { maximumFractionDigits: 0 }) : 'n/a'}`
    ].join('\n');

    const msg = `Root signal: ${symbol} (${root_tf})\nTime: ${timeStr}\nDecision: ${decision}\n\nTV score: ${(tvScore * 100).toFixed(0)}% • MTF score: ${(mtfScore * 100).toFixed(0)}%\n\nMTF status:\n${alignmentLines}\n\nMarket data:\n${marketLines}\n\nAccept reason: ${accept?.reason || 'n/a'}`;

    try {
      await bot.sendMessage(config.TELEGRAM_CHAT_ID, msg);
      logger.info({ symbol, root_tf }, 'Telegram message sent with market data');
    } catch (err) {
      logger.warn({ err }, 'Failed to send telegram');
    }
  },

  /**
   * sendStartupSummary:
   * - snapshot: array of latest signals (db.getLatestSignalsSnapshot())
   *
   * Behavior changes:
   * - Sends a header "Startup root TF summary (N):"
   * - Then sends each signal as its own labeled block (a), b), ...), using sendNewSignalSingleBlock
   * - Then sends "Recommended to open (slots: X):" header and each recommended as its own labeled block
   */
  async sendStartupSummary({ snapshot = [] } = {}) {
    if (!bot) return;
    try {
      const db = dbModule.get();

      // Flatten snapshot to an array of signal objects (snapshot is expected to already be that)
      const signals = Array.isArray(snapshot) ? snapshot.slice() : [];

      // Sort signals for stable ordering: symbol asc, root_tf asc
      signals.sort((a, b) => {
        const s = (a.symbol || '').localeCompare(b.symbol || '', undefined, { sensitivity: 'base' });
        if (s !== 0) return s;
        return String(a.root_tf || '').localeCompare(String(b.root_tf || ''), undefined, { numeric: true });
      });

      const count = signals.length;

      const header = `Startup root TF summary (${count}):`;
      await bot.sendMessage(config.TELEGRAM_CHAT_ID, header);

      // Send each signal as its own message with alphabetic label (a, b, ...)
      for (let i = 0; i < signals.length; i++) {
        const label = this.getLabel(i, { lowercase: true });
        try {
          await this.sendNewSignalSingleBlock(signals[i], label);
        } catch (e) {
          logger.debug({ e, i }, 'sendStartupSummary: failed to send single signal block (continuing)');
        }
        // small delay between messages
        await this._sleep(config.TELEGRAM_SEND_DELAY_MS || 100);
      }

      // Second block: recommended signals to open trade/simulated open trades
      const openCountRow = db.prepare("SELECT COUNT(*) as cnt FROM trades WHERE status = 'open'").get();
      const openCount = openCountRow ? Number(openCountRow.cnt || 0) : 0;
      const maxSlots = Math.max(0, config.MAX_OPEN_TRADES - openCount);

      // Prepare candidate accepts sorted by tvScore desc then mtfScore
      const candidates = signals
        .map(s => ({
          symbol: s.symbol,
          root_tf: s.root_tf,
          tvScore: s.meta?.tvScore || 0,
          mtfScore: s.meta?.mtfScore || 0,
          acceptDecision: s.meta?.decision || 'monitor',
          reason: s.meta?.acceptReason || 'n/a',
          raw: s
        }))
        .filter(c => c.acceptDecision === 'accept')
        .sort((a, b) => {
          if (b.tvScore !== a.tvScore) return b.tvScore - a.tvScore;
          return b.mtfScore - a.mtfScore;
        });

      const recommended = candidates.slice(0, maxSlots);

      const recHeader = `Recommended to open (slots: ${maxSlots}):`;
      await bot.sendMessage(config.TELEGRAM_CHAT_ID, recHeader);

      if (recommended.length === 0) {
        await bot.sendMessage(config.TELEGRAM_CHAT_ID, 'No recommended signals');
      } else {
        // For recommended, restart labeling from 'a' again
        for (let i = 0; i < recommended.length; i++) {
          const label = this.getLabel(i, { lowercase: true });
          const r = recommended[i];
          // If we have the raw full signal, reuse sendNewSignalSingleBlock so formatting is consistent
          if (r.raw) {
            try {
              await this.sendNewSignalSingleBlock(r.raw, label);
            } catch (e) {
              // fallback to simple text if something goes wrong
              const line = `${label}) ${r.symbol} ${r.root_tf} - TV:${(r.tvScore * 100).toFixed(0)}% MTF:${(r.mtfScore * 100).toFixed(0)}% - ${r.reason}${config.OPENTRADE ? '' : ' (SIMULATED)'}`;
              await bot.sendMessage(config.TELEGRAM_CHAT_ID, line);
            }
          } else {
            const line = `${label}) ${r.symbol} ${r.root_tf} - TV:${(r.tvScore * 100).toFixed(0)}% MTF:${(r.mtfScore * 100).toFixed(0)}% - ${r.reason}${config.OPENTRADE ? '' : ' (SIMULATED)'}`;
            await bot.sendMessage(config.TELEGRAM_CHAT_ID, line);
          }
          await this._sleep(config.TELEGRAM_SEND_DELAY_MS || 100);
        }
      }

      logger.info('Startup telegram summary sent (detailed per-signal blocks)');
    } catch (err) {
      logger.warn({ err }, 'Failed to send startup telegram summary');
    }
  },

  /**
   * sendRootCandleUpdate:
   * - Called when new root candle(s) open. Sends the same style as startup but only for affected root TF signals.
   */
  async sendRootCandleUpdate({ snapshot = [], newRootTfs = [] } = {}) {
    if (!bot) return;
    try {
      // Filter snapshot to only those signals coming from root tfs (if newRootTfs provided)
      const filtered = Array.isArray(snapshot) && newRootTfs && newRootTfs.length
        ? snapshot.filter(s => newRootTfs.includes(String(s.root_tf)))
        : snapshot;
      await this.sendStartupSummary({ snapshot: filtered });
    } catch (err) {
      logger.warn({ err }, 'Failed to send root candle update');
    }
  }
};
