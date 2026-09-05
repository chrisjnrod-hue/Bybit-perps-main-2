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

  // Alphabetic label generator: 0 -> 'a', 25 -> 'z', 26 -> 'aa', ...
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
      `💰 Price: ${price !== null ? price.toLocaleString('en-US', { maximumFractionDigits: 8 }) : 'n/a'}`,
      `💵 24h Volume: ${vol24 !== null ? vol24.toLocaleString('en-US', { maximumFractionDigits: 2 }) + ' USDT' : 'n/a'}`,
      `📈 Volume Change: ${volChange !== null ? volChange.toFixed(2) + '%' : 'n/a'}`,
      `💎 Market Cap: ${marketCap ? '$' + marketCap.toLocaleString('en-US', { maximumFractionDigits: 0 }) : 'n/a'}`
    ];
    return lines.join('\n');
  },

  buildSignalMessage(signal) {
    const { symbol, root_tf, detected_at, meta = {} } = signal || {};
    const timeStr = detected_at ? new Date(detected_at).toISOString() : new Date().toISOString();
    const alignment = meta.alignment || {};
    const tvScore = (typeof meta.tvScore === 'number') ? meta.tvScore : (meta.tvScore ? Number(meta.tvScore) : 0);
    const mtfScore = (typeof meta.mtfScore === 'number') ? meta.mtfScore : null;
    const decision = meta.decision || 'monitor';
    const reason = meta.acceptReason || meta.reason || 'n/a';

    const { lines: alignmentLines, mtfScore: computedMtfScore } = this.buildAlignmentLines(alignment);
    const usedMtfScore = (mtfScore !== null) ? mtfScore : computedMtfScore;

    const scoringLine = `📊 Scoring:\nTV: ${(tvScore * 100).toFixed(0)}% • MTF: ${(usedMtfScore * 100).toFixed(0)}%`;
    const mtfHeader = `🛰️ MTF Status:`;
    const marketBlock = `💱 Market Data:\n${this.formatMarketData(meta.marketData || {})}`;

    const msgParts = [
      `🎯 New signal: ${symbol} (${root_tf})`,
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

  // detail block: per-signal
  async sendNewSignalSingleBlock(signal) {
    if (!bot) return;
    try {
      const msg = this.buildSignalMessage(signal);
      await bot.sendMessage(config.TELEGRAM_CHAT_ID, msg);
      logger.info({ symbol: signal?.symbol, root_tf: signal?.root_tf }, 'Telegram new-signal message sent (detail block)');
    } catch (err) {
      logger.warn({ err }, 'Failed to send telegram new-signal block');
    }
  },

  // more detailed root block (kept for compatibility)
  async sendRootSignalBlock({ symbol, root_tf, alignment, detected_at, accept, marketData, tvScore = 0, mtfScore = 0 }) {
    if (!bot) return;
    const timeStr = new Date(detected_at).toISOString();
    let alignmentLines = Object.entries(alignment || {}).map(([tf, info]) => {
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
      `💰 Price: ${price ? price.toLocaleString('en-US', { maximumFractionDigits: 8 }) : 'n/a'}`,
      `💵 24h USDT Volume: ${vol24 ? vol24.toLocaleString('en-US', { maximumFractionDigits: 2 }) : 'n/a'}`,
      `Prev 24h USDT Volume: ${prevVol ? prevVol.toLocaleString('en-US', { maximumFractionDigits: 2 }) : 'n/a'}`,
      `📈 24h Volume Change: ${volChange !== null ? volChange.toFixed(2) + '%' : 'n/a'}`,
      `💎 Market Cap: ${marketCap ? '$' + marketCap.toLocaleString('en-US', { maximumFractionDigits: 0 }) : 'n/a'}`
    ].join('\n');

    const msg = [
      `🎯 Root signal: ${symbol} (${root_tf})`,
      `⏰ Time: ${timeStr}`,
      `${decision === 'accept' ? '✅ Decision' : '⚠️ Decision'}: ${decision}`,
      '',
      `📊 Scoring: TV: ${(tvScore * 100).toFixed(0)}% • MTF: ${(mtfScore * 100).toFixed(0)}%`,
      '',
      `🛰️ MTF status:\n${alignmentLines}`,
      '',
      `💱 Market data:\n${marketLines}`,
      '',
      `Accept reason: ${accept?.reason || 'n/a'}`
    ].join('\n');

    try {
      await bot.sendMessage(config.TELEGRAM_CHAT_ID, msg);
      logger.info({ symbol, root_tf }, 'Telegram message sent with market data');
    } catch (err) {
      logger.warn({ err }, 'Failed to send telegram');
    }
  },

  /**
   * sendStartupSummary:
   * - builds a snapshot of latest root signals and sends:
   *   1) First block: alphabetic summary a) SYMBOL [tfs] ... (A→Z)
   *   2) Per-signal detailed blocks (no per-signal label)
   *   3) Recommended header and labeled short recommended lines
   */
  async sendStartupSummary({ snapshot = [] } = {}) {
    if (!bot) return;
    try {
      const db = dbModule.get();

      // 1) use provided snapshot or fetch from DB
      let signals = Array.isArray(snapshot) && snapshot.length ? snapshot.slice() : [];
      if (!signals.length && typeof dbModule.getLatestSignalsSnapshot === 'function') {
        signals = dbModule.getLatestSignalsSnapshot() || [];
      }

      // 2) if still empty, attempt to run poller.initialScan + poller.scanOnce (best-effort) and wait for snapshot
      if (!signals.length) {
        try {
          const poller = require('./poller');
          logger.info('telegram.sendStartupSummary: snapshot empty — attempting poller.initialScan() and poller.scanOnce()');
          try { await poller.initialScan(); } catch (e) { logger.debug({ e }, 'initialScan failed inside telegram sendStartupSummary'); }
          try { if (typeof poller.scanOnce === 'function') await poller.scanOnce(); } catch (e) { logger.debug({ e }, 'scanOnce failed inside telegram sendStartupSummary'); }
        } catch (e) {
          logger.debug({ e }, 'telegram.sendStartupSummary: could not require poller (continuing)');
        }

        const waitMs = Number(config.STARTUP_SUMMARY_WAIT_MS || 15000);
        const retryMs = Number(config.STARTUP_SUMMARY_RETRY_MS || 500);
        const start = Date.now();
        while (Date.now() - start < waitMs) {
          try {
            signals = dbModule.getLatestSignalsSnapshot() || [];
          } catch (e) {
            logger.debug({ e }, 'sendStartupSummary: error fetching snapshot while waiting');
            signals = [];
          }
          if (signals && signals.length) break;
          await this._sleep(retryMs);
        }
      }

      // 3) Build summary map: symbol -> array of root_tfs
      const bySymbol = {};
      for (const s of signals) {
        if (!s || !s.symbol) continue;
        if (!bySymbol[s.symbol]) bySymbol[s.symbol] = [];
        if (!bySymbol[s.symbol].includes(String(s.root_tf))) bySymbol[s.symbol].push(String(s.root_tf));
      }

      // stable sort symbols
      const symbols = Object.keys(bySymbol).sort((a, b) => a.localeCompare(b, undefined, { sensitivity: 'base' }));

      // First block: A→Z summary lines
      const summaryLines = symbols.map((sym, idx) => {
        const label = this.getLabel(idx, { lowercase: true });
        const tfs = bySymbol[sym].join(',');
        return `${label}) ${sym} [${tfs}]`;
      });
      const header = `Startup root TF summary (${symbols.length}):\n${summaryLines.join('\n')}`;
      await bot.sendMessage(config.TELEGRAM_CHAT_ID, header);

      // 4) Per-signal detailed blocks (no per-signal label)
      // Sort signals for stable ordering
      signals.sort((a, b) => {
        const s = (a.symbol || '').localeCompare(b.symbol || '', undefined, { sensitivity: 'base' });
        if (s !== 0) return s;
        return String(a.root_tf || '').localeCompare(String(b.root_tf || ''), undefined, { numeric: true });
      });

      for (let i = 0; i < signals.length; i++) {
        const s = signals[i];
        try {
          await this.sendNewSignalSingleBlock(s); // detail block with MTF & market data
        } catch (e) {
          logger.debug({ e, i }, 'sendStartupSummary: failed to send per-signal detail block');
        }
        await this._sleep(config.TELEGRAM_SEND_DELAY_MS || 100);
      }

      // 5) Recommended header and short labeled lines
      let openCount = 0;
      try {
        const row = db.prepare("SELECT COUNT(*) as cnt FROM trades WHERE status = 'open'").get();
        openCount = row ? Number(row.cnt || 0) : 0;
      } catch (e) {
        logger.debug({ e }, 'sendStartupSummary: failed to read open trades count (assuming 0)');
        openCount = 0;
      }
      const maxSlots = Math.max(0, config.MAX_OPEN_TRADES - openCount);
      const recHeader = `Recommended to open (slots: ${maxSlots}):`;
      await bot.sendMessage(config.TELEGRAM_CHAT_ID, recHeader);

      // Prepare candidate accepts sorted by tvScore/mftScore
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

      if (recommended.length === 0) {
        await bot.sendMessage(config.TELEGRAM_CHAT_ID, 'No recommended signals');
      } else {
        for (let i = 0; i < recommended.length; i++) {
          const r = recommended[i];
          const label = this.getLabel(i, { lowercase: true });
          const line = `${label}) ${r.symbol} ${r.root_tf} - TV:${(r.tvScore * 100).toFixed(0)}% MTF:${(r.mtfScore * 100).toFixed(0)}% - ${r.reason}${config.OPENTRADE ? '' : ' (SIMULATED)'}`;
          await bot.sendMessage(config.TELEGRAM_CHAT_ID, line);
          await this._sleep(config.TELEGRAM_SEND_DELAY_MS || 100);
        }
      }

      logger.info('Startup telegram summary sent (summary + details + recommended)');
    } catch (err) {
      logger.warn({ err }, 'Failed to send startup telegram summary');
    }
  },

  async sendRootCandleUpdate({ snapshot = [], newRootTfs = [] } = {}) {
    if (!bot) return;
    try {
      // If snapshot empty, getLatestSignalsSnapshot will be used in sendStartupSummary
      let signals = Array.isArray(snapshot) && snapshot.length ? snapshot.slice() : [];
      if (!signals && typeof dbModule.getLatestSignalsSnapshot === 'function') {
        signals = dbModule.getLatestSignalsSnapshot() || [];
      }

      const filtered = (newRootTfs && newRootTfs.length)
        ? signals.filter(s => newRootTfs.includes(String(s.root_tf)))
        : signals;

      await this.sendStartupSummary({ snapshot: filtered });
    } catch (err) {
      logger.warn({ err }, 'Failed to send root candle update');
    }
  }
};
