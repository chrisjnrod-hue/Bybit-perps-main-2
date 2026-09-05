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

  // Compose a detailed per-signal message (no label)
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

  /**
   * sendNewSignalSingleBlock(signal, label)
   * - if label provided (e.g., 'a'), message will be prefixed with 'a) '
   */
  async sendNewSignalSingleBlock(signal, label = null) {
    if (!bot) return;
    try {
      const baseMsg = this.buildSignalMessage(signal);
      const msg = label ? `${label}) ${baseMsg}` : baseMsg;
      await bot.sendMessage(config.TELEGRAM_CHAT_ID, msg);
      logger.info({ symbol: signal?.symbol, root_tf: signal?.root_tf, label }, 'Telegram new-signal message sent (detail block)');
    } catch (err) {
      logger.warn({ err }, 'Failed to send telegram new-signal block');
    }
  },

  // Keep sendRootSignalBlock for callers which use it directly
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
   * - single summary block: counts per root TF only (no alphabetic symbol listing)
   * - then per-signal detailed blocks, alphabetically ordered and labeled a), b), ...
   * - finally recommended header and labeled short recommended lines
   */
  async sendStartupSummary({ snapshot = [] } = {}) {
    if (!bot) return;
    try {
      const db = dbModule.get();

      let signals = Array.isArray(snapshot) && snapshot.length ? snapshot.slice() : [];
      if (!signals.length && typeof dbModule.getLatestSignalsSnapshot === 'function') {
        signals = dbModule.getLatestSignalsSnapshot() || [];
      }

      if (!signals.length) {
        try {
          const poller = require('./poller');
          try { await poller.initialScan(); } catch (e) { logger.debug({ e }, 'sendStartupSummary: initialScan failed (continuing)'); }
          try { if (typeof poller.scanOnce === 'function') await poller.scanOnce({ notifyNewSignals: false }); } catch (e) { logger.debug({ e }, 'sendStartupSummary: scanOnce failed (continuing)'); }
        } catch (e) {
          logger.debug({ e }, 'sendStartupSummary: unable to require poller (continuing)');
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

      // Build counts per root_tf (no alphabetic symbol listing)
      const tfCounts = {};
      for (const s of signals) {
        const tf = String(s.root_tf || 'unknown');
        tfCounts[tf] = (tfCounts[tf] || 0) + 1;
      }

      const orderedRootTfs = Array.isArray(config.ROOT_TFS) && config.ROOT_TFS.length
        ? config.ROOT_TFS.map(String)
        : Object.keys(tfCounts);
      for (const tf of Object.keys(tfCounts)) {
        if (!orderedRootTfs.includes(tf)) orderedRootTfs.push(tf);
      }

      const summaryParts = orderedRootTfs.map(tf => `${tf}: ${tfCounts[tf] || 0}`);
      const header = `Startup root TF summary (${signals.length} signals):\n${summaryParts.join(' • ')}`;
      await bot.sendMessage(config.TELEGRAM_CHAT_ID, header);

      // Per-signal blocks: alphabetical order by symbol, then tf — each labeled a), b), ...
      signals.sort((a, b) => {
        const s = (a.symbol || '').localeCompare(b.symbol || '', undefined, { sensitivity: 'base' });
        if (s !== 0) return s;
        return String(a.root_tf || '').localeCompare(String(b.root_tf || ''), undefined, { numeric: true });
      });

      for (let i = 0; i < signals.length; i++) {
        const label = this.getLabel(i, { lowercase: true });
        try {
          await this.sendNewSignalSingleBlock(signals[i], label);
        } catch (e) {
          logger.debug({ e, i }, 'sendStartupSummary: failed to send per-signal labeled block (continuing)');
        }
        await this._sleep(config.TELEGRAM_SEND_DELAY_MS || 100);
      }

      // Recommended header and short labeled lines
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

      logger.info('Startup telegram summary sent (counts summary + labeled per-signal blocks + recommended)');
    } catch (err) {
      logger.warn({ err }, 'Failed to send startup telegram summary');
    }
  },

  async sendRootCandleUpdate({ snapshot = [], newRootTfs = [] } = {}) {
    if (!bot) return;
    try {
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
