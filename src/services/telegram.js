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

  _sleep(ms) { return new Promise((resolve) => setTimeout(resolve, ms || 0)); },

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

    const priceAlt = (typeof md.last === 'number') ? md.last : (md.last ? Number(md.last) : null);
    const vol24Alt = (typeof md.volume24h === 'number') ? md.volume24h : (md.volume24h ? Number(md.volume24h) : null);
    const volChangeAlt = (typeof md.volumeChangePct === 'number') ? md.volumeChangePct : (md.volumeChangePct ? Number(md.volumeChangePct) : null);
    const marketCapAlt = (typeof md.marketCap === 'number') ? md.marketCap : (md.marketCap ? Number(md.marketCap) : null);

    const finalPrice = price !== null ? price : (priceAlt !== null ? priceAlt : 0);
    const finalVol24 = vol24 !== null ? vol24 : (vol24Alt !== null ? vol24Alt : 0);
    const finalVolChange = volChange !== null ? volChange : (volChangeAlt !== null ? volChangeAlt : null);
    const finalMarketCap = marketCap !== null ? marketCap : (marketCapAlt !== null ? marketCapAlt : null);

    const lines = [
      `💰 Price: ${finalPrice !== null && finalPrice > 0 ? '$' + finalPrice.toLocaleString('en-US', { maximumFractionDigits: 8 }) : '0'}`,
      `💵 24h Volume: ${finalVol24 !== null && finalVol24 > 0 ? '$' + finalVol24.toLocaleString('en-US', { maximumFractionDigits: 2 }) + ' USDT' : '0 USDT'}`,
      `📈 Volume Change: ${finalVolChange !== null ? (Number(finalVolChange).toFixed(2) + '%') : 'n/a'}`,
      `💎 Market Cap: ${finalMarketCap && finalMarketCap > 0 ? '$' + finalMarketCap.toLocaleString('en-US', { maximumFractionDigits: 0 }) : 'n/a'}`
    ];
    return lines.join('\n');
  },

  buildSignalMessage(signal) {
    const { symbol, root_tf, detected_at, meta = {} } = signal || {};
    const timeStr = detected_at ? new Date(detected_at).toISOString() : new Date().toISOString();
    const alignment = meta.alignment || {};

    const tvScoreRaw = (typeof meta.tvScore === 'number') ? meta.tvScore : (meta.tvScore ? Number(meta.tvScore) : 0);
    const tvScorePctFromMeta = (typeof meta.tvScorePct === 'number' && !isNaN(meta.tvScorePct)) ? meta.tvScorePct : null;
    const tvPercent = tvScorePctFromMeta !== null ? tvScorePctFromMeta : Math.round((tvScoreRaw || 0) * 100);

    const tvSource = meta.tvSource || 'error';
    const mtfScore = (typeof meta.mtfScore === 'number') ? meta.mtfScore : null;
    const decision = meta.decision || 'monitor';
    const reason = meta.acceptReason || meta.reason || 'n/a';

    const { lines: alignmentLines, mtfScore: computedMtfScore } = this.buildAlignmentLines(alignment);
    const usedMtfScore = (mtfScore !== null) ? mtfScore : computedMtfScore;
    const mtfPercent = Math.round((usedMtfScore || 0) * 100);

    const scoringLine = `📊 Scoring:\nTV: ${tvPercent}% (${tvSource}) • MTF: ${mtfPercent}%`;
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

  async sendNewSignalSingleBlock(signal, _label = null) {
    if (!bot) return;
    try {
      const baseMsg = this.buildSignalMessage(signal);
      await bot.sendMessage(config.TELEGRAM_CHAT_ID, baseMsg);
      logger.info({ symbol: signal?.symbol, root_tf: signal?.root_tf }, 'Telegram new-signal message sent (detail block)');
    } catch (err) {
      logger.warn({ err }, 'Failed to send telegram new-signal block');
    }
  },

  async sendRootSignalBlock({ symbol, root_tf, alignment, detected_at, accept, marketData, tvScore = 0, tvScorePct = null, tvSource = 'error', mtfScore = 0 }) {
    if (!bot) return;
    const timeStr = new Date(detected_at).toISOString();
    
    let alignmentLines = Object.entries(alignment || {}).map(([tf, info]) => {
      if (!info || !info.hasOwnProperty('histogram')) return `${tf}: ⚪ unknown`;
      const posSym = info.positive ? '🟢' : '🔴';
      const rise = info.rising ? '↑' : '↓';
      return `${tf}: ${posSym} ${info.positive ? 'POS' : 'NEG'} hist=${Number(info.histogram).toFixed(6)} ${rise}`;
    }).join('\n');

    const decision = accept && accept.decision ? accept.decision : 'monitor';
    const tvPercent = (typeof tvScorePct === 'number' && !isNaN(tvScorePct)) ? tvScorePct : Math.round((tvScore || 0) * 100);
    const mtfPercent = Math.round((mtfScore || 0) * 100);
    const marketLines = this.formatMarketData(marketData || {});

    const msg = [
      `🎯 Root signal: ${symbol} (${root_tf})`,
      `⏰ Time: ${timeStr}`,
      `${decision === 'accept' ? '✅ Decision' : '⚠️ Decision'}: ${decision}`,
      '',
      `📊 Scoring: TV: ${tvPercent}% (${tvSource}) • MTF: ${mtfPercent}%`,
      '',
      `🛰️ MTF Status:`,
      alignmentLines || 'No MTF data',
      '',
      `💱 Market Data:`,
      marketLines,
      '',
      `Reason: ${accept?.reason || 'n/a'}`
    ].join('\n');

    try {
      await bot.sendMessage(config.TELEGRAM_CHAT_ID, msg);
      logger.info({ symbol, root_tf }, 'Telegram root signal message sent with market data & TV rating');
    } catch (err) {
      logger.warn({ err }, 'Failed to send telegram root signal block');
    }
  },

  async sendStartupSummary({ snapshot = [] } = {}) {
    if (!bot) return;
    try {
      const db = dbModule.get();

      let signals = Array.isArray(snapshot) && snapshot.length ? snapshot.slice() : [];
      if (!signals.length && typeof dbModule.getLatestSignalsSnapshot === 'function') {
        signals = dbModule.getLatestSignalsSnapshot() || [];
      }

      if (!Array.isArray(signals) || signals.length === 0) {
        try {
          if (db && db.prepare) {
            const rows = db.prepare('SELECT key, symbol, root_tf, detected_at, state, meta FROM signals ORDER BY detected_at DESC LIMIT ?').all(500);
            if (rows && rows.length) {
              signals = rows.map(r => {
                let meta = r.meta;
                if (typeof meta === 'string') {
                  try { meta = JSON.parse(meta); } catch (e) { /* keep as string */ }
                }
                return {
                  key: r.key,
                  symbol: r.symbol,
                  root_tf: r.root_tf,
                  detected_at: r.detected_at,
                  state: r.state,
                  meta
                };
              });
            }
          }
        } catch (e) {
          logger.debug({ e }, 'sendStartupSummary: fallback DB read failed');
        }
      }

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

      const header = `📊 Startup root TF summary (${signals.length} signals):\n${summaryParts.join(' • ')}\n\n${symbolLines}`;
      await bot.sendMessage(config.TELEGRAM_CHAT_ID, header);
      logger.info('Sent startup summary header');
      await this._sleep(config.TELEGRAM_SEND_DELAY_MS || 100);

      signals.sort((a, b) => {
        const s = (a.symbol || '').localeCompare(b.symbol || '', undefined, { sensitivity: 'base' });
        if (s !== 0) return s;
        return String(a.root_tf || '').localeCompare(String(b.root_tf || ''), undefined, { numeric: true });
      });

      for (let i = 0; i < signals.length; i++) {
        try {
          await this.sendNewSignalSingleBlock(signals[i], null);
        } catch (e) {
          logger.debug({ e, i }, 'sendStartupSummary: failed to send per-signal block');
        }
        await this._sleep(config.TELEGRAM_SEND_DELAY_MS || 100);
      }

      let openCount = 0;
      try {
        const row = db.prepare("SELECT COUNT(*) as cnt FROM trades WHERE status = 'open'").get();
        openCount = row ? Number(row.cnt || 0) : 0;
      } catch (e) {
        logger.debug({ e }, 'sendStartupSummary: failed to read open trades count');
        openCount = 0;
      }
      const maxSlots = Math.max(0, config.MAX_OPEN_TRADES - openCount);
      const recHeader = `📈 Recommended to open (${maxSlots} slots available):`;
      await bot.sendMessage(config.TELEGRAM_CHAT_ID, recHeader);
      await this._sleep(config.TELEGRAM_SEND_DELAY_MS || 100);

      const normalizedCandidates = signals.map(s => {
        const meta = s.meta || {};
        const dec = (meta && meta.decision) || (meta && meta.accept && meta.accept.decision) || (meta && meta.acceptDecision) || 'monitor';
        const tvPct = (typeof meta.tvScorePct === 'number') ? meta.tvScorePct : (typeof meta.tvScore === 'number' ? Math.round(meta.tvScore * 100) : 0);
        const mtf = (typeof meta.mtfScore === 'number') ? meta.mtfScore : 0;
        return {
          key: s.key,
          symbol: s.symbol,
          root_tf: s.root_tf,
          tvScorePct: tvPct,
          mtfScore: mtf,
          acceptDecision: String(dec),
          reason: meta?.acceptReason || meta?.reason || 'n/a',
          raw: s
        };
      });

      let candidates = normalizedCandidates.filter(c => String(c.acceptDecision).toLowerCase() === 'accept');

      if (candidates.length === 0) {
        candidates = normalizedCandidates.filter(c => String(c.acceptDecision).toLowerCase() !== 'reject');
      }

      candidates.sort((a, b) => {
        if (b.tvScorePct !== a.tvScorePct) return b.tvScorePct - a.tvScorePct;
        return b.mtfScore - a.mtfScore;
      });

      const recommended = candidates.slice(0, maxSlots);

      if (!recommended || recommended.length === 0) {
        await bot.sendMessage(config.TELEGRAM_CHAT_ID, 'No recommended signals (all rejections or filtered)');
      } else {
        for (let i = 0; i < recommended.length; i++) {
          const r = recommended[i];
          const label = this.getLabel(i, { lowercase: true });
          const tvPercent = r.tvScorePct;
          const mtfPercent = Math.round((r.mtfScore || 0) * 100);
          const simNote = config.OPENTRADE ? '' : ' [SIMULATED]';
          const line = `${label}) ${r.symbol} ${r.root_tf} - TV:${tvPercent}% MTF:${mtfPercent}% - ${r.reason}${simNote}`;
          await bot.sendMessage(config.TELEGRAM_CHAT_ID, line);
          await this._sleep(config.TELEGRAM_SEND_DELAY_MS || 100);
        }
      }

      logger.info('Startup telegram summary completed (header + per-signal blocks + recommended)');
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

      logger.info({ newRootTfs, filteredCount: filtered.length }, 'sendRootCandleUpdate: sending for new root candles');
      await this.sendStartupSummary({ snapshot: filtered });
    } catch (err) {
      logger.warn({ err }, 'Failed to send root candle update');
    }
  },

  async sendAlignmentConfirmation({ symbol, alignment, blockId }) {
    if (!bot) return;
    try {
      const { lines: alignmentLines, mtfScore, positiveCount, total } = this.buildAlignmentLines(alignment);
      const mtfPercent = Math.round((mtfScore || 0) * 100);
      
      const msg = [
        `✅ ALL MTF ALIGNED: ${symbol}`,
        `⏰ Block: ${blockId}`,
        ``,
        `🛰️ MTF Status (${positiveCount}/${total}):`,
        alignmentLines,
        ``,
        `📊 Overall Score: ${mtfPercent}%`
      ].join('\n');

      await bot.sendMessage(config.TELEGRAM_CHAT_ID, msg);
      logger.info({ symbol, blockId }, 'Telegram alignment confirmation sent');
    } catch (err) {
      logger.warn({ err, symbol }, 'Failed to send alignment confirmation');
    }
  }
};
