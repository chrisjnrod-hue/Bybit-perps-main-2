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

  // Low-level builder for alignment lines with MTF score and signal count per TF
  buildAlignmentLines(alignment, countPerTf = {}) {
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
      const tfCount = countPerTf[tf] || 0;
      const countStr = tfCount > 0 ? ` (${tfCount} signals)` : '';
      lines.push(`${tf}: ${pos} ${hist} ${rise}${countStr}`.trim());
    }
    const mtfScore = total ? (positiveCount / total) : 0;
    return { lines: lines.join('\n'), mtfScore, positiveCount, total };
  },

  // Sends a single-block message for a newly discovered signal (scan boundary)
  async sendNewSignalSingleBlock(signal) {
    if (!bot) return;
    try {
      const { symbol, root_tf, detected_at, meta } = signal;
      const timeStr = new Date(detected_at).toISOString();
      const alignment = meta.alignment || {};
      const { lines, mtfScore } = this.buildAlignmentLines(alignment);

      const tvScore = (meta && typeof meta.tvScore === 'number') ? meta.tvScore : 0;
      const decision = meta.decision || 'monitor';
      const reason = meta.acceptReason || meta.decision || 'n/a';
      
      const mdata = meta.marketData || {};
      const price = mdata.price ? Number(mdata.price) : null;
      const vol24 = mdata.volume_24h_usdt ? Number(mdata.volume_24h_usdt) : null;
      const volChange = (typeof mdata.volume_change_pct === 'number') ? Number(mdata.volume_change_pct) : null;
      const marketCap = mdata.market_cap ? Number(mdata.market_cap) : null;

      const marketLines = [
        `Price: ${price ? price.toLocaleString('en-US', { maximumFractionDigits: 8 }) : 'n/a'}`,
        `24h USDT Volume: ${vol24 ? vol24.toLocaleString('en-US', { maximumFractionDigits: 2 }) : 'n/a'}`,
        `24h Volume Change: ${volChange !== null ? volChange.toFixed(2) + '%' : 'n/a'}`,
        `Market Cap: ${marketCap ? '$' + marketCap.toLocaleString('en-US', { maximumFractionDigits: 0 }) : 'n/a'}`
      ].join('\n');

      const msg = [
        `New signal: ${symbol} (${root_tf})`,
        `Time: ${timeStr}`,
        `Decision: ${decision} (reason: ${reason})`,
        '',
        `TV score: ${(tvScore*100).toFixed(0)}% • MTF score: ${(mtfScore*100).toFixed(0)}%`,
        '',
        `MTF status:\n${lines}`,
        '',
        `Market data:\n${marketLines}`,
      ].join('\n');

      await bot.sendMessage(config.TELEGRAM_CHAT_ID, msg);
      logger.info({ symbol, root_tf }, 'Telegram new-signal message sent');
    } catch (err) {
      logger.warn({ err }, 'Failed to send telegram new-signal block');
    }
  },

  // Similar to earlier sendRootSignalBlock but accepts tvScore and mtfScore with market data
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
    const volChange = (typeof marketData?.volume_change_pct === 'number') ? Number(marketData.volume_change_pct) : null;
    const marketCap = marketData?.market_cap ? Number(marketData.market_cap) : null;

    const marketLines = [
      `Price: ${price ? price.toLocaleString('en-US', { maximumFractionDigits: 8 }) : 'n/a'}`,
      `24h USDT Volume: ${vol24 ? vol24.toLocaleString('en-US', { maximumFractionDigits: 2 }) : 'n/a'}`,
      `24h Volume Change: ${volChange !== null ? volChange.toFixed(2) + '%' : 'n/a'}`,
      `Market Cap: ${marketCap ? '$' + marketCap.toLocaleString('en-US', { maximumFractionDigits: 0 }) : 'n/a'}`
    ].join('\n');

    const msg = `Root signal: ${symbol} (${root_tf})\nTime: ${timeStr}\nDecision: ${decision}\n\nTV score: ${(tvScore*100).toFixed(0)}% • MTF score: ${(mtfScore*100).toFixed(0)}%\n\nMTF status:\n${alignmentLines}\n\nMarket data:\n${marketLines}\n\nAccept reason: ${accept?.reason || 'n/a'}`;

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
   * Builds:
   * 1) First block: root tfs signal summary A-Z + numeric count PER TF
   * 2) Second block: recommended signals to open trade (respect MAX_OPEN_TRADES) — shows acceptance reason and simulated tag if OPENTRADE disabled
   * 3+) Subsequent blocks: A-Z listing of all root TF signals with market data per signal
   */
  async sendStartupSummary({ snapshot = [] } = {}) {
    if (!bot) return;
    try {
      const db = dbModule.get();

      // Build map symbol -> array of signals
      const bySymbol = {};
      const tfCounts = {}; // tf -> count
      
      for (const s of snapshot) {
        if (!bySymbol[s.symbol]) bySymbol[s.symbol] = [];
        bySymbol[s.symbol].push(s);
        
        // Count signals per TF
        if (!tfCounts[s.root_tf]) tfCounts[s.root_tf] = 0;
        tfCounts[s.root_tf]++;
      }

      // First block: A-Z summary with signal count per TF
      const symbols = Object.keys(bySymbol).sort((a, b) => a.localeCompare(b, undefined, { sensitivity: 'base' }));
      const count = symbols.length;
      
      const tfSummary = Object.entries(tfCounts)
        .sort((a, b) => {
          // Sort TF: numeric ascending, then D at end
          const aNum = a[0] === 'D' ? 999 : Number(a[0]);
          const bNum = b[0] === 'D' ? 999 : Number(b[0]);
          return aNum - bNum;
        })
        .map(([tf, cnt]) => `${tf}: ${cnt}`)
        .join(' • ');
      
      const summaryLines = symbols.map(sym => {
        const tfs = bySymbol[sym].map(si => si.root_tf).join(',');
        return `${sym} [${tfs}]`;
      });
      
      const firstBlock = `Startup root TF summary (total: ${count}):\n${tfSummary}\n\n${summaryLines.join('\n')}`;

      // Second block: recommended signals to open trade/simulated open trades
      // Determine available slots
      const openCountRow = db.prepare("SELECT COUNT(*) as cnt FROM trades WHERE status = 'open'").get();
      const openCount = openCountRow ? Number(openCountRow.cnt || 0) : 0;
      const maxSlots = Math.max(0, config.MAX_OPEN_TRADES - openCount);

      // Prepare candidate accepts sorted by tvScore desc then mtfScore
      const candidates = snapshot
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
      const recLines = recommended.map((r, idx) => {
        const sim = config.OPENTRADE ? '' : ' (SIMULATED)';
        return `${String.fromCharCode(65 + idx)}) ${r.symbol} ${r.root_tf} - TV:${(r.tvScore*100).toFixed(0)}% MTF:${(r.mtfScore*100).toFixed(0)}% - ${r.reason}${sim}`;
      });
      const secondBlock = `Recommended to open (slots: ${maxSlots}):\n${recLines.length ? recLines.join('\n') : 'No recommended signals'}`;

      // Subsequent blocks: one block per symbol with details and market data
      const listingBlocks = [];
      for (const sym of symbols) {
        const lines = bySymbol[sym].map(s => {
          const tv = s.meta?.tvScore || 0;
          const mtf = s.meta?.mtfScore || 0;
          const dec = s.meta?.decision || 'monitor';
          const reason = s.meta?.acceptReason || 'n/a';
          
          // Add market data to each signal
          const mdata = s.meta?.marketData || {};
          const price = mdata.price ? Number(mdata.price).toFixed(8) : 'n/a';
          const vol24 = mdata.volume_24h_usdt ? (Number(mdata.volume_24h_usdt) / 1e6).toFixed(2) : 'n/a';
          const volChange = (typeof mdata.volume_change_pct === 'number') ? Number(mdata.volume_change_pct).toFixed(2) : 'n/a';
          const marketCap = mdata.market_cap ? '$' + (Number(mdata.market_cap) / 1e6).toFixed(0) + 'M' : 'n/a';
          
          return `${sym} ${s.root_tf} — ${dec} | TV:${(tv*100).toFixed(0)}% MTF:${(mtf*100).toFixed(0)}% | Vol:${vol24}M VolΔ:${volChange}% MC:${marketCap}`;
        });
        listingBlocks.push(lines.join('\n'));
      }

      // Compose and send as multiple messages/blocks (Telegram message size limits considered)
      await bot.sendMessage(config.TELEGRAM_CHAT_ID, firstBlock);
      await bot.sendMessage(config.TELEGRAM_CHAT_ID, secondBlock);
      // send each listing block as a separate message (respecting your requirement "A-Z listing of all root tfs signals per block")
      for (const blk of listingBlocks) {
        await bot.sendMessage(config.TELEGRAM_CHAT_ID, blk);
      }

      logger.info('Startup telegram summary sent');
    } catch (err) {
      logger.warn({ err }, 'Failed to send startup telegram summary');
    }
  },

  /**
   * sendRootCandleUpdate:
   * - Called when new root candle(s) open. Should send:
   *   1) Root TF summary with signal counts (matching initial deploy format)
   *   2) Recommended blocks (respect MAX_OPEN_TRADES)
   *   3) A–Z listing per symbol with market data
   */
  async sendRootCandleUpdate({ snapshot = [], newRootTfs = [] } = {}) {
    if (!bot) return;
    try {
      const db = dbModule.get();

      // Build map symbol -> array of signals (ALL signals, not filtered by newRootTfs)
      // This ensures we maintain consistency with initial deploy
      const bySymbol = {};
      const tfCounts = {};
      
      for (const s of snapshot) {
        if (!bySymbol[s.symbol]) bySymbol[s.symbol] = [];
        bySymbol[s.symbol].push(s);
        
        if (!tfCounts[s.root_tf]) tfCounts[s.root_tf] = 0;
        tfCounts[s.root_tf]++;
      }

      // First block: Root TF summary with signal counts (same as startup)
      const symbols = Object.keys(bySymbol).sort((a, b) => a.localeCompare(b, undefined, { sensitivity: 'base' }));
      const count = symbols.length;
      
      const tfSummary = Object.entries(tfCounts)
        .sort((a, b) => {
          const aNum = a[0] === 'D' ? 999 : Number(a[0]);
          const bNum = b[0] === 'D' ? 999 : Number(b[0]);
          return aNum - bNum;
        })
        .map(([tf, cnt]) => `${tf}: ${cnt}`)
        .join(' • ');
      
      const summaryLines = symbols.map(sym => {
        const tfs = bySymbol[sym].map(si => si.root_tf).join(',');
        return `${sym} [${tfs}]`;
      });
      
      const firstBlock = `New root candle(s): ${newRootTfs.join(',')} — Total signals: ${count}\n${tfSummary}\n\n${summaryLines.join('\n')}`;

      // Second block: recommended signals to open trade/simulated open trades
      const openCountRow = db.prepare("SELECT COUNT(*) as cnt FROM trades WHERE status = 'open'").get();
      const openCount = openCountRow ? Number(openCountRow.cnt || 0) : 0;
      const maxSlots = Math.max(0, config.MAX_OPEN_TRADES - openCount);

      const candidates = snapshot
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
      const recLines = recommended.map((r, idx) => {
        const sim = config.OPENTRADE ? '' : ' (SIMULATED)';
        return `${String.fromCharCode(65 + idx)}) ${r.symbol} ${r.root_tf} - TV:${(r.tvScore*100).toFixed(0)}% MTF:${(r.mtfScore*100).toFixed(0)}% - ${r.reason}${sim}`;
      });
      const secondBlock = `Recommended to open (slots: ${maxSlots}):\n${recLines.length ? recLines.join('\n') : 'No recommended signals'}`;

      // Subsequent blocks: one block per symbol with market data
      const listingBlocks = [];
      for (const sym of symbols) {
        const lines = bySymbol[sym].map(s => {
          const tv = s.meta?.tvScore || 0;
          const mtf = s.meta?.mtfScore || 0;
          const dec = s.meta?.decision || 'monitor';
          
          const mdata = s.meta?.marketData || {};
          const price = mdata.price ? Number(mdata.price).toFixed(8) : 'n/a';
          const vol24 = mdata.volume_24h_usdt ? (Number(mdata.volume_24h_usdt) / 1e6).toFixed(2) : 'n/a';
          const volChange = (typeof mdata.volume_change_pct === 'number') ? Number(mdata.volume_change_pct).toFixed(2) : 'n/a';
          const marketCap = mdata.market_cap ? '$' + (Number(mdata.market_cap) / 1e6).toFixed(0) + 'M' : 'n/a';
          
          return `${sym} ${s.root_tf} — ${dec} | TV:${(tv*100).toFixed(0)}% MTF:${(mtf*100).toFixed(0)}% | Vol:${vol24}M VolΔ:${volChange}% MC:${marketCap}`;
        });
        listingBlocks.push(lines.join('\n'));
      }

      // Send all blocks
      await bot.sendMessage(config.TELEGRAM_CHAT_ID, firstBlock);
      await bot.sendMessage(config.TELEGRAM_CHAT_ID, secondBlock);
      for (const blk of listingBlocks) {
        await bot.sendMessage(config.TELEGRAM_CHAT_ID, blk);
      }

      logger.info({ newRootTfs }, 'Root candle update telegram sent');
    } catch (err) {
      logger.warn({ err }, 'Failed to send root candle update');
    }
  }
};
