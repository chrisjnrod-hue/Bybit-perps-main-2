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

  // UPDATED: Show alignment lines with proper 5m/15m values
  buildAlignmentLines(alignment) {
    const lines = [];
    let positiveCount = 0;
    let total = 0;
    
    // Sort to show: 5m, 15m, 60m, 240m, D
    const tfOrder = ['5', '15', '60', '240', 'D'];
    const sortedTfs = tfOrder.filter(tf => alignment && tf in alignment);
    const otherTfs = Object.keys(alignment || {}).filter(tf => !tfOrder.includes(tf)).sort();
    const allTfs = [...sortedTfs, ...otherTfs];

    for (const tf of allTfs) {
      const info = alignment[tf];
      if (!info) continue;
      
      total++;
      const ok = info && typeof info.histogram !== 'undefined';
      const pos = ok ? (info.positive ? '🟢 POS' : '🔴 NEG') : '❓ UNKNOWN';
      if (info && info.positive) positiveCount++;
      const hist = ok ? `${Number(info.histogram).toFixed(8)}` : 'n/a';
      const rise = ok ? (info.rising ? '📈' : '📉') : '';
      
      // HIGHLIGHT 5m and 15m
      const tfLabel = (tf === '5' || tf === '15') ? `*${String(tf).padEnd(5)}*` : String(tf).padEnd(5);
      lines.push(`${tfLabel} ${pos.padEnd(12)} hist=${hist} ${rise}`.trim());
    }
    
    const mtfScore = total ? (positiveCount / total) : 0;
    return { lines: lines.join('\n'), mtfScore, positiveCount, total };
  },

  // UPDATED: Send new signal with full MTF data
  async sendNewSignalSingleBlock(signal) {
    if (!bot) return;
    try {
      const { symbol, root_tf, detected_at, meta, alignment, marketData, tvScore, mtfScore } = signal;
      const timeStr = new Date(detected_at).toISOString();
      
      const alignObj = alignment || meta?.alignment || {};
      const { lines, mtfScore: score } = this.buildAlignmentLines(alignObj);

      const tv = tvScore || meta?.tvScore || 0;
      const mtf = mtfScore || meta?.mtfScore || 0;
      const decision = meta?.decision || 'monitor';
      const reason = meta?.acceptReason || 'n/a';

      const mdata = marketData || meta?.marketData || {};
      const price = mdata?.price ? Number(mdata.price) : null;
      const vol24 = mdata?.volume_24h_usdt ? Number(mdata.volume_24h_usdt) : null;
      const volChange = (typeof mdata?.volume_change_pct === 'number') ? Number(mdata.volume_change_pct) : null;
      const marketCap = mdata?.market_cap ? Number(mdata.market_cap) : null;

      const marketLines = [
        `💰 Price: ${price ? '$' + price.toLocaleString('en-US', { maximumFractionDigits: 8 }) : 'n/a'}`,
        `📊 24h Volume: ${vol24 ? '$' + vol24.toLocaleString('en-US', { maximumFractionDigits: 0 }) : 'n/a'} USDT`,
        `📈 Volume Change: ${volChange !== null ? volChange.toFixed(2) + '%' : 'n/a'}`,
        `💎 Market Cap: ${marketCap ? '$' + marketCap.toLocaleString('en-US', { maximumFractionDigits: 0 }) : 'n/a'}`
      ].join('\n');

      const msg = [
        `🎯 New signal: ${symbol} (${root_tf})`,
        `⏰ Time: ${timeStr}`,
        `✅ Decision: ${decision} (${reason})`,
        ``,
        `📊 Scoring:`,
        `  TV: ${(tv*100).toFixed(0)}% • MTF: ${(mtf*100).toFixed(0)}%`,
        ``,
        `📡 MTF Status: (5m/15m highlighted)`,
        lines,
        ``,
        `💵 Market Data:`,
        marketLines
      ].join('\n');

      await bot.sendMessage(config.TELEGRAM_CHAT_ID, msg, { parse_mode: 'Markdown' });
      logger.info({ symbol, root_tf }, 'Telegram new-signal message sent with MTF status');
    } catch (err) {
      logger.warn({ err }, 'Failed to send telegram new-signal block');
    }
  },

  // UPDATED: Root signal block with full MTF data
  async sendRootSignalBlock({ symbol, root_tf, alignment, detected_at, accept, marketData, tvScore = 0, mtfScore = 0 }) {
    if (!bot) return;
    const timeStr = new Date(detected_at).toISOString();
    
    const { lines: alignmentLines } = this.buildAlignmentLines(alignment);

    const decision = accept && accept.decision ? accept.decision : 'monitor';

    const price = marketData?.price ? Number(marketData.price) : null;
    const vol24 = marketData?.volume_24h_usdt ? Number(marketData.volume_24h_usdt) : null;
    const volChange = (typeof marketData?.volume_change_pct === 'number') ? Number(marketData.volume_change_pct) : null;
    const marketCap = marketData?.market_cap ? Number(marketData.market_cap) : null;

    const marketLines = [
      `💰 Price: ${price ? '$' + price.toLocaleString('en-US', { maximumFractionDigits: 8 }) : 'n/a'}`,
      `📊 24h Volume: ${vol24 ? '$' + vol24.toLocaleString('en-US', { maximumFractionDigits: 0 }) : 'n/a'} USDT`,
      `📈 Volume Change: ${volChange !== null ? volChange.toFixed(2) + '%' : 'n/a'}`,
      `💎 Market Cap: ${marketCap ? '$' + marketCap.toLocaleString('en-US', { maximumFractionDigits: 0 }) : 'n/a'}`
    ].join('\n');

    const msg = [
      `🎯 Root signal: ${symbol} (${root_tf})`,
      `⏰ Time: ${timeStr}`,
      `✅ Decision: ${decision} ${accept?.reason ? `(${accept.reason})` : ''}`,
      ``,
      `📊 Scoring:`,
      `  TV: ${(tvScore*100).toFixed(0)}% • MTF: ${(mtfScore*100).toFixed(0)}%`,
      ``,
      `📡 MTF Status: (5m/15m highlighted)`,
      alignmentLines,
      ``,
      `💵 Market Data:`,
      marketLines
    ].join('\n');

    try {
      await bot.sendMessage(config.TELEGRAM_CHAT_ID, msg, { parse_mode: 'Markdown' });
      logger.info({ symbol, root_tf }, 'Telegram root signal sent with MTF status');
    } catch (err) {
      logger.warn({ err }, 'Failed to send telegram signal block');
    }
  },

  /**
   * sendStartupSummary - SHOWS RECOMMENDED SIGNALS WITH COUNTS
   */
  async sendStartupSummary({ snapshot = [] } = {}) {
    if (!bot) return;
    try {
      const db = dbModule.get();

      // Handle empty snapshot
      if (!snapshot || snapshot.length === 0) {
        const emptyMsg = [
          `═══════════════════════════════════`,
          `📊 Startup Root TF Summary`,
          `═══════════════════════════════════`,
          ``,
          `⏳ Status: Initializing...`,
          `🔍 Scanning symbols and detecting signals...`,
          `⌛ Compiling MACD data...`,
          ``,
          `Signals will appear here once detected.`,
          `═══════════════════════════════════`
        ].join('\n');
        
        await bot.sendMessage(config.TELEGRAM_CHAT_ID, emptyMsg);
        logger.info('Startup summary sent (initializing)');
        return;
      }

      // Build maps
      const bySymbol = {};
      const byTf = {};
      for (const s of snapshot) {
        if (!bySymbol[s.symbol]) bySymbol[s.symbol] = [];
        bySymbol[s.symbol].push(s);
        
        if (!byTf[s.root_tf]) byTf[s.root_tf] = 0;
        byTf[s.root_tf]++;
      }

      // First block: Summary with TF counts
      const symbols = Object.keys(bySymbol).sort((a, b) => a.localeCompare(b, undefined, { sensitivity: 'base' }));
      const count = symbols.length;
      
      const summaryLines = [
        `🎯 Root TF Signal Count:`,
        ...Object.keys(byTf).sort().map(tf => `  ${String(tf).padEnd(4)}: ${byTf[tf]} signal${byTf[tf] > 1 ? 's' : ''}`)
      ];
      
      const symbolLines = symbols.map(sym => {
        const tfs = bySymbol[sym].map(si => si.root_tf).join(',');
        const tvAvg = bySymbol[sym].reduce((acc, s) => acc + (s.meta?.tvScore || 0), 0) / bySymbol[sym].length;
        return `${sym.padEnd(12)} [${tfs}] TV:${(tvAvg*100).toFixed(0)}%`;
      });

      const firstBlock = [
        `═══════════════════════════════════`,
        `📊 Startup Root TF Summary (${count} symbols)`,
        `═══════════════════════════════════`,
        ...summaryLines,
        ``,
        ...symbolLines,
        `═══════════════════════════════════`
      ].join('\n');

      // Second block: RECOMMENDED TRADES
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
      const recLines = recommended.length > 0 
        ? recommended.map((r, idx) => {
            const sim = config.OPENTRADE ? '✅' : '🔄 [SIM]';
            return `  ${String.fromCharCode(65 + idx)}) ${r.symbol.padEnd(12)} ${r.root_tf.padEnd(5)} • TV:${(r.tvScore*100).toFixed(0)}% MTF:${(r.mtfScore*100).toFixed(0)}% • ${r.reason} ${sim}`;
          })
        : ['  ℹ️  No signals with ACCEPT decision yet (monitoring)'];

      const secondBlock = [
        `═══════════════════════════════════`,
        `🚀 Recommended to Open (${maxSlots} slots | Total Accepts: ${candidates.length})`,
        `═══════════════════════════════════`,
        ...recLines,
        `═══════════════════════════════════`
      ].join('\n');

      // Subsequent blocks: Per-symbol details
      const listingBlocks = [];
      for (const sym of symbols) {
        const lines = bySymbol[sym].map(s => {
          const tv = s.meta?.tvScore || 0;
          const mtf = s.meta?.mtfScore || 0;
          const dec = s.meta?.decision || 'monitor';
          const reason = s.meta?.acceptReason || 'n/a';
          const vol = s.meta?.marketData?.volume_24h_usdt ? `$${(s.meta.marketData.volume_24h_usdt / 1e6).toFixed(1)}M` : 'n/a';
          const cap = s.meta?.marketData?.market_cap ? `$${(s.meta.marketData.market_cap / 1e9).toFixed(2)}B` : 'n/a';
          return `${sym.padEnd(12)} ${s.root_tf.padEnd(5)} • ${dec.padEnd(7)} • TV:${(tv*100).toFixed(0)}% MTF:${(mtf*100).toFixed(0)}% • Vol:${vol} Cap:${cap}`;
        });
        listingBlocks.push([
          `─ ${sym} ─`,
          ...lines
        ].join('\n'));
      }

      // Send blocks
      await bot.sendMessage(config.TELEGRAM_CHAT_ID, firstBlock);
      await bot.sendMessage(config.TELEGRAM_CHAT_ID, secondBlock);
      
      if (listingBlocks.length > 0) {
        for (const blk of listingBlocks) {
          await bot.sendMessage(config.TELEGRAM_CHAT_ID, blk);
        }
      }

      logger.info({ 
        symbolCount: symbols.length, 
        totalAccepts: candidates.length,
        recommended: recommended.length,
        blockCount: 2 + listingBlocks.length 
      }, 'Startup summary sent with recommended signals');
    } catch (err) {
      logger.warn({ err }, 'Failed to send startup summary');
    }
  },

  async sendRootCandleUpdate({ snapshot = [], newRootTfs = [] } = {}) {
    if (!bot) return;
    try {
      const filtered = snapshot.filter(s => newRootTfs.includes(String(s.root_tf)));
      await this.sendStartupSummary({ snapshot: filtered });
    } catch (err) {
      logger.warn({ err }, 'Failed to send root candle update');
    }
  }
};
