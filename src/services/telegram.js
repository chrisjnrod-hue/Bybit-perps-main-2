/**
 * sendStartupSummary - CRITICAL CHECKS
 */
async sendStartupSummary({ snapshot = [] } = {}) {
  if (!bot) {
    logger.warn('Telegram bot not initialized');
    return;
  }
  
  try {
    logger.info({ snapshotLength: snapshot ? snapshot.length : 0 }, 'sendStartupSummary called');

    // Handle empty snapshot
    if (!snapshot || snapshot.length === 0) {
      logger.warn('Snapshot is empty, sending initializing message');
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
      logger.info('Startup summary sent (initializing state)');
      return;
    }

    logger.info({ symbolCount: snapshot.length }, '✅ Snapshot has signals, building blocks');

    const db = dbModule.get();

    // Build maps
    const bySymbol = {};
    const byTf = {};
    for (const s of snapshot) {
      if (!bySymbol[s.symbol]) bySymbol[s.symbol] = [];
      bySymbol[s.symbol].push(s);
      
      if (!byTf[s.root_tf]) byTf[s.root_tf] = 0;
      byTf[s.root_tf]++;
    }

    logger.info({ 
      symbolCount: Object.keys(bySymbol).length,
      tfCount: Object.keys(byTf).length
    }, 'Building summary blocks');

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

    logger.info('Sending first block (summary)');
    await bot.sendMessage(config.TELEGRAM_CHAT_ID, firstBlock);

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

    logger.info({ acceptCount: candidates.length }, 'Sending second block (recommended)');
    await bot.sendMessage(config.TELEGRAM_CHAT_ID, secondBlock);
    
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

    if (listingBlocks.length > 0) {
      logger.info({ blockCount: listingBlocks.length }, 'Sending listing blocks');
      for (const blk of listingBlocks) {
        await bot.sendMessage(config.TELEGRAM_CHAT_ID, blk);
      }
    }

    logger.info({ 
      symbolCount: symbols.length, 
      totalAccepts: candidates.length,
      recommended: recommended.length,
      blockCount: 2 + listingBlocks.length 
    }, '✅ Startup summary sent completely with all blocks');
  } catch (err) {
    logger.error({ err }, '❌ Failed to send startup summary');
  }
}
