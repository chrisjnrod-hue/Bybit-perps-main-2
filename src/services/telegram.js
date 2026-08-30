const TelegramBot = require('node-telegram-bot-api');
const config = require('../config');
const logger = require('pino')();

let bot = null;
module.exports = {
  init() {
    if (!config.TELEGRAM_BOT_TOKEN) {
      logger.warn('Telegram token not configured; telegram disabled');
      return;
    }
    bot = new TelegramBot(config.TELEGRAM_BOT_TOKEN, { polling: false });
  },

  async sendRootSignalBlock({ symbol, root_tf, alignment, detected_at, accept, marketData }) {
    if (!bot) return;
    const timeStr = new Date(detected_at).toISOString();
    let alignmentLines = Object.entries(alignment).map(([tf, info]) => {
      if (!info || !info.hasOwnProperty('histogram')) return `${tf}: unknown`;
      return `${tf}: ${info.positive ? 'POS' : 'NEG'} hist=${info.histogram.toFixed(6)} ${info.rising ? '↑' : '↓'}`;
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

    const msg = `Root signal: ${symbol} (${root_tf})\nTime: ${timeStr}\nDecision: ${decision}\n\nMTF status:\n${alignmentLines}\n\nMarket data:\n${marketLines}\n\nAccept reason: ${accept?.reason || 'n/a'}`;

    try {
      await bot.sendMessage(config.TELEGRAM_CHAT_ID, msg);
      logger.info({ symbol, root_tf }, 'Telegram message sent with market data');
    } catch (err) {
      logger.warn({ err }, 'Failed to send telegram');
    }
  }
};
