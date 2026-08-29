const fetch = require('node-fetch');
const bybit = require('./bybitRest');
const dbModule = require('../db');
const config = require('../config');
const logger = require('pino')();

const COINGECKO_BASE = 'https://api.coingecko.com/api/v3';

async function fetchCoinGeckoMarketCapBySymbol(baseSymbol) {
  try {
    const q = encodeURIComponent(baseSymbol);
    const res = await fetch(`${COINGECKO_BASE}/search?query=${q}`);
    if (!res.ok) return null;
    const j = await res.json();
    if (!j || !j.coins || j.coins.length === 0) return null;
    const found = j.coins.find(c => (c.symbol || '').toLowerCase() === baseSymbol.toLowerCase()) || j.coins[0];
    if (!found || !found.id) return null;
    const id = found.id;
    const marketsRes = await fetch(`${COINGECKO_BASE}/coins/markets?vs_currency=usd&ids=${encodeURIComponent(id)}`);
    if (!marketsRes.ok) return null;
    const m = await marketsRes.json();
    if (!m || !m.length) return null;
    return m[0].market_cap || null;
  } catch (err) {
    logger.debug({ err, baseSymbol }, 'CoinGecko market cap fetch failed');
    return null;
  }
}

module.exports = {
  async updateSymbolMarketData(symbol) {
    const db = dbModule.get();
    const ticker = await bybit.fetchTicker24h(symbol);
    let lastPrice = 0;
    let baseVolume = 0;
    if (ticker) {
      lastPrice = Number(ticker.last_price || ticker.last || ticker.last_trade_price || ticker.close || ticker.price || 0);
      baseVolume = Number(ticker.volume || ticker.volume_24h || ticker.turnover_24h || 0);
    }
    const volume_24h_usdt = (baseVolume && lastPrice) ? Number((baseVolume * lastPrice).toFixed(8)) : 0;
    const row = db.prepare('SELECT volume_24h FROM symbols WHERE symbol = ?').get(symbol);
    const last_stored = row ? (row.volume_24h || 0) : 0;
    const update = db.prepare('UPDATE symbols SET price = ?, prev_volume_24h = ?, volume_24h = ?, volume_24h_updated_at = ? WHERE symbol = ?');
    const now = Date.now();
    update.run(lastPrice, last_stored, volume_24h_usdt, now, symbol);
    let market_cap = null;
    if (config.COINGECKO_ENABLED) {
      const base = symbol.replace(/USDT$/i, '');
      market_cap = await fetchCoinGeckoMarketCapBySymbol(base);
      try {
        db.prepare('UPDATE symbols SET market_cap = ? WHERE symbol = ?').run(market_cap, symbol);
      } catch (e) {}
    } else {
      const existing = db.prepare('SELECT market_cap FROM symbols WHERE symbol = ?').get(symbol);
      market_cap = existing ? existing.market_cap : null;
    }
    let volume_change_pct = null;
    if (last_stored && last_stored > 0) {
      volume_change_pct = ((volume_24h_usdt - last_stored) / Math.abs(last_stored)) * 100;
    } else {
      volume_change_pct = null;
    }
    return {
      price: lastPrice,
      base_volume_24h: baseVolume,
      volume_24h_usdt,
      prev_volume_24h: last_stored,
      volume_change_pct,
      market_cap
    };
  }
};