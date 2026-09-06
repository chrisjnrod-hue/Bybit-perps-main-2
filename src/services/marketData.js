// src/services/marketData.js
const fetch = require('node-fetch');
const dbModule = require('../db');
const config = require('../config');
const logger = require('pino')();

const BYBIT_REST_BASE = config.BYBIT_REST_BASE || 'https://api.bybit.com';
const COINGECKO_API_BASE = 'https://api.coingecko.com/api/v3';

module.exports = {
  /**
   * updateSymbolMarketData(symbol)
   * Fetches price, 24h volume (USDT), volume change %, market cap
   * Returns: { price, volume_24h_usdt, volume_change_pct, market_cap }
   */
  async updateSymbolMarketData(symbol) {
    try {
      if (!symbol) {
        logger.debug('updateSymbolMarketData: symbol is empty');
        return {
          price: 0,
          volume_24h_usdt: 0,
          volume_change_pct: null,
          market_cap: null
        };
      }

      // Fetch from Bybit v5 tickers (price + 24h volume in USDT)
      let price = 0;
      let volume24hUsdt = 0;

      try {
        const tickerUrl = `${BYBIT_REST_BASE}/v5/market/tickers?category=linear&symbol=${symbol}`;
        const res = await fetch(tickerUrl, { timeout: 5000 });
        if (res.ok) {
          const json = await res.json();
          if (json.result && json.result.list && json.result.list.length > 0) {
            const ticker = json.result.list[0];
            price = Number(ticker.lastPrice || ticker.last || ticker.close || 0);
            // turnover24h is volume in USDT (quote currency)
            volume24hUsdt = Number(ticker.turnover24h || ticker.volume || 0);
            logger.debug({ symbol, price, volume24hUsdt }, 'Market data fetched from Bybit');
          }
        } else {
          logger.debug({ symbol, status: res.status }, 'Bybit ticker API returned non-ok status');
        }
      } catch (err) {
        logger.debug({ err: err && err.message, symbol }, 'Bybit ticker fetch failed');
      }

      // Fetch from CoinGecko if enabled (volume change % and market cap)
      let volumeChangePct = null;
      let marketCap = null;

      if (config.COINGECKO_ENABLED) {
        try {
          const coinId = this.extractCoinIdFromSymbol(symbol);
          if (coinId) {
            // Use proper include flags for simple/price endpoint
            const cgUrl = `${COINGECKO_API_BASE}/simple/price?ids=${encodeURIComponent(coinId)}&vs_currencies=usd&include_market_cap=true&include_24hr_vol=true&include_24hr_change=true`;
            const res = await fetch(cgUrl, { timeout: 5000 });
            if (res.ok) {
              const json = await res.json();
              if (json && json[coinId]) {
                const data = json[coinId];
                // Extract market cap (usd_market_cap)
                if (typeof data.usd_market_cap !== 'undefined' && data.usd_market_cap !== null) {
                  marketCap = Number(data.usd_market_cap) || null;
                }
                // Extract 24h volume (usd_24h_vol) - not used for percent change but helpful
                if (typeof data.usd_24h_vol !== 'undefined' && data.usd_24h_vol !== null) {
                  // we don't override volume24hUsdt directly because Bybit turnover24h is quote-based
                  // but keep for debugging/consistency if Bybit missed it
                  if (!volume24hUsdt || volume24hUsdt === 0) {
                    volume24hUsdt = Number(data.usd_24h_vol) || volume24hUsdt;
                  }
                }
                // Extract 24h percent change (usd_24h_change)
                if (typeof data.usd_24h_change !== 'undefined' && data.usd_24h_change !== null) {
                  volumeChangePct = Number(data.usd_24h_change) || null;
                }
                logger.debug({ symbol, coinId, volumeChangePct, marketCap }, 'CoinGecko market data fetched');
              }
            } else {
              logger.debug({ symbol, coinId, status: res.status }, 'CoinGecko API returned non-ok status');
            }
          }
        } catch (err) {
          logger.debug({ err: err && err.message, symbol }, 'CoinGecko fetch failed');
        }
      }

      const result = {
        price: price || 0,
        volume_24h_usdt: volume24hUsdt || 0,
        volume_change_pct: volumeChangePct,
        market_cap: marketCap
      };

      // Persist to DB
      try {
        const db = dbModule.get();
        db.prepare('INSERT OR REPLACE INTO market_data (symbol, price, volume_24h_usdt, volume_change_pct, market_cap, updated_at) VALUES (?, ?, ?, ?, ?, ?)')
          .run(symbol, result.price, result.volume_24h_usdt, result.volume_change_pct || null, result.market_cap || null, Date.now());
      } catch (err) {
        logger.debug({ err, symbol }, 'Failed to persist market data to DB');
      }

      return result;
    } catch (err) {
      logger.error({ err, symbol }, 'updateSymbolMarketData: unexpected error');
      return {
        price: 0,
        volume_24h_usdt: 0,
        volume_change_pct: null,
        market_cap: null
      };
    }
  },

  /**
   * getSymbolMarketData(symbol)
   * Retrieves cached market data from DB (without fetching fresh)
   */
  async getSymbolMarketData(symbol) {
    try {
      const db = dbModule.get();
      const row = db.prepare('SELECT price, volume_24h_usdt, volume_change_pct, market_cap FROM market_data WHERE symbol = ?').get(symbol);
      if (row) {
        return {
          price: row.price || 0,
          volume_24h_usdt: row.volume_24h_usdt || 0,
          volume_change_pct: row.volume_change_pct,
          market_cap: row.market_cap
        };
      }
    } catch (err) {
      logger.debug({ err, symbol }, 'getSymbolMarketData error');
    }
    return {
      price: 0,
      volume_24h_usdt: 0,
      volume_change_pct: null,
      market_cap: null
    };
  },

  /**
   * extractCoinIdFromSymbol(symbol)
   * Maps USDT symbol to CoinGecko coin ID
   * E.g., BTCUSDT -> bitcoin, ETHUSDT -> ethereum
   */
  extractCoinIdFromSymbol(symbol) {
    if (!symbol) return null;

    // Remove USDT/USDT.P suffix
    const base = symbol.replace(/USDT[Pp]?$/i, '').toUpperCase();

    // Common mappings
    const coinIdMap = {
      BTC: 'bitcoin',
      ETH: 'ethereum',
      BNB: 'binancecoin',
      XRP: 'ripple',
      ADA: 'cardano',
      SOL: 'solana',
      DOT: 'polkadot',
      DOGE: 'dogecoin',
      AVAX: 'avalanche-2',
      MATIC: 'matic-network',
      LINK: 'chainlink',
      UNI: 'uniswap',
      LTC: 'litecoin',
      BCH: 'bitcoin-cash',
      FIL: 'filecoin',
      ATOM: 'cosmos',
      XLM: 'stellar',
      VET: 'vechain',
      THETA: 'theta-token',
      EOS: 'eos',
      TRON: 'tron',
      IOTA: 'iota',
      NEO: 'neo',
      XMR: 'monero',
      ZEC: 'zcash',
      DASH: 'dash',
      MANA: 'decentraland',
      SAND: 'the-sandbox',
      APE: 'apecoin',
      GMX: 'gmx',
      ARB: 'arbitrum',
      OP: 'optimism',
      BLUR: 'blur',
      JTO: 'jito',
      WLD: 'world-coin'
    };

    return coinIdMap[base] || null;
  }
};
