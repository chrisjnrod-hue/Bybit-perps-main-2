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

      // Fetch previous persisted market_data (for computing volume change if needed)
      let prevRow = null;
      try {
        const db = dbModule.get();
        prevRow = db.prepare('SELECT price, volume_24h_usdt, volume_change_pct, market_cap, updated_at FROM market_data WHERE symbol = ?').get(symbol);
      } catch (e) {
        // ignore DB read errors for prev row
        prevRow = null;
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

      // Fetch from CoinGecko if enabled (market cap and optional percent metrics)
      let volumeChangePctFromCg = null;
      let marketCapFromCg = null;

      if (config.COINGECKO_ENABLED) {
        try {
          const coinId = this.extractCoinIdFromSymbol(symbol);
          if (coinId) {
            // simple/price with include flags: returns usd_market_cap, usd_24h_vol, usd_24h_change (price change %)
            const cgUrl = `${COINGECKO_API_BASE}/simple/price?ids=${encodeURIComponent(coinId)}&vs_currencies=usd&include_market_cap=true&include_24hr_vol=true&include_24hr_change=true`;
            const res = await fetch(cgUrl, { timeout: 5000 });
            if (res.ok) {
              const json = await res.json();
              if (json && json[coinId]) {
                const data = json[coinId];
                if (typeof data.usd_market_cap !== 'undefined' && data.usd_market_cap !== null) {
                  marketCapFromCg = Number(data.usd_market_cap) || null;
                }
                if (typeof data.usd_24h_vol !== 'undefined' && data.usd_24h_vol !== null) {
                  // If Bybit turnover is missing, use CG volume
                  if (!volume24hUsdt || volume24hUsdt === 0) {
                    volume24hUsdt = Number(data.usd_24h_vol) || volume24hUsdt;
                  }
                }
                // Note: usd_24h_change from simple/price is price % change; if you want price change, you can map it.
                // We will not override our computed volume change with price-change.
                logger.debug({ symbol, coinId, marketCapFromCg }, 'CoinGecko market data fetched (simple/price)');
              }
            } else {
              logger.debug({ symbol, coinId, status: res.status }, 'CoinGecko API returned non-ok status');
            }
          }
        } catch (err) {
          logger.debug({ err: err && err.message, symbol }, 'CoinGecko fetch failed');
        }
      }

      // Compute volume change pct if we have a previous persisted volume
      let computedVolChangePct = null;
      try {
        const prevVol = prevRow && typeof prevRow.volume_24h_usdt === 'number' ? Number(prevRow.volume_24h_usdt) : (prevRow && prevRow.volume_24h_usdt ? Number(prevRow.volume_24h_usdt) : null);
        if (prevVol && prevVol > 0) {
          computedVolChangePct = ((volume24hUsdt - prevVol) / prevVol) * 100;
        } else {
          computedVolChangePct = null;
        }
      } catch (e) {
        computedVolChangePct = null;
      }

      // Final chosen volume change: prefer CoinGecko-provided volume percent if you ever fetch it (we didn't map a CG volume percent),
      // otherwise use computedVolChangePct
      const finalVolumeChangePct = (typeof volumeChangePctFromCg === 'number' && !Number.isNaN(volumeChangePctFromCg))
        ? Number(volumeChangePctFromCg)
        : (typeof computedVolChangePct === 'number' && !Number.isNaN(computedVolChangePct) ? Number(computedVolChangePct) : null);

      // Final market cap: prefer CoinGecko value if present
      const finalMarketCap = (typeof marketCapFromCg === 'number' && !Number.isNaN(marketCapFromCg)) ? marketCapFromCg : (prevRow && prevRow.market_cap ? Number(prevRow.market_cap) : null);

      const result = {
        price: price || 0,
        volume_24h_usdt: volume24hUsdt || 0,
        volume_change_pct: finalVolumeChangePct,
        market_cap: finalMarketCap
      };

      // Persist to DB
      try {
        const db = dbModule.get();
        db.prepare('INSERT OR REPLACE INTO market_data (symbol, price, volume_24h_usdt, volume_change_pct, market_cap, updated_at) VALUES (?, ?, ?, ?, ?, ?)')
          .run(symbol, result.price, result.volume_24h_usdt, result.volume_change_pct === null ? null : result.volume_change_pct, result.market_cap === null ? null : result.market_cap, Date.now());
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
   */
  extractCoinIdFromSymbol(symbol) {
    if (!symbol) return null;
    const base = symbol.replace(/USDT[Pp]?$/i, '').toUpperCase();

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
