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

      // Read previously persisted current row (if any) to compute change and fallback market cap
      let prevCurrentRow = null;
      try {
        const db = dbModule.get();
        prevCurrentRow = db.prepare('SELECT price, volume_24h_usdt, volume_change_pct, market_cap, updated_at FROM market_data WHERE symbol = ?').get(symbol);
      } catch (e) {
        prevCurrentRow = null;
      }

      // Fetch from Bybit v5 tickers (price + 24h volume in USDT)
      let price = 0;
      let volume24hUsdt = 0;

      try {
        const tickerUrl = `${BYBIT_REST_BASE}/v5/market/tickers?category=linear&symbol=${symbol}`;
        const res = await fetch(tickerUrl, { timeout: 5000 });
        if (res.ok) {
          const json = await res.json().catch(() => null);
          if (json && json.result && Array.isArray(json.result.list) && json.result.list.length > 0) {
            const ticker = json.result.list[0];
            price = Number(ticker.lastPrice || ticker.last || ticker.close || 0);
            // turnover24h is volume in USDT (quote currency)
            volume24hUsdt = Number(ticker.turnover24h || ticker.volume || 0);
            logger.debug({ symbol, price, volume24hUsdt }, 'Market data fetched from Bybit');
          } else {
            logger.debug({ symbol, body: json }, 'Bybit ticker API responded but unexpected shape');
          }
        } else {
          logger.debug({ symbol, status: res.status }, 'Bybit ticker API returned non-ok status');
        }
      } catch (err) {
        logger.debug({ err: err && err.message, symbol }, 'Bybit ticker fetch failed');
      }

      // Fetch from CoinGecko if enabled (market cap)
      let marketCapFromCg = null;
      // Note: CoinGecko simple/price endpoint does not provide a "volume change %" for volume, only the raw volume number.
      if (config.COINGECKO_ENABLED) {
        try {
          const coinId = this.extractCoinIdFromSymbol(symbol);
          if (coinId) {
            const cgUrl = `${COINGECKO_API_BASE}/simple/price?ids=${encodeURIComponent(coinId)}&vs_currencies=usd&include_market_cap=true&include_24hr_vol=true`;
            const res = await fetch(cgUrl, { timeout: 5000 });
            if (res.ok) {
              const json = await res.json().catch(() => null);
              if (json && json[coinId]) {
                const data = json[coinId];
                if (typeof data.usd_market_cap !== 'undefined' && data.usd_market_cap !== null) {
                  marketCapFromCg = Number(data.usd_market_cap) || null;
                }
                // If Bybit had no volume, fallback to CoinGecko's usd_24h_vol
                if ((!volume24hUsdt || volume24hUsdt === 0) && typeof data.usd_24h_vol !== 'undefined' && data.usd_24h_vol !== null) {
                  volume24hUsdt = Number(data.usd_24h_vol) || volume24hUsdt;
                }
                logger.debug({ symbol, coinId, marketCapFromCg }, 'CoinGecko market data fetched (simple/price)');
              } else {
                logger.debug({ symbol, coinId, body: json }, 'CoinGecko simple/price returned empty for coinId');
              }
            } else {
              logger.debug({ symbol, coinId, status: res.status }, 'CoinGecko API returned non-ok status');
            }
          } else {
            logger.debug({ symbol }, 'extractCoinIdFromSymbol returned null; cannot query CoinGecko for market cap');
          }
        } catch (err) {
          logger.debug({ err: err && err.message, symbol }, 'CoinGecko fetch failed');
        }
      }

      // Compute volume change pct using previous persisted current row (prevCurrentRow) if available
      let computedVolChangePct = null;
      try {
        const prevVol = prevCurrentRow && typeof prevCurrentRow.volume_24h_usdt === 'number'
          ? Number(prevCurrentRow.volume_24h_usdt)
          : (prevCurrentRow && prevCurrentRow.volume_24h_usdt ? Number(prevCurrentRow.volume_24h_usdt) : null);

        if (prevVol && prevVol > 0) {
          computedVolChangePct = ((volume24hUsdt - prevVol) / prevVol) * 100;
        } else {
          // if previous persisted value missing, we cannot compute change yet
          computedVolChangePct = null;
        }
      } catch (e) {
        computedVolChangePct = null;
      }

      // Final chosen volume change: prefer computed change (we don't have a CG-provided volume change %)
      const finalVolumeChangePct = (typeof computedVolChangePct === 'number' && !Number.isNaN(computedVolChangePct))
        ? Number(computedVolChangePct)
        : null;

      // Final market cap: prefer CoinGecko value if present, otherwise fallback to previous persisted market_cap
      const finalMarketCap = (typeof marketCapFromCg === 'number' && !Number.isNaN(marketCapFromCg))
        ? marketCapFromCg
        : (prevCurrentRow && prevCurrentRow.market_cap ? Number(prevCurrentRow.market_cap) : null);

      const result = {
        price: price || 0,
        volume_24h_usdt: volume24hUsdt || 0,
        volume_change_pct: finalVolumeChangePct,
        market_cap: finalMarketCap
      };

      // Persist: keep a current row and append to history table
      try {
        const db = dbModule.get();

        // Ensure tables exist
        try {
          db.prepare(`CREATE TABLE IF NOT EXISTS market_data (
            symbol TEXT PRIMARY KEY,
            price REAL,
            volume_24h_usdt REAL,
            volume_change_pct REAL,
            market_cap REAL,
            updated_at INTEGER
          )`).run();
        } catch (e) { /* ignore */ }

        try {
          db.prepare(`CREATE TABLE IF NOT EXISTS market_data_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            price REAL,
            volume_24h_usdt REAL,
            volume_change_pct REAL,
            market_cap REAL,
            updated_at INTEGER
          )`).run();
        } catch (e) { /* ignore */ }

        // Insert previous current row into history if it exists (so history has previous snapshot)
        try {
          if (prevCurrentRow) {
            db.prepare(`INSERT INTO market_data_history (symbol, price, volume_24h_usdt, volume_change_pct, market_cap, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?)`)
              .run(symbol,
                   prevCurrentRow.price || 0,
                   prevCurrentRow.volume_24h_usdt || 0,
                   prevCurrentRow.volume_change_pct === null ? null : prevCurrentRow.volume_change_pct,
                   prevCurrentRow.market_cap === null ? null : prevCurrentRow.market_cap,
                   prevCurrentRow.updated_at || Date.now());
          }
        } catch (e) {
          logger.debug({ e, symbol }, 'Failed to append previous market_data to history (non-fatal)');
        }

        // Upsert current
        try {
          db.prepare('INSERT OR REPLACE INTO market_data (symbol, price, volume_24h_usdt, volume_change_pct, market_cap, updated_at) VALUES (?, ?, ?, ?, ?, ?)')
            .run(symbol, result.price, result.volume_24h_usdt, result.volume_change_pct === null ? null : result.volume_change_pct, result.market_cap === null ? null : result.market_cap, Date.now());
        } catch (e) {
          logger.debug({ e, symbol }, 'Failed to upsert market_data (non-fatal)');
        }

        // Also append the new snapshot into history for auditing
        try {
          db.prepare(`INSERT INTO market_data_history (symbol, price, volume_24h_usdt, volume_change_pct, market_cap, updated_at)
                      VALUES (?, ?, ?, ?, ?, ?)`)
            .run(symbol, result.price, result.volume_24h_usdt, result.volume_change_pct === null ? null : result.volume_change_pct, result.market_cap === null ? null : result.market_cap, Date.now());
        } catch (e) {
          logger.debug({ e, symbol }, 'Failed to insert market_data_history (non-fatal)');
        }
      } catch (err) {
        logger.debug({ err, symbol }, 'Failed to persist market data to DB (non-fatal)');
      }

      // Debug logs to explain n/a cases
      if (result.market_cap === null) {
        logger.debug({ symbol, COINGECKO_ENABLED: config.COINGECKO_ENABLED }, 'market_cap not available (CoinGecko disabled or no data). Enable COINGECKO_ENABLED=true to fetch market cap.');
      }
      if (result.volume_change_pct === null) {
        logger.debug({ symbol, reason: prevCurrentRow ? 'prev volume is zero or missing' : 'no previous persisted record to compute change yet' }, 'volume_change_pct unavailable (n/a)');
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
