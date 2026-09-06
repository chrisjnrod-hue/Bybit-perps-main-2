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
   *
   * Improvements:
   * - If no previous current row, check market_data_history for a previous snapshot so we can compute a volume change earlier.
   * - Try simple/price first; if market cap missing, call /coins/{id} for richer market_data (market cap + total volume).
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

      // If prevCurrentRow missing, try the latest history entry (so we can compute change sooner)
      if (!prevCurrentRow) {
        try {
          const db = dbModule.get();
          const histRow = db.prepare('SELECT price, volume_24h_usdt, volume_change_pct, market_cap, updated_at FROM market_data_history WHERE symbol = ? ORDER BY updated_at DESC LIMIT 1').get(symbol);
          if (histRow) {
            prevCurrentRow = histRow;
            logger.debug({ symbol, fromHistory: true }, 'Using latest market_data_history row as previous snapshot');
          }
        } catch (e) {
          // ignore
        }
      }

      // 1) Fetch from Bybit v5 tickers (price + 24h volume in USDT)
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

      // 2) Try from CoinGecko if enabled (first via simple/price)
      let marketCapFromCg = null;
      let cgTotalVolume = null;
      let cgPriceChange24h = null;
      let coinId = null;

      if (config.COINGECKO_ENABLED) {
        try {
          coinId = this.extractCoinIdFromSymbol(symbol);
          if (coinId) {
            // simple/price first (cheap)
            const simpleUrl = `${COINGECKO_API_BASE}/simple/price?ids=${encodeURIComponent(coinId)}&vs_currencies=usd&include_market_cap=true&include_24hr_vol=true&include_24hr_change=true`;
            const res = await fetch(simpleUrl, { timeout: 5000 });
            if (res.ok) {
              const json = await res.json().catch(() => null);
              if (json && json[coinId]) {
                const data = json[coinId];
                if (typeof data.usd_market_cap !== 'undefined' && data.usd_market_cap !== null) {
                  marketCapFromCg = Number(data.usd_market_cap) || null;
                }
                if (typeof data.usd_24h_vol !== 'undefined' && data.usd_24h_vol !== null) {
                  cgTotalVolume = Number(data.usd_24h_vol) || null;
                  // if Bybit didn't return volume, use CG's volume
                  if ((!volume24hUsdt || volume24hUsdt === 0) && cgTotalVolume) {
                    volume24hUsdt = cgTotalVolume;
                  }
                }
                if (typeof data.usd_24h_change !== 'undefined' && data.usd_24h_change !== null) {
                  cgPriceChange24h = Number(data.usd_24h_change) || null;
                }
                logger.debug({ symbol, coinId, marketCapFromCg, cgTotalVolume }, 'CoinGecko simple/price fetched');
              } else {
                logger.debug({ symbol, coinId, body: json }, 'CoinGecko simple/price missing coinId result');
              }
            } else {
              logger.debug({ symbol, coinId, status: res.status }, 'CoinGecko simple/price HTTP error');
            }

            // If market cap missing from simple/price, do a richer /coins/{id} call
            if (!marketCapFromCg) {
              try {
                const richUrl = `${COINGECKO_API_BASE}/coins/${encodeURIComponent(coinId)}?localization=false&tickers=false&market_data=true&community_data=false&developer_data=false&sparkline=false`;
                const r2 = await fetch(richUrl, { timeout: 7000 });
                if (r2.ok) {
                  const json2 = await r2.json().catch(() => null);
                  if (json2 && json2.market_data) {
                    const md = json2.market_data;
                    if (md.market_cap && typeof md.market_cap.usd !== 'undefined' && md.market_cap.usd !== null) {
                      marketCapFromCg = Number(md.market_cap.usd) || null;
                    }
                    if (md.total_volume && typeof md.total_volume.usd !== 'undefined' && md.total_volume.usd !== null) {
                      if ((!volume24hUsdt || volume24hUsdt === 0) && md.total_volume.usd) {
                        volume24hUsdt = Number(md.total_volume.usd) || volume24hUsdt;
                      } else {
                        cgTotalVolume = Number(md.total_volume.usd) || cgTotalVolume;
                      }
                    }
                    if (typeof md.price_change_percentage_24h !== 'undefined' && md.price_change_percentage_24h !== null) {
                      cgPriceChange24h = Number(md.price_change_percentage_24h) || null;
                    }
                    logger.debug({ symbol, coinId, marketCapFromCg, cgTotalVolume }, 'CoinGecko /coins/{id} market_data fetched');
                  } else {
                    logger.debug({ symbol, coinId, body: json2 }, 'CoinGecko /coins/{id} returned unexpected shape');
                  }
                } else {
                  logger.debug({ symbol, coinId, status: r2.status }, 'CoinGecko /coins/{id} HTTP error');
                }
              } catch (err) {
                logger.debug({ err: err && err.message, symbol, coinId }, 'CoinGecko /coins/{id} fetch failed');
              }
            }
          } else {
            logger.debug({ symbol }, 'No coinId mapping for symbol; cannot query CoinGecko for additional data');
          }
        } catch (err) {
          logger.debug({ err: err && err.message, symbol }, 'CoinGecko attempts failed');
        }
      }

      // 3) Compute volume change pct using previous persisted current row (or history row)
      let computedVolChangePct = null;
      try {
        const prevVol = prevCurrentRow && typeof prevCurrentRow.volume_24h_usdt === 'number'
          ? Number(prevCurrentRow.volume_24h_usdt)
          : (prevCurrentRow && prevCurrentRow.volume_24h_usdt ? Number(prevCurrentRow.volume_24h_usdt) : null);

        if (prevVol && prevVol > 0) {
          computedVolChangePct = ((volume24hUsdt - prevVol) / prevVol) * 100;
        } else {
          computedVolChangePct = null;
        }
      } catch (e) {
        computedVolChangePct = null;
      }

      // Final chosen volume change: prefer computed change
      const finalVolumeChangePct = (typeof computedVolChangePct === 'number' && !Number.isNaN(computedVolChangePct))
        ? Number(computedVolChangePct)
        : null;

      // Final market cap: prefer CoinGecko, otherwise fallback to previous persisted
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

        // Insert previous current row into history if it exists
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

        // Append the new snapshot into history for audit
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
        logger.debug({ symbol, COINGECKO_ENABLED: config.COINGECKO_ENABLED, coinId }, 'market_cap not available (CoinGecko disabled, no mapping, or no data). Enable COINGECKO_ENABLED=true and ensure symbol maps to a coinId to fetch market cap.');
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
