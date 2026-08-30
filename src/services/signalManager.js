const dbModule = require('../db');
const wsManager = require('./bybitWs');
const macd = require('./macd');
const telegram = require('./telegram');
const tradeManager = require('./tradeManager');
const marketData = require('./marketData');
const config = require('../config');
const logger = require('pino')();

// Controls whether opening live trades is allowed; initially true but poller will disable on start and enable at first boundary.
let openTradesAllowed = true;

// Exported setter so poller can enable/disable opening trades
function setOpenTradesAllowed(v) {
  openTradesAllowed = !!v;
  logger.info({ openTradesAllowed }, 'signalManager: openTradesAllowed set');
}

const inProgress = new Map();

module.exports = {
  start() {
    logger.info('SignalManager started');
  },

  setOpenTradesAllowed, // export setter

  async handleRootSignal({ symbol, root_tf, detected_at }) {
    const key = `${symbol}:${root_tf}`;
    if (inProgress.has(key)) return;
    inProgress.set(key, true);
    try {
      logger.info({ symbol, root_tf }, 'Root signal received');

      // Always fetch market data and proceed — do NOT early-return on filters.
      const mdata = await marketData.updateSymbolMarketData(symbol);

      // Subscribe to MTF websockets (for alignment updates)
      wsManager.subscribeSymbolMTF(symbol, config.MTF_TFS);
      const alignment = await this.evaluateMtfAlignment(symbol);
      const accept = await this.applyDecision(alignment);

      // Always send Telegram signal blocks (do not gate this on market filters).
      await telegram.sendRootSignalBlock({ symbol, root_tf, alignment, detected_at, accept, marketData: mdata });

      // Only when decision is 'accept' do we attempt to open a trade.
      // Opening trades are further gated by:
      //  - config.OPENTRADE
      //  - the openTradesAllowed flag (we disable until first aligned boundary)
      //  - market filters (MIN_MARKET_CAP, MIN_24H_USDT_VOLUME, MIN_24H_VOLUME_CHANGE_PCT)
      if (accept && accept.decision === 'accept') {
        if (!config.OPENTRADE) {
          logger.info({ symbol }, 'Accept but OPENTRADE disabled; skipping openTrade');
        } else if (!openTradesAllowed) {
          logger.info({ symbol }, 'Accept but open trades not yet enabled (waiting for first boundary)');
        } else {
          // Apply market-level filters now (these decide whether to open trades).
          let passFilters = true;
          if (config.MIN_MARKET_CAP > 0) {
            if (!mdata.market_cap || Number(mdata.market_cap) < config.MIN_MARKET_CAP) {
              passFilters = false;
              logger.info({ symbol, market_cap: mdata.market_cap }, 'Filtered out by MIN_MARKET_CAP (for opening only)');
            }
          }
          if (config.MIN_24H_USDT_VOLUME > 0) {
            if (!mdata.volume_24h_usdt || Number(mdata.volume_24h_usdt) < config.MIN_24H_USDT_VOLUME) {
              passFilters = false;
              logger.info({ symbol, volume_24h_usdt: mdata.volume_24h_usdt }, 'Filtered out by MIN_24H_USDT_VOLUME (for opening only)');
            }
          }
          if (isFinite(config.MIN_24H_VOLUME_CHANGE_PCT)) {
            const change = mdata.volume_change_pct;
            if (change === null) {
              if (config.MIN_24H_VOLUME_CHANGE_PCT > 0) {
                passFilters = false;
                logger.info({ symbol }, 'No previous volume to compute change; filtered by MIN_24H_VOLUME_CHANGE_PCT (for opening only)');
              }
            } else {
              if (change < config.MIN_24H_VOLUME_CHANGE_PCT) {
                passFilters = false;
                logger.info({ symbol, volume_change_pct: change }, 'Filtered out by MIN_24H_VOLUME_CHANGE_PCT (for opening only)');
              }
            }
          }

          if (passFilters) {
            await tradeManager.openTrade({ symbol, root_tf, alignment });
          } else {
            logger.info({ symbol }, 'Decision accepted but market filters prevented opening a trade');
          }
        }
      }
    } catch (err) {
      logger.error({ err, symbol, root_tf }, 'handleRootSignal error');
    } finally {
      setTimeout(() => inProgress.delete(key), 60 * 60 * 1000);
    }
  },

  async evaluateMtfAlignment(symbol) {
    const result = {};
    for (const tf of config.MTF_TFS) {
      const hist = await macd.computeMacdHistogram(symbol, tf);
      if (!hist || hist.length === 0) {
        result[tf] = { ok: false };
        continue;
      }
      const last = hist[hist.length - 1];
      const prev = hist[hist.length - 2] || last;
      result[tf] = {
        histogram: last.histogram,
        macd: last.MACD,
        signal: last.signal,
        rising: last.histogram > prev.histogram,
        positive: last.histogram > 0
      };
    }
    return result;
  },

  async applyDecision(alignment) {
    const tfList = Object.keys(alignment);
    let allPositive = tfList.every(tf => alignment[tf].positive);
    if (allPositive) return { decision: 'accept', reason: 'all_positive' };
    const negatives = tfList.filter(tf => !alignment[tf].positive);
    if (negatives.length === 1 && negatives[0].toUpperCase() === 'D') {
      const d = alignment['D'];
      if (d && d.rising) return { decision: 'accept', reason: 'daily_rising' };
      return { decision: 'monitor', reason: 'daily_not_rising' };
    }
    if (negatives.length >= 1) {
      return { decision: 'monitor', reason: 'some_negative' };
    }
    return { decision: 'reject', reason: 'unknown' };
  }
};
