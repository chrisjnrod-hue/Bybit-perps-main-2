// config.js
const parseList = (v) => (v ? v.split(',').map(x => x.trim()) : []);

/* Env boolean helper */
function envBool(name, defaultVal = false) {
  if (typeof process.env[name] === 'undefined') return defaultVal;
  const v = String(process.env[name]).toLowerCase().trim();
  return v === '1' || v === 'true' || v === 'yes';
}

const OPENTRADE = envBool('OPENTRADE', envBool('OPENTRADES', false));

module.exports = {
  BYBIT_REST_BASE: process.env.BYBIT_REST_BASE || 'https://api.bybit.com',
  BYBIT_WS_PUBLIC: process.env.BYBIT_WS_PUBLIC || 'wss://stream.bybit.com/realtime_public',
  BYBIT_API_KEY: process.env.BYBIT_API_KEY,
  BYBIT_API_SECRET: process.env.BYBIT_API_SECRET,
  OPENTRADE,
  OPENTRADES: OPENTRADE, // backward-compatible alias
  TELEGRAM_BOT_TOKEN: process.env.TELEGRAM_BOT_TOKEN,
  TELEGRAM_CHAT_ID: process.env.TELEGRAM_CHAT_ID,
  ROOT_TFS: parseList(process.env.ROOT_TFS || '60,240,D'),
  MTF_TFS: parseList(process.env.MTF_TFS || '5,15,60,240,D'),
  ROOT_MIDSCAN_INTERVAL: Number(process.env.ROOT_MIDSCAN_INTERVAL || 0),
  PAGE_SIZE: Number(process.env.PAGE_SIZE || 50),
  MAX_CONCURRENT_WS: Number(process.env.MAX_CONCURRENT_WS || 20),
  BATCH_WS_SIZE: Number(process.env.BATCH_WS_SIZE || 20),
  SEED_KLINES_LIMIT: Number(process.env.SEED_KLINES_LIMIT || 200),

  SYMBOL_SEED_ALL: envBool('SYMBOL_SEED_ALL', false),
  WS_INITIAL_SCAN_TIMEOUT: Number(process.env.WS_INITIAL_SCAN_TIMEOUT || 10000),
  BYBIT_PAGINATION_LIMIT: Number(process.env.BYBIT_PAGINATION_LIMIT || 1000),

  SEED_CONCURRENCY: Number(process.env.SEED_CONCURRENCY || 6),
  USE_WS: envBool('USE_WS', false),

  MAX_OPEN_TRADES: Number(process.env.MAX_OPEN_TRADES || 3),
  MIN_24H_VOLUME: Number(process.env.MIN_24H_VOLUME || 100000),
  MIN_TV_RATING: Number(process.env.MIN_TV_RATING || 0.6),
  MTF_ALIGNMENT_RATING: Number(process.env.MTF_ALIGNMENT_RATING || 0.6),
  DEFAULT_LEVERAGE: Number(process.env.DEFAULT_LEVERAGE || 3),
  DEFAULT_TP_PERCENT: Number(process.env.DEFAULT_TP_PERCENT || 3),
  DEFAULT_SL_PERCENT: Number(process.env.DEFAULT_SL_PERCENT || 1.5),
  SPREAD_PERCENT: Number(process.env.SPREAD_PERCENT || 0.05),
  SLIPPAGE_PERCENT: Number(process.env.SLIPPAGE_PERCENT || 0.1),

  BREAK_EVEN_MODE: process.env.BREAK_EVEN_MODE || 'off',
  BREAK_EVEN_TRIGGER_PERCENT: Number(process.env.BREAK_EVEN_TRIGGER_PERCENT || 1),
  BREAK_EVEN_PERCENT: Number(process.env.BREAK_EVEN_PERCENT || 0.5),
  TRAILING_LOOKBACK: Number(process.env.TRAILING_LOOKBACK || 3),

  MIN_MARKET_CAP: Number(process.env.MIN_MARKET_CAP || 0),
  MIN_24H_USDT_VOLUME: Number(process.env.MIN_24H_USDT_VOLUME || 0),
  MIN_24H_VOLUME_CHANGE_PCT: Number(process.env.MIN_24H_VOLUME_CHANGE_PCT || -9999),

  COINGECKO_ENABLED: envBool('COINGECKO_ENABLED', false),

  ROOT_SCAN_INTERVAL_SECS: Number(process.env.ROOT_SCAN_INTERVAL_SECS || 0),
  NEW_ROOT_CANDLE_NOTIFY: envBool('NEW_ROOT_CANDLE_NOTIFY', true),

  BREAK_EVEN_ACTIVE: envBool('BREAK_EVEN_ACTIVE', false),
  BREAK_EVEN_TRAILING: envBool('BREAK_EVEN_TRAILING', false),

  SYMBOL_FILTER: process.env.SYMBOL_FILTER || '^[A-Z0-9]+USDT(\\.P)?$',

  CLOSE_LEAST_PROFITABLE_ENABLED: envBool('CLOSE_LEAST_PROFITABLE_ENABLED', false),
  CLOSE_LEAST_PROFITABLE_MINS_BEFORE_BOUNDARY: Number(process.env.CLOSE_LEAST_PROFITABLE_MINS_BEFORE_BOUNDARY || 5),

  EXCLUDE_STABLES: parseList(process.env.EXCLUDE_STABLES || 'USDT,USDC,TUSD,DAI'),
  PORT: Number(process.env.PORT || 3000),
  LOG_LEVEL: process.env.LOG_LEVEL || 'info',
  TELEGRAM_SEND_DELAY_MS: Number(process.env.TELEGRAM_SEND_DELAY_MS || 100),

  PRIORITIZE_ROOT_TFS: envBool('PRIORITIZE_ROOT_TFS', true),
  ROOT_TFS_PRIORITY: parseList(process.env.ROOT_TFS_PRIORITY || '240,D,60'),

  ENFORCE_MTF_FLIP_1D: envBool('ENFORCE_MTF_FLIP_1D', false),
  MTF_FLIP_REQUIREMENT_1D: process.env.MTF_FLIP_REQUIREMENT_1D || '1h,4h,1d',

  ROOT_SCAN_5MIN_BOUNDARY: envBool('ROOT_SCAN_5MIN_BOUNDARY', true),
  ROOT_SCAN_5MIN_BOUNDARY_INTERVAL_MS: Number(process.env.ROOT_SCAN_5MIN_BOUNDARY_INTERVAL_MS || 300000),
  ALIGNMENT_CONFIRMATION_ALERT: envBool('ALIGNMENT_CONFIRMATION_ALERT', true),

  VALIDATE_SIGNAL_FLIP_AT_OPEN: envBool('VALIDATE_SIGNAL_FLIP_AT_OPEN', false),
  SIGNAL_FLIP_OPEN_PRICE_TOLERANCE_PCT: Number(process.env.SIGNAL_FLIP_OPEN_PRICE_TOLERANCE_PCT || 0.5),

  ROOT_CANDLE_OPEN_SCAN_ENABLED: envBool('ROOT_CANDLE_OPEN_SCAN_ENABLED', false),
  ROOT_CANDLE_OPEN_SCAN_INTERVAL_SECS: Number(process.env.ROOT_CANDLE_OPEN_SCAN_INTERVAL_SECS || 60)
};
