const parseList = (value) =>
  value
    ? String(value)
        .split(',')
        .map(item => item.trim())
        .filter(Boolean)
    : [];

function envBool(name, defaultValue = false) {
  if (typeof process.env[name] === 'undefined') {
    return defaultValue;
  }

  const value = String(process.env[name]).toLowerCase().trim();

  return value === '1' || value === 'true' || value === 'yes';
}

module.exports = {
  BYBIT_REST_BASE:
    process.env.BYBIT_REST_BASE || 'https://api.bybit.com',

  // Bybit V5 public linear WebSocket endpoint.
  BYBIT_WS_PUBLIC:
    process.env.BYBIT_WS_PUBLIC ||
    'wss://stream.bybit.com/v5/public/linear',

  BYBIT_API_KEY: process.env.BYBIT_API_KEY,
  BYBIT_API_SECRET: process.env.BYBIT_API_SECRET,

  OPENTRADE:
    process.env.OPENTRADE === 'true' ||
    process.env.ENABLE_OPEN_TRADES === 'true',

  TELEGRAM_BOT_TOKEN: process.env.TELEGRAM_BOT_TOKEN,
  TELEGRAM_CHAT_ID: process.env.TELEGRAM_CHAT_ID,

  ROOT_TFS: parseList(process.env.ROOT_TFS || '60,240,D'),
  MTF_TFS: parseList(process.env.MTF_TFS || '5,15,60,240,D'),

  ROOT_MIDSCAN_INTERVAL: Number(
    process.env.ROOT_MIDSCAN_INTERVAL || 0
  ),

  PAGE_SIZE: Number(process.env.PAGE_SIZE || 50),

  SEED_KLINES_LIMIT: Number(
    process.env.SEED_KLINES_LIMIT || 200
  ),

  SEED_CONCURRENCY: Number(
    process.env.SEED_CONCURRENCY || 6
  ),

  SYMBOL_SEED_ALL: envBool(
    'SYMBOL_SEED_ALL',
    false
  ),

  USE_WS: envBool('USE_WS', false),

  WS_INITIAL_SCAN_TIMEOUT: Number(
    process.env.WS_INITIAL_SCAN_TIMEOUT || 10000
  ),

  // WebSocket controls.
  MAX_CONCURRENT_WS: Number(
    process.env.MAX_CONCURRENT_WS || 20
  ),

  BATCH_WS_SIZE: Number(
    process.env.BATCH_WS_SIZE || 10
  ),

  WS_SUBSCRIBE_CHUNK: Number(
    process.env.WS_SUBSCRIBE_CHUNK || 10
  ),

  WS_CONNECT_TIMEOUT_MS: Number(
    process.env.WS_CONNECT_TIMEOUT_MS || 15000
  ),

  WS_RECONNECT_DELAY_MS: Number(
    process.env.WS_RECONNECT_DELAY_MS || 1000
  ),

  WS_RECONNECT_BACKOFF_FACTOR: Number(
    process.env.WS_RECONNECT_BACKOFF_FACTOR || 2
  ),

  WS_RECONNECT_MAX_MS: Number(
    process.env.WS_RECONNECT_MAX_MS || 30000
  ),

  WS_MAX_RECONNECT_ATTEMPTS: Number(
    process.env.WS_MAX_RECONNECT_ATTEMPTS || 10
  ),

  WS_SUBSCRIBE_RETRY_BASE_MS: Number(
    process.env.WS_SUBSCRIBE_RETRY_BASE_MS || 1000
  ),

  WS_SUBSCRIBE_RETRY_MAX_MS: Number(
    process.env.WS_SUBSCRIBE_RETRY_MAX_MS || 60000
  ),

  BYBIT_PAGINATION_LIMIT: Number(
    process.env.BYBIT_PAGINATION_LIMIT || 1000
  ),

  MAX_OPEN_TRADES: Number(
    process.env.MAX_OPEN_TRADES || 3
  ),

  MIN_24H_VOLUME: Number(
    process.env.MIN_24H_VOLUME || 100000
  ),

  MIN_TV_RATING: Number(
    process.env.MIN_TV_RATING || 0.6
  ),

  MTF_ALIGNMENT_RATING: Number(
    process.env.MTF_ALIGNMENT_RATING || 0.6
  ),

  DEFAULT_LEVERAGE: Number(
    process.env.DEFAULT_LEVERAGE || 3
  ),

  DEFAULT_TP_PERCENT: Number(
    process.env.DEFAULT_TP_PERCENT || 3
  ),

  DEFAULT_SL_PERCENT: Number(
    process.env.DEFAULT_SL_PERCENT || 1.5
  ),

  SPREAD_PERCENT: Number(
    process.env.SPREAD_PERCENT || 0.05
  ),

  SLIPPAGE_PERCENT: Number(
    process.env.SLIPPAGE_PERCENT || 0.1
  ),

  BREAK_EVEN_MODE:
    process.env.BREAK_EVEN_MODE || 'off',

  BREAK_EVEN_TRIGGER_PERCENT: Number(
    process.env.BREAK_EVEN_TRIGGER_PERCENT || 1
  ),

  BREAK_EVEN_PERCENT: Number(
    process.env.BREAK_EVEN_PERCENT || 0.5
  ),

  TRAILING_LOOKBACK: Number(
    process.env.TRAILING_LOOKBACK || 3
  ),

  MIN_MARKET_CAP: Number(
    process.env.MIN_MARKET_CAP || 0
  ),

  MIN_24H_USDT_VOLUME: Number(
    process.env.MIN_24H_USDT_VOLUME || 0
  ),

  MIN_24H_VOLUME_CHANGE_PCT: Number(
    process.env.MIN_24H_VOLUME_CHANGE_PCT || -9999
  ),

  COINGECKO_ENABLED:
    process.env.COINGECKO_ENABLED === 'true',

  ROOT_SCAN_INTERVAL_SECS: Number(
    process.env.ROOT_SCAN_INTERVAL_SECS || 0
  ),

  NEW_ROOT_CANDLE_NOTIFY: envBool(
    'NEW_ROOT_CANDLE_NOTIFY',
    true
  ),

  BREAK_EVEN_ACTIVE:
    process.env.BREAK_EVEN_ACTIVE === 'true',

  BREAK_EVEN_TRAILING:
    process.env.BREAK_EVEN_TRAILING === 'true',

  SYMBOL_FILTER:
    process.env.SYMBOL_FILTER ||
    '^[A-Z0-9]+USDT(\\.P)?$',

  CLOSE_LEAST_PROFITABLE_ENABLED: envBool(
    'CLOSE_LEAST_PROFITABLE_ENABLED',
    false
  ),

  CLOSE_LEAST_PROFITABLE_MINS_BEFORE_BOUNDARY: Number(
    process.env.CLOSE_LEAST_PROFITABLE_MINS_BEFORE_BOUNDARY || 5
  ),

  EXCLUDE_STABLES: parseList(
    process.env.EXCLUDE_STABLES ||
    'USDT,USDC,TUSD,DAI'
  ),

  PORT: Number(
    process.env.PORT || 3000
  ),

  LOG_LEVEL:
    process.env.LOG_LEVEL || 'info',

  TELEGRAM_SEND_DELAY_MS: Number(
    process.env.TELEGRAM_SEND_DELAY_MS || 100
  ),

  PRIORITIZE_ROOT_TFS: envBool(
    'PRIORITIZE_ROOT_TFS',
    true
  ),

  ROOT_TFS_PRIORITY: parseList(
    process.env.ROOT_TFS_PRIORITY || '240,D,60'
  ),

  ENFORCE_MTF_FLIP_1D: envBool(
    'ENFORCE_MTF_FLIP_1D',
    false
  ),

  MTF_FLIP_REQUIREMENT_1D:
    process.env.MTF_FLIP_REQUIREMENT_1D ||
    '1h,4h,1d',

  ROOT_SCAN_5MIN_BOUNDARY: envBool(
    'ROOT_SCAN_5MIN_BOUNDARY',
    true
  ),

  ALIGNMENT_CONFIRMATION_ALERT: envBool(
    'ALIGNMENT_CONFIRMATION_ALERT',
    true
  ),

  VALIDATE_SIGNAL_FLIP_AT_OPEN: envBool(
    'VALIDATE_SIGNAL_FLIP_AT_OPEN',
    false
  ),

  SIGNAL_FLIP_OPEN_PRICE_TOLERANCE_PCT: Number(
    process.env.SIGNAL_FLIP_OPEN_PRICE_TOLERANCE_PCT || 0.5
  ),

  ROOT_CANDLE_OPEN_SCAN_ENABLED: envBool(
    'ROOT_CANDLE_OPEN_SCAN_ENABLED',
    false
  ),

  ROOT_CANDLE_OPEN_SCAN_INTERVAL_SECS: Number(
    process.env.ROOT_CANDLE_OPEN_SCAN_INTERVAL_SECS || 60
  )
};
