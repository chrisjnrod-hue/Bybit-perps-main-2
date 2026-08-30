/**
 * SQLite wrapper to persist symbols, klines, signals, trades.
 * Adds missing columns on existing DB (migration).
 *
 * Added: close() to allow graceful shutdown.
 */
const path = require('path');
const fs = require('fs');
const Database = require('better-sqlite3');
const logger = require('pino')();

let db;
module.exports = {
  init() {
    const dbPath = process.env.DB_PATH || path.join(__dirname, '..', 'data', 'db.sqlite');
    const dir = path.dirname(dbPath);
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    db = new Database(dbPath);
    db.pragma('journal_mode = WAL');
    db.exec(`
      CREATE TABLE IF NOT EXISTS symbols (
        symbol TEXT PRIMARY KEY,
        base TEXT,
        quote TEXT,
        fetched_at INTEGER
      );
      CREATE TABLE IF NOT EXISTS klines (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT,
        timeframe TEXT,
        open_time INTEGER,
        open REAL, high REAL, low REAL, close REAL, volume REAL,
        UNIQUE(symbol, timeframe, open_time)
      );
      CREATE INDEX IF NOT EXISTS idx_klines_sym_tf_ot ON klines(symbol, timeframe, open_time);
      CREATE TABLE IF NOT EXISTS signals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT,
        root_tf TEXT,
        detected_at INTEGER,
        state TEXT,
        meta TEXT
      );
      CREATE TABLE IF NOT EXISTS trades (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT,
        opened_at INTEGER,
        side TEXT,
        size REAL,
        entry_price REAL,
        tp REAL,
        sl REAL,
        status TEXT,
        meta TEXT
      );
    `);

    // Migration: add additional columns to symbols if not present
    const existing = db.prepare("PRAGMA table_info(symbols)").all().map(r => r.name);
    const toAdd = [
      { name: 'market_cap', type: 'REAL' },
      { name: 'price', type: 'REAL' },
      { name: 'volume_24h', type: 'REAL' },
      { name: 'prev_volume_24h', type: 'REAL' },
      { name: 'volume_24h_updated_at', type: 'INTEGER' }
    ];
    for (const col of toAdd) {
      if (!existing.includes(col.name)) {
        try {
          db.prepare(`ALTER TABLE symbols ADD COLUMN ${col.name} ${col.type}`).run();
          logger.info({ column: col.name }, 'Added column to symbols table');
        } catch (err) {
          logger.warn({ err, column: col.name }, 'Failed to add column (may already exist)');
        }
      }
    }

    logger.info({ dbPath }, 'Database initialized');
  },

  get() { return db; },

  // Close DB connection for graceful shutdown
  close() {
    try {
      if (db && typeof db.close === 'function') {
        db.close();
        logger.info('SQLite database closed via db.close()');
      }
    } catch (err) {
      logger.warn({ err }, 'Error closing SQLite database');
    }
  }
};
