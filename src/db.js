/**
 * SQLite wrapper to persist symbols, klines, signals, trades, market_data.
 * Adds missing columns on existing DB (migration).
 * FIXED: Robust error handling, connection validation, auto-reconnect
 */
const path = require('path');
const fs = require('fs');
const Database = require('better-sqlite3');
const logger = require('pino')();

let db;
let dbPath;
let isInitialized = false;

module.exports = {
  init() {
    dbPath = process.env.DB_PATH || path.join(__dirname, '..', 'data', 'db.sqlite');
    const dir = path.dirname(dbPath);
    
    if (!fs.existsSync(dir)) {
      try {
        fs.mkdirSync(dir, { recursive: true });
      } catch (err) {
        logger.error({ err, dir }, 'db.init: failed to create data directory');
        throw err;
      }
    }

    try {
      db = new Database(dbPath, { timeout: 5000 });
      
      // Test connection immediately
      db.prepare('SELECT 1').get();
      
      db.pragma('journal_mode = WAL');
      db.pragma('busy_timeout = 5000');
      
      this._createSchema();
      this._runMigrations();
      
      isInitialized = true;
      logger.info({ dbPath }, 'Database initialized successfully');
    } catch (err) {
      logger.error({ err, dbPath }, 'db.init: failed to initialize database');
      db = null;
      isInitialized = false;
      throw err;
    }
  },

  _createSchema() {
    if (!db) throw new Error('Database not initialized');

    try {
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
        CREATE INDEX IF NOT EXISTS idx_signals_sym_tf_dt ON signals(symbol, root_tf, detected_at);
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
        CREATE TABLE IF NOT EXISTS notification_state (
          key TEXT PRIMARY KEY,
          value TEXT
        );
        CREATE TABLE IF NOT EXISTS market_data (
          symbol TEXT PRIMARY KEY,
          price REAL DEFAULT 0,
          volume_24h_usdt REAL DEFAULT 0,
          volume_change_pct REAL,
          market_cap REAL,
          updated_at INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS market_data_history (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          symbol TEXT,
          price REAL DEFAULT 0,
          volume_24h_usdt REAL DEFAULT 0,
          volume_change_pct REAL,
          market_cap REAL,
          updated_at INTEGER DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_market_data_updated_at ON market_data(updated_at);
        CREATE INDEX IF NOT EXISTS idx_market_data_history_symbol_updated_at ON market_data_history(symbol, updated_at);
      `);
    } catch (err) {
      logger.error({ err }, 'db._createSchema failed');
      throw err;
    }
  },

  _runMigrations() {
    if (!db) throw new Error('Database not initialized');

    try {
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
            if (!String(err).includes('duplicate column name')) {
              logger.warn({ err, column: col.name }, 'Failed to add column to symbols');
            }
          }
        }
      }

      // Migration: ensure market_data table has all columns
      const mdExisting = db.prepare("PRAGMA table_info(market_data)").all().map(r => r.name);
      const mdToAdd = [
        { name: 'price', type: 'REAL DEFAULT 0' },
        { name: 'volume_24h_usdt', type: 'REAL DEFAULT 0' },
        { name: 'volume_change_pct', type: 'REAL' },
        { name: 'market_cap', type: 'REAL' },
        { name: 'updated_at', type: 'INTEGER DEFAULT 0' }
      ];
      
      for (const col of mdToAdd) {
        if (!mdExisting.includes(col.name)) {
          try {
            db.prepare(`ALTER TABLE market_data ADD COLUMN ${col.name} ${col.type}`).run();
            logger.info({ column: col.name }, 'Added column to market_data table');
          } catch (err) {
            if (!String(err).includes('duplicate column name')) {
              logger.warn({ err, column: col.name }, 'Failed to add column to market_data');
            }
          }
        }
      }
    } catch (err) {
      logger.error({ err }, 'db._runMigrations failed');
      throw err;
    }
  },

  isConnected() {
    if (!db || !isInitialized) {
      return false;
    }

    try {
      db.prepare('SELECT 1').get();
      return true;
    } catch (err) {
      logger.warn({ err }, 'db.isConnected: connection test failed');
      return false;
    }
  },

  get() {
    if (!db) {
      logger.warn('db.get: database is null');
      return null;
    }

    if (!this.isConnected()) {
      logger.warn('db.get: database connection is no longer open; attempting auto-reconnect');
      try {
        this.init();
        if (this.isConnected()) {
          logger.info('db.get: auto-reconnect succeeded');
          return db;
        }
      } catch (err) {
        logger.error({ err }, 'db.get: auto-reconnect failed');
        db = null;
        return null;
      }
    }

    return db;
  },

  setState(key, value) {
    const connection = this.get();
    if (!connection) {
      logger.warn({ key }, 'db.setState: no database connection');
      return false;
    }

    try {
      const v = JSON.stringify(value);
      const stmt = connection.prepare('INSERT OR REPLACE INTO notification_state (key, value) VALUES (?, ?)');
      stmt.run(key, v);
      return true;
    } catch (err) {
      logger.warn({ err, key }, 'db.setState failed');
      return false;
    }
  },

  getState(key) {
    const connection = this.get();
    if (!connection) {
      logger.warn({ key }, 'db.getState: no database connection');
      return null;
    }

    try {
      const row = connection.prepare('SELECT value FROM notification_state WHERE key = ?').get(key);
      if (!row || !row.value) return null;
      return JSON.parse(row.value);
    } catch (err) {
      logger.warn({ err, key }, 'db.getState failed');
      return null;
    }
  },

  deleteState(key) {
    const connection = this.get();
    if (!connection) {
      logger.warn({ key }, 'db.deleteState: no database connection');
      return false;
    }

    try {
      const stmt = connection.prepare('DELETE FROM notification_state WHERE key = ?');
      stmt.run(key);
      return true;
    } catch (err) {
      logger.warn({ err, key }, 'db.deleteState failed');
      return false;
    }
  },

  insertSignal({ symbol, root_tf, detected_at = Date.now(), state = 'detected', meta = {} } = {}) {
    const connection = this.get();
    if (!connection) {
      logger.warn({ symbol, root_tf }, 'db.insertSignal: no database connection');
      return null;
    }

    try {
      const metaStr = JSON.stringify(meta || {});
      const stmt = connection.prepare('INSERT INTO signals (symbol, root_tf, detected_at, state, meta) VALUES (?, ?, ?, ?, ?)');
      const result = stmt.run(symbol, root_tf, detected_at, state, metaStr);
      logger.debug({ symbol, root_tf, id: result.lastInsertRowid }, 'Signal inserted into DB');
      return result.lastInsertRowid;
    } catch (err) {
      logger.warn({ err, symbol, root_tf }, 'db.insertSignal failed');
      return null;
    }
  },

  getLatestSignalsSnapshot() {
    const connection = this.get();
    if (!connection) {
      logger.warn('db.getLatestSignalsSnapshot: no database connection');
      return [];
    }

    try {
      const rows = connection.prepare(`
        SELECT DISTINCT
          s1.symbol,
          s1.root_tf,
          s1.detected_at,
          s1.state,
          s1.meta
        FROM signals s1
        WHERE (s1.symbol, s1.root_tf, s1.detected_at) IN (
          SELECT symbol, root_tf, MAX(detected_at)
          FROM signals
          GROUP BY symbol, root_tf
        )
        ORDER BY UPPER(s1.symbol) ASC
      `).all();

      return rows.map(r => {
        let meta = {};
        try {
          meta = r.meta ? JSON.parse(r.meta) : {};
        } catch (e) {
          meta = {};
        }
        return {
          key: `${r.symbol}:${r.root_tf}`,
          symbol: r.symbol,
          root_tf: r.root_tf,
          detected_at: r.detected_at,
          state: r.state,
          meta
        };
      });
    } catch (err) {
      logger.warn({ err }, 'db.getLatestSignalsSnapshot failed');
      return [];
    }
  },

  close() {
    try {
      if (db && typeof db.close === 'function') {
        db.close();
        db = null;
        isInitialized = false;
        logger.info('SQLite database closed via db.close()');
      }
    } catch (err) {
      logger.warn({ err }, 'Error closing SQLite database');
    }
  }
};
