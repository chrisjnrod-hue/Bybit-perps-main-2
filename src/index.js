require('dotenv').config();
const express = require('express');
const pino = require('pino');
const config = require('./config');
const db = require('./db');
const poller = require('./services/poller');
const wsManager = require('./services/bybitWs');
const signalManager = require('./services/signalManager');
const telegram = require('./services/telegram');
const tradeManager = require('./services/tradeManager');
const debugRoutes = require('./routes/debug');

const logger = pino({ level: config.LOG_LEVEL || 'info' });

const app = express();
app.use(express.json());
app.use('/debug', debugRoutes);

const PORT = config.PORT || 3000;

(async () => {
  try {
    logger.info('Starting app...');
    db.init();
    poller.start();
    wsManager.start();
    signalManager.start();
    telegram.init();
    tradeManager.registerWs(wsManager);

    app.get('/', (req, res) => res.json({ ok: true, version: '0.3.0' }));

    app.listen(PORT, () => {
      logger.info(`Server listening on ${PORT}`);
    });
  } catch (err) {
    logger.error({ err }, 'Failed to start');
    process.exit(1);
  }
})();
