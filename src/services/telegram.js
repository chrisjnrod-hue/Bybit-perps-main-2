const TELEGRAM_MIN_DELAY_MS = 1100;

module.exports = {
  // ...existing methods...

  async _sendMessage(text, options = {}) {
    if (!bot) return null;

    try {
      return await bot.sendMessage(
        config.TELEGRAM_CHAT_ID,
        text,
        options
      );
    } catch (err) {
      const retryAfter =
        err?.response?.body?.parameters?.retry_after ||
        err?.response?.parameters?.retry_after ||
        0;

      if (err?.response?.statusCode === 429 || retryAfter > 0) {
        const retryMs = Math.max(
          TELEGRAM_MIN_DELAY_MS,
          Number(retryAfter) * 1000
        );

        logger.warn(
          { retryMs, retryAfter },
          'Telegram rate limit reached; retrying'
        );

        await this._sleep(retryMs);

        return bot.sendMessage(
          config.TELEGRAM_CHAT_ID,
          text,
          options
        );
      }

      throw err;
    }
  },

  async sendNewSignalSingleBlock(signal) {
    if (!bot) return;

    const baseMsg = this.buildSignalMessage(signal);

    try {
      await this._sendMessage(baseMsg);
      logger.debug(
        { symbol: signal?.symbol, root_tf: signal?.root_tf },
        'Telegram: signal detail block sent'
      );
    } catch (err) {
      logger.warn(
        { err, symbol: signal?.symbol, root_tf: signal?.root_tf },
        'Telegram: failed to send signal detail block'
      );

      throw err;
    }
  },

  async sendStartupSummary({ snapshot = [] } = {}) {
    if (!bot) return;

    try {
      const signals = Array.isArray(snapshot) ? snapshot : [];

      if (signals.length === 0) {
        logger.warn('Telegram: no signals provided to startup summary');
        return;
      }

      // Existing summary-building code...

      await this._sendMessage(header);
      await this._sleep(TELEGRAM_MIN_DELAY_MS);

      // Do not mutate the caller's snapshot.
      const sortedSignals = [...signals].sort((a, b) => {
        const symbolOrder = (a.symbol || '').localeCompare(
          b.symbol || '',
          undefined,
          { sensitivity: 'base' }
        );

        if (symbolOrder !== 0) return symbolOrder;

        return String(a.root_tf || '').localeCompare(
          String(b.root_tf || ''),
          undefined,
          { numeric: true }
        );
      });

      for (let i = 0; i < sortedSignals.length; i++) {
        const signal = sortedSignals[i];

        try {
          await this.sendNewSignalSingleBlock(signal);
        } catch (err) {
          logger.warn(
            {
              err,
              symbol: signal?.symbol,
              root_tf: signal?.root_tf,
              index: i + 1,
              total: sortedSignals.length
            },
            'Telegram: signal detail block failed'
          );
        }

        await this._sleep(TELEGRAM_MIN_DELAY_MS);
      }

      // Use _sendMessage for the remaining startup-summary messages.
      await this._sendMessage(recHeader);
      await this._sleep(TELEGRAM_MIN_DELAY_MS);

      // Existing recommendation logic, replacing every:
      // await bot.sendMessage(...)
      // with:
      // await this._sendMessage(...)

    } catch (err) {
      logger.error({ err }, 'Telegram: startup summary flow failed');
    }
  }
};
