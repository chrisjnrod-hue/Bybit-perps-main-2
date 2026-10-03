async runBoundaryScanOnce() {
  clearExpiredProcessedSignalKeys();

  const boundary = new Date();

  logger.info(
    {
      boundary: boundary.toISOString()
    },
    'poller.loop2: starting five-minute boundary scan (mid-candle detection via WS)'
  );

  const db = dbModule.get();

  const rows = db
    .prepare(
      `
        SELECT symbol
        FROM symbols
        ORDER BY symbol COLLATE NOCASE ASC
      `
    )
    .all();

  const validRows = rows.filter((row) =>
    isUsdtSymbol(row.symbol)
  );

  const rootTfs = buildRootTfs();
  const newSignals = [];
  const alignmentAlerts = [];

  const latestSignals = dbModule.getLatestSignalsSnapshot() || [];

  for (const row of validRows) {
    const symbol = row.symbol;

    for (const tf of rootTfs) {
      try {
        let latestOpen = getLatestOpenTime(
          db,
          symbol,
          tf
        );

        if (latestOpen === null) {
          await this.seedKlinesForSymbol(symbol, tf);
          latestOpen = getLatestOpenTime(db, symbol, tf);
        }

        if (latestOpen === null) {
          continue;
        }

        const processedStateKey =
          this.getLoop2ProcessedCandleKey(
            symbol,
            tf
          );

        const processedOpen = Number(
          dbModule.getState(
            processedStateKey
          ) || 0
        );

        // New root candle already handled by root-candle loop.
        if (latestOpen > processedOpen) {
          logger.debug(
            {
              symbol,
              tf,
              latestOpen,
              processedOpen
            },
            'poller.loop2: new root candle detected; skipping mid-candle detection for this root candle'
          );
          continue;
        }

        const activeSignals = latestSignals.filter((signal) => {
          return (
            signal.symbol === symbol &&
            normalizeRootTf(signal.root_tf) === tf
          );
        });

        const isAlreadyInSummary = activeSignals.length > 0;

        if (!isAlreadyInSummary) {
          const midCandleStateKey =
            `poller.loop2.midCandle.${symbol}.${tf}.${latestOpen}`;

          const midCandleAlreadyReported =
            dbModule.getState(midCandleStateKey);

          if (!midCandleAlreadyReported) {
            const midCandleFlip =
              await detectMidCandleFlip(
                symbol,
                tf
              );

            if (midCandleFlip) {
              const mtfValidation = await validateMtfAlignmentConsensus(symbol);

              const eventId = buildEventId(
                'midcandle',
                symbol,
                tf,
                latestOpen
              );

              const midSignalCandidate = {
                symbol,
                root_tf: tf,
                detected_at: Date.now(),
                candle_open_time: latestOpen,
                eventId,
                signalType: 'midcandle_update',
                notifyImmediately: true
              };

              // 1) Do not duplicate startup-batch signals
              if (isStartupBatchSignal(midSignalCandidate, 'midcandle')) {
                logger.debug(
                  {
                    symbol,
                    tf,
                    latestOpen,
                    eventId
                  },
                  'poller.loop2: skipping startup-batch duplicate mid-candle signal'
                );
                continue;
              }

              // 2) Always process new mid-candle root flips, even when MTF is not aligned
              //    decision is set based on mtf validation below
              if (!alreadyProcessedSignal(midSignalCandidate, 'midcandle')) {
                const signal =
                  await signalManager.handleRootSignal(midSignalCandidate);

                if (signal) {
                  const decision =
                    mtfValidation.isAligned
                      ? 'accept'
                      : 'monitor';

                  const finalSignal = {
                    ...signal,
                    eventId,
                    notificationType: 'midcandle_update',
                    signalType: 'midcandle_update',
                    state: decision,
                    meta: {
                      ...(signal.meta || {}),
                      mtfScore: mtfValidation.mtfScore,
                      mtfAligned: mtfValidation.isAligned,
                      alignment: mtfValidation.alignment || {},
                      decision,
                      acceptReason: mtfValidation.isAligned
                        ? 'mtf_alignment_met'
                        : 'mtf_alignment_monitor',
                      tvScore: 0,
                      tvSource: 'loop2'
                    }
                  };

                  newSignals.push(finalSignal);

                  logger.info(
                    {
                      symbol,
                      tf,
                      latestOpen,
                      eventId,
                      mtfScore: (mtfValidation.mtfScore * 100).toFixed(0) + '%',
                      decision,
                      mtfAligned: mtfValidation.isAligned
                    },
                    'poller.loop2: mid-candle histogram flip detected and enqueued with decision tag'
                  );
                }

                dbModule.setState(
                  midCandleStateKey,
                  Date.now()
                );
              }
            }
          }
        } else {
          // 3) Existing active signal path remains monitoring-only, not signal detection
          const activeSignal = activeSignals[0];
          const mtfValidation = await validateMtfAlignmentConsensus(symbol);

          const alignmentStateKey =
            `poller.loop2.alignment.${symbol}.${tf}`;

          const previousAlignedState =
            dbModule.getState(
              alignmentStateKey
            );

          const previousAligned =
            previousAlignedState === 'true' ||
            previousAlignedState === true;

          const isAllPositive =
            mtfValidation.totalCount > 0 &&
            mtfValidation.positiveCount === mtfValidation.totalCount;

          const shouldAlert =
            isAllPositive &&
            !previousAligned;

          if (shouldAlert) {
            const alertSignalCandidate = {
              ...activeSignal,
              symbol,
              root_tf: tf,
              detected_at: Date.now(),
              eventId: buildEventId('mtf_align', symbol, tf, Date.now()),
              signalType: 'mtf_alignment',
              notificationType: 'mtf_alignment',
              state: 'monitor',
              meta: {
                ...(activeSignal.meta || {}),
                alignment: mtfValidation.alignment || {},
                decision: 'monitor',
                acceptReason: 'mtf_alignment_alert',
                tvScore: 0,
                tvSource: 'loop2',
                mtfScore: mtfValidation.mtfScore
              }
            };

            if (!alreadyProcessedSignal(alertSignalCandidate, 'alignment') && !isStartupBatchSignal(alertSignalCandidate, 'alignment')) {
              dbModule.setState(
                alignmentStateKey,
                String(true)
              );

              const alertEventId = buildEventId('mtf_align', symbol, tf, Date.now());

              alignmentAlerts.push({
                ...activeSignal,
                symbol,
                root_tf: tf,
                detected_at: Date.now(),
                eventId: alertEventId,
                signalType: 'mtf_alignment',
                notificationType: 'mtf_alignment',
                state: 'monitor',
                meta: {
                  ...(activeSignal.meta || {}),
                  alignment: mtfValidation.alignment || {},
                  decision: 'monitor',
                  acceptReason: 'mtf_alignment_alert',
                  tvScore: 0,
                  tvSource: 'loop2',
                  mtfScore: mtfValidation.mtfScore
                }
              });

              logger.info(
                {
                  symbol,
                  tf,
                  mtfScore: (mtfValidation.mtfScore * 100).toFixed(0) + '%',
                  positiveCount: mtfValidation.positiveCount,
                  totalCount: mtfValidation.totalCount
                },
                'poller.loop2: MTF alignment alert created for existing active signal (100% consensus met)'
              );
            }
          } else if (!isAllPositive && previousAligned) {
            dbModule.setState(
              alignmentStateKey,
              String(false)
            );

            logger.info(
              {
                symbol,
                tf,
                mtfScore: (mtfValidation.mtfScore * 100).toFixed(0) + '%'
              },
              'poller.loop2: MTF alignment dropped below 100%'
            );
          } else if (previousAlignedState === undefined) {
            dbModule.setState(
              alignmentStateKey,
              String(false)
            );
          }
        }
      } catch (err) {
        logger.debug(
          {
            err,
            symbol,
            tf
          },
          'poller.loop2: symbol/timeframe scan failed'
        );
      }
    }
  }

  for (const signal of newSignals) {
    try {
      if (isStartupBatchSignal(signal, signal.signalType || 'midcandle_update')) {
        logger.debug(
          {
            symbol: signal.symbol,
            root_tf: signal.root_tf,
            eventId: signal.eventId
          },
          'poller.loop2: skipping startup-batch duplicate midcandle enqueue'
        );
        continue;
      }

      const queued =
        notificationQueue.enqueueSignal(
          signal,
          'midcandle_update'
        );

      logger.info(
        {
          symbol: signal.symbol,
          root_tf: signal.root_tf,
          queued,
          decision: signal.meta && signal.meta.decision
        },
        'poller.loop2: midcandle update enqueued'
      );
    } catch (err) {
      logger.error(
        {
          err,
          signal
        },
        'poller.loop2: FAILED to enqueue midcandle update'
      );
    }
  }

  for (const alert of alignmentAlerts) {
    try {
      const queued =
        notificationQueue.enqueueSignal(
          alert,
          'mtf_alignment'
        );

      logger.info(
        {
          symbol: alert.symbol,
          root_tf: alert.root_tf,
          queued,
          mtfScore: (
            alert.meta.mtfScore * 100
          ).toFixed(0) + '%'
        },
        'poller.loop2: realtime alignment alert enqueued'
      );
    } catch (err) {
      logger.error(
        {
          err,
          alert
        },
        'poller.loop2: FAILED to enqueue alignment alert'
      );
    }
  }

  logger.info(
    {
      newSignals: newSignals.length,
      alignmentAlerts: alignmentAlerts.length
    },
    'poller.loop2: exact five-minute boundary scan completed'
  );
}
