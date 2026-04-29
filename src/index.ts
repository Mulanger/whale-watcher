import { loadConfig } from './config.js';
import { getLogger } from './logger.js';
import { connectMongo, closeMongo, isMongoConnected } from './db/mongo.js';
import { ensureIndexes } from './db/indexes.js';
import { connectRedis, closeRedis, isRedisConnected, publishWhale } from './redis/publisher.js';
import { TradesPoller } from './pipeline/trades_poller.js';
import { AllTradesPoller } from './pipeline/all_trades_poller.js';
import { pushPending } from './redis/pending_queue.js';
import { runIntentWorker, stopIntentWorker } from './pipeline/intent_worker.js';
import { startRefreshMarketsJob } from './jobs/refresh_markets.js';
import { startRefreshTraderStatsJob } from './jobs/refresh_trader_stats.js';
import { startDailyAggregator, type DailyAggregatorHandle } from './jobs/aggregate_daily_stats.js';
import { startHealthServer } from './http/health.js';

let shuttingDown = false;
let poller: TradesPoller | null = null;
let allTradesPoller: AllTradesPoller | null = null;
let refreshMarketsInterval: ReturnType<typeof setInterval> | null = null;
let refreshTradersInterval: ReturnType<typeof setInterval> | null = null;
let dailyAggregator: DailyAggregatorHandle | null = null;
let healthServer: ReturnType<typeof startHealthServer> | null = null;

async function main(): Promise<void> {
  const config = loadConfig();
  const log = getLogger();

  log.info('Starting whale-watcher...');

  const {
    trades,
    markets,
    traders,
    intentDiscards,
    tradeEvents,
    traderDailyStats,
  } = await connectMongo();
  await ensureIndexes(trades, markets, traders, intentDiscards, tradeEvents, traderDailyStats);

  await connectRedis();

  poller = new TradesPoller(trades, async (whale, rawTrade) => {
    if (config.intentClassificationEnabled) {
      await pushPending(whale._id, {
        rawTrade,
        enrichedDoc: whale,
        attempts: 0,
      }, 60);
      log.info({ id: whale._id, usd: whale.usdSize, tier: whale.tier }, 'whale queued for intent classification');
      return;
    }

    await publishWhale(whale);
    log.info({ id: whale._id, usd: whale.usdSize, tier: whale.tier, market: { slug: whale.market.slug } }, 'whale');
  });

  await poller.start();

  if (config.intentClassificationEnabled) {
    void runIntentWorker({ trades, intentDiscards }).catch((err) => {
      log.error({ err }, 'intent worker crashed');
    });
  }

  refreshMarketsInterval = startRefreshMarketsJob(trades, markets);
  refreshTradersInterval = startRefreshTraderStatsJob(trades, traders);

  if (config.tradeEventsEnabled) {
    allTradesPoller = new AllTradesPoller(tradeEvents);
    await allTradesPoller.start();
    dailyAggregator = startDailyAggregator(tradeEvents, traderDailyStats, trades);
  }

  healthServer = startHealthServer(config.healthPort, () => {
    const state = poller?.getState() ?? {
      tradesIngestedTotal: 0,
      lastPollAt: null,
      lastError: null,
    };
    return {
      mongoConnected: isMongoConnected(),
      redisConnected: isRedisConnected(),
      lastPollAt: state.lastPollAt,
      lastPollAge: state.lastPollAt ? Date.now() - state.lastPollAt : Infinity,
      tradesIngestedTotal: state.tradesIngestedTotal,
      lastError: state.lastError,
      leaderboard: getLeaderboardHealth(config, allTradesPoller, dailyAggregator),
    };
  });

  log.info('Whale-watcher started successfully');
}

process.on('SIGTERM', async () => {
  if (shuttingDown) return;
  shuttingDown = true;
  const log = getLogger();
  log.info('Received SIGTERM, shutting down...');

  if (poller) await poller.stop();
  if (allTradesPoller) await allTradesPoller.stop();
  stopIntentWorker();

  if (refreshMarketsInterval) clearInterval(refreshMarketsInterval);
  if (refreshTradersInterval) clearInterval(refreshTradersInterval);
  if (dailyAggregator) dailyAggregator.stop();

  await Promise.all([closeMongo(), closeRedis()]);

  if (healthServer) healthServer.close();

  log.info('Shutdown complete');
  process.exit(0);
});

process.on('SIGINT', async () => {
  if (shuttingDown) return;
  shuttingDown = true;
  const log = getLogger();
  log.info('Received SIGINT, shutting down...');

  if (poller) await poller.stop();
  if (allTradesPoller) await allTradesPoller.stop();
  stopIntentWorker();

  if (refreshMarketsInterval) clearInterval(refreshMarketsInterval);
  if (refreshTradersInterval) clearInterval(refreshTradersInterval);
  if (dailyAggregator) dailyAggregator.stop();

  await Promise.all([closeMongo(), closeRedis()]);

  if (healthServer) healthServer.close();

  log.info('Shutdown complete');
  process.exit(0);
});

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});

function getLeaderboardHealth(
  config: ReturnType<typeof loadConfig>,
  pollerInstance: AllTradesPoller | null,
  aggregator: DailyAggregatorHandle | null
) {
  if (!config.tradeEventsEnabled) {
    return { enabled: false, ok: true };
  }

  const now = Date.now();
  const allTradesState = pollerInstance?.getState() ?? null;
  const dailyAggregatorState = aggregator?.getState() ?? null;
  const allTradesLastPollAge = allTradesState?.lastPollAt ? now - allTradesState.lastPollAt : Infinity;
  const dailyAggregatorLastRunAge = dailyAggregatorState?.lastRunAt ? now - dailyAggregatorState.lastRunAt : Infinity;
  const allTradesStaleAfterMs = Math.max(config.allTradesIntervalMs * 4, 120_000);
  const dailyAggregatorStaleAfterMs = Math.max(config.dailyAggregatorIntervalMs * 2, 10 * 60_000);
  const ok = Boolean(allTradesState && dailyAggregatorState)
    && allTradesLastPollAge < allTradesStaleAfterMs
    && dailyAggregatorLastRunAge < dailyAggregatorStaleAfterMs
    && !allTradesState?.lastError
    && !dailyAggregatorState?.lastError;

  return {
    enabled: true,
    ok,
    allTrades: {
      lastPollAt: allTradesState?.lastPollAt ?? null,
      lastPollAge: allTradesLastPollAge,
      lastError: allTradesState?.lastError ?? null,
      tradeEventsIngestedTotal: allTradesState?.tradeEventsIngestedTotal ?? 0,
      lastAttempted: allTradesState?.lastAttempted ?? 0,
      lastInserted: allTradesState?.lastInserted ?? 0,
      lastDuplicateSkipped: allTradesState?.lastDuplicateSkipped ?? 0,
      skippedOverlappingPolls: allTradesState?.skippedOverlappingPolls ?? 0,
      staleAfterMs: allTradesStaleAfterMs,
    },
    dailyAggregator: {
      lastRunAt: dailyAggregatorState?.lastRunAt ?? null,
      lastRunAge: dailyAggregatorLastRunAge,
      lastError: dailyAggregatorState?.lastError ?? null,
      lastRowsUpdated: dailyAggregatorState?.lastRowsUpdated ?? 0,
      lastAggregatedDays: dailyAggregatorState?.lastAggregatedDays ?? [],
      running: dailyAggregatorState?.running ?? false,
      staleAfterMs: dailyAggregatorStaleAfterMs,
    },
  };
}
