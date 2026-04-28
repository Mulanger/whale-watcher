import { loadConfig } from './config.js';
import { getLogger } from './logger.js';
import { connectMongo, closeMongo, isMongoConnected } from './db/mongo.js';
import { ensureIndexes } from './db/indexes.js';
import { connectRedis, closeRedis, isRedisConnected, publishWhale } from './redis/publisher.js';
import { TradesPoller } from './pipeline/trades_poller.js';
import { startRefreshMarketsJob } from './jobs/refresh_markets.js';
import { startRefreshTraderStatsJob } from './jobs/refresh_trader_stats.js';
import { startHealthServer } from './http/health.js';

let shuttingDown = false;
let poller: TradesPoller | null = null;
let refreshMarketsInterval: ReturnType<typeof setInterval> | null = null;
let refreshTradersInterval: ReturnType<typeof setInterval> | null = null;
let healthServer: ReturnType<typeof startHealthServer> | null = null;

async function main(): Promise<void> {
  const config = loadConfig();
  const log = getLogger();

  log.info('Starting whale-watcher...');

  const { trades, markets, traders } = await connectMongo();
  await ensureIndexes(trades, markets, traders);

  await connectRedis();

  poller = new TradesPoller(trades, async (whale) => {
    await publishWhale(whale);
    log.info({
      id: whale._id,
      usd: whale.usdSize,
      tier: whale.tier,
      market: { slug: whale.market.slug },
    }, 'whale');
  });

  await poller.start();

  refreshMarketsInterval = startRefreshMarketsJob(trades, markets);
  refreshTradersInterval = startRefreshTraderStatsJob(trades, traders);

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

  if (refreshMarketsInterval) clearInterval(refreshMarketsInterval);
  if (refreshTradersInterval) clearInterval(refreshTradersInterval);

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

  if (refreshMarketsInterval) clearInterval(refreshMarketsInterval);
  if (refreshTradersInterval) clearInterval(refreshTradersInterval);

  await Promise.all([closeMongo(), closeRedis()]);

  if (healthServer) healthServer.close();

  log.info('Shutdown complete');
  process.exit(0);
});

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
