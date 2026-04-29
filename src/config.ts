import { z } from 'zod';

const ConfigSchema = z.object({
  nodeEnv: z.enum(['development', 'production', 'test']),
  logLevel: z.enum(['trace', 'debug', 'info', 'warn', 'error', 'fatal']),
  polymarketDataUrl: z.string().url(),
  polymarketGammaUrl: z.string().url(),
  pollIntervalMs: z.number().int().positive(),
  whaleUsdFloor: z.number().positive(),
  tradesPageLimit: z.number().int().positive().max(10000),
  mongoUri: z.string(),
  mongoDb: z.string(),
  redisUrl: z.string(),
  redisChannel: z.string(),
  intentClassificationEnabled: z.boolean(),
  tradeEventsEnabled: z.boolean(),
  tradeEventsUsdFloor: z.number().positive(),
  allTradesIntervalMs: z.number().int().positive(),
  tradeEventsPageLimit: z.number().int().positive().max(10000),
  dailyAggregatorIntervalMs: z.number().int().positive(),
  healthPort: z.number().int().positive(),
});

export type Config = z.infer<typeof ConfigSchema>;

let _config: Config | null = null;

export function loadConfig(): Config {
  if (_config) return _config;

  const raw = {
    nodeEnv: process.env['NODE_ENV'] ?? 'development',
    logLevel: process.env['LOG_LEVEL'] ?? 'info',
    polymarketDataUrl: process.env['POLYMARKET_DATA_URL'] ?? 'https://data-api.polymarket.com',
    polymarketGammaUrl: process.env['POLYMARKET_GAMMA_URL'] ?? 'https://gamma-api.polymarket.com',
    pollIntervalMs: parseInt(process.env['POLL_INTERVAL_MS'] ?? '3000', 10),
    whaleUsdFloor: parseFloat(process.env['WHALE_USD_FLOOR'] ?? '10000'),
    tradesPageLimit: parseInt(process.env['TRADES_PAGE_LIMIT'] ?? '500', 10),
    mongoUri: process.env['MONGO_URI'] ?? '',
    mongoDb: process.env['MONGO_DB'] ?? 'polywatch',
    redisUrl: process.env['REDIS_URL'] ?? 'redis://localhost:6379',
    redisChannel: process.env['REDIS_CHANNEL'] ?? 'whales',
    intentClassificationEnabled: (process.env['INTENT_CLASSIFICATION_ENABLED'] ?? 'false') === 'true',
    tradeEventsEnabled: (process.env['TRADE_EVENTS_ENABLED'] ?? 'false') === 'true',
    tradeEventsUsdFloor: parseFloat(process.env['TRADE_EVENTS_USD_FLOOR'] ?? '1000'),
    allTradesIntervalMs: parseInt(process.env['ALL_TRADES_INTERVAL_MS'] ?? '30000', 10),
    tradeEventsPageLimit: parseInt(process.env['TRADE_EVENTS_PAGE_LIMIT'] ?? '1000', 10),
    dailyAggregatorIntervalMs: parseInt(process.env['DAILY_AGGREGATOR_INTERVAL_MS'] ?? '300000', 10),
    healthPort: parseInt(process.env['HEALTH_PORT'] ?? '8080', 10),
  };

  const result = ConfigSchema.safeParse(raw);
  if (!result.success) {
    throw new Error(`Invalid config: ${result.error.message}`);
  }

  _config = result.data;
  return _config;
}
