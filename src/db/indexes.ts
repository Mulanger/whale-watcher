import type { Collection } from 'mongodb';
import type { EnrichedWhale, MarketDoc, TraderDoc } from './mongo.js';
import { getLogger } from '../logger.js';

export async function ensureIndexes(
  trades: Collection<EnrichedWhale>,
  markets: Collection<MarketDoc>,
  traders: Collection<TraderDoc>
): Promise<void> {
  const log = getLogger();

  log.info('Ensuring indexes...');

  await trades.createIndexes([
    { key: { timestamp: -1 } },
    { key: { 'market.category': 1, timestamp: -1 } },
    { key: { tier: 1, timestamp: -1 } },
    { key: { 'market.conditionId': 1, timestamp: -1 } },
    { key: { 'trader.proxyWallet': 1, timestamp: -1 } },
    { key: { usdSize: -1 } },
    { key: { ingestedAt: 1 }, expireAfterSeconds: 60 * 60 * 24 * 90 },
  ]);

  await markets.createIndexes([
    { key: { slug: 1 }, unique: true },
    { key: { category: 1, isActive: 1 } },
    { key: { refreshedAt: 1 } },
  ]);

  await traders.createIndexes([
    { key: { vol30d: -1 } },
    { key: { refreshedAt: 1 } },
  ]);

  log.info('Indexes ensured');
}
