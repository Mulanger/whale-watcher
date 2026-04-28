import type { Collection } from 'mongodb';
import { loadConfig } from '../config.js';
import { getLogger } from '../logger.js';
import { getMarket } from '../polymarket/client.js';
import type { EnrichedWhale, MarketDoc } from '../db/mongo.js';

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

export async function refreshActiveMarkets(
  trades: Collection<EnrichedWhale>,
  markets: Collection<MarketDoc>
): Promise<void> {
  const config = loadConfig();
  const log = getLogger();

  const cutoff = Math.floor(Date.now() / 1000) - 86400;

  const recent = await trades.aggregate([
    { $match: { timestamp: { $gte: cutoff } } },
    { $group: { _id: '$market.conditionId' } },
  ]).toArray();

  log.info({ count: recent.length }, 'Refreshing active markets');

  for (const { _id: conditionId } of recent) {
    try {
      const m = await getMarket(conditionId);
      await markets.updateOne(
        { _id: conditionId },
        {
          $set: {
            slug: m.slug,
            title: m.title,
            category: m.category,
            eventSlug: m.eventSlug,
            endDate: m.endDate ? new Date(m.endDate) : null,
            yesPriceCents: m.yesPrice != null ? Math.round(m.yesPrice * 100) : null,
            noPriceCents: m.noPrice != null ? Math.round(m.noPrice * 100) : null,
            volume24h: m.volume24h,
            liquidity: m.liquidity,
            isActive: !!(m.active && !m.closed),
            refreshedAt: new Date(),
          },
        },
        { upsert: true }
      );
    } catch (e) {
      log.warn({ conditionId, err: e }, 'Market refresh failed');
    }
    await sleep(150);
  }
}

export function startRefreshMarketsJob(
  trades: Collection<EnrichedWhale>,
  markets: Collection<MarketDoc>
): ReturnType<typeof setInterval> {
  const log = getLogger();
  const run = async () => {
    try {
      await refreshActiveMarkets(trades, markets);
    } catch (err) {
      log.error({ err }, 'Refresh markets job failed');
    }
  };

  run();
  return setInterval(run, 60_000);
}
