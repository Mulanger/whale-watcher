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
    { $sort: { timestamp: -1 } },
    {
      $group: {
        _id: '$market.conditionId',
        slug: { $first: '$market.slug' },
        title: { $first: '$market.title' },
        icon: { $first: '$market.icon' },
        category: { $first: '$market.category' },
        eventSlug: { $first: '$market.eventSlug' },
        yesPriceCents: { $first: '$market.yesPriceCents' },
        noPriceCents: { $first: '$market.noPriceCents' },
      },
    },
  ]).toArray();

  log.info({ count: recent.length }, 'Refreshing active markets');

  for (const market of recent) {
    const conditionId = market._id;
    try {
      const m = await getMarket(conditionId, {
        slug: market.slug ?? undefined,
        eventSlug: market.eventSlug ?? undefined,
      });
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
      await markets.updateOne(
        { _id: conditionId },
        {
          $set: {
            slug: market.slug,
            title: market.title,
            icon: market.icon ?? null,
            category: market.category ?? null,
            eventSlug: market.eventSlug ?? null,
            yesPriceCents: market.yesPriceCents ?? null,
            noPriceCents: market.noPriceCents ?? null,
            volume24h: null,
            liquidity: null,
            isActive: false,
            refreshedAt: new Date(),
          },
        },
        { upsert: true }
      );
      log.debug({ conditionId, slug: market.slug, eventSlug: market.eventSlug, err: e }, 'Market refresh used stored trade metadata');
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
