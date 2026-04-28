import { getLogger } from '../logger.js';
import { getMarket } from '../polymarket/client.js';
import type { PolymarketTrade, GammaMarket } from '../polymarket/types.js';
import type { EnrichedWhale } from '../db/mongo.js';
import { classifyTier } from './tier_classifier.js';
import { synthesizeTradeId } from './dedup.js';

const marketCache = new Map<string, { data: GammaMarket | null; expiresAt: number }>();
const CACHE_TTL_MS = 60 * 60 * 1000;

async function getCachedMarketMeta(conditionId: string): Promise<GammaMarket | null> {
  const now = Date.now();
  const cached = marketCache.get(conditionId);

  if (cached && cached.expiresAt > now) {
    return cached.data;
  }

  try {
    const meta = await getMarket(conditionId);
    marketCache.set(conditionId, { data: meta, expiresAt: now + CACHE_TTL_MS });
    return meta;
  } catch (e) {
    getLogger().warn({ conditionId, err: e }, 'Failed to fetch market meta');
    marketCache.set(conditionId, { data: null, expiresAt: now + CACHE_TTL_MS });
    return null;
  }
}

export async function enrich(trade: PolymarketTrade): Promise<EnrichedWhale> {
  const usd = trade.size * trade.price;
  const marketMeta = await getCachedMarketMeta(trade.conditionId);

  return {
    _id: synthesizeTradeId(trade),
    tier: classifyTier(usd),
    side: trade.side,
    outcome: trade.outcome.toUpperCase() as 'YES' | 'NO',
    usdSize: usd,
    shares: trade.size,
    priceCents: Math.round(trade.price * 100),
    timestamp: trade.timestamp,
    ingestedAt: new Date(),
    market: {
      conditionId: trade.conditionId,
      slug: trade.slug,
      title: trade.title,
      icon: trade.icon ?? null,
      category: marketMeta?.category ?? null,
      eventSlug: trade.eventSlug || null,
      yesPriceCents: marketMeta?.yesPrice != null ? Math.round(marketMeta.yesPrice * 100) : null,
      noPriceCents: marketMeta?.noPrice != null ? Math.round(marketMeta.noPrice * 100) : null,
      polymarketUrl: `https://polymarket.com/event/${trade.eventSlug}/${trade.slug}`,
    },
    trader: {
      proxyWallet: trade.proxyWallet,
      pseudonym: trade.pseudonym || null,
      displayName: trade.name || null,
      profileImage: trade.profileImage || null,
      vol30d: null,
      winRate: null,
      tradeCount: null,
    },
    transactionHash: trade.transactionHash,
    raw: trade,
  };
}
