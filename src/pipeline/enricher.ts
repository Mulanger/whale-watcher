import { getLogger } from '../logger.js';
import { getMarket } from '../polymarket/client.js';
import type { PolymarketTrade, GammaMarket } from '../polymarket/types.js';
import type { EnrichedWhale } from '../db/mongo.js';
import { classifyTier } from './tier_classifier.js';
import { synthesizeTradeId } from './dedup.js';
import { normalizeOutcome } from './outcome.js';

const marketCache = new Map<string, { data: GammaMarket | null; expiresAt: number }>();
const CACHE_TTL_MS = 60 * 60 * 1000;

export async function enrich(trade: PolymarketTrade): Promise<EnrichedWhale> {
  const usd = trade.size * trade.price;
  const marketMeta = await getCachedMarketMetaForTrade(trade);

  return {
    _id: synthesizeTradeId(trade),
    tier: classifyTier(usd),
    side: trade.side,
    outcome: normalizeOutcome(trade.outcome),
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

async function getCachedMarketMetaForTrade(trade: PolymarketTrade): Promise<GammaMarket | null> {
  const now = Date.now();
  const cacheKey = `${trade.conditionId}:${trade.slug}:${trade.eventSlug}`;
  const cached = marketCache.get(cacheKey);

  if (cached && cached.expiresAt > now) {
    return cached.data;
  }

  try {
    const meta = await getMarket(trade.conditionId, {
      slug: trade.slug,
      eventSlug: trade.eventSlug || undefined,
    });
    marketCache.set(cacheKey, { data: meta, expiresAt: now + CACHE_TTL_MS });
    return meta;
  } catch (e) {
    getLogger().warn({
      conditionId: trade.conditionId,
      slug: trade.slug,
      eventSlug: trade.eventSlug,
      err: e,
    }, 'Failed to fetch market meta');
    marketCache.set(cacheKey, { data: null, expiresAt: now + CACHE_TTL_MS });
    return null;
  }
}
