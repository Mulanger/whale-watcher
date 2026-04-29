import { z } from 'zod';

export const PolymarketTradeSchema = z.object({
  proxyWallet: z.string(),
  side: z.enum(['BUY', 'SELL']),
  asset: z.string(),
  conditionId: z.string(),
  size: z.number(),
  price: z.number(),
  timestamp: z.number(),
  title: z.string(),
  slug: z.string(),
  icon: z.string().nullable(),
  eventSlug: z.string(),
  outcome: z.string(),
  outcomeIndex: z.number(),
  name: z.string().nullable(),
  pseudonym: z.string().nullable(),
  bio: z.string().nullable(),
  profileImage: z.string().nullable(),
  profileImageOptimized: z.string().nullable(),
  transactionHash: z.string(),
});

export type PolymarketTrade = z.infer<typeof PolymarketTradeSchema>;

function nullableNumber(value: unknown): number | null {
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (typeof value === 'string' && value.trim() !== '') {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

function parseOutcomePrice(value: unknown, index: number): number | null {
  if (Array.isArray(value)) return nullableNumber(value[index]);
  if (typeof value !== 'string') return null;

  try {
    const parsed = JSON.parse(value);
    return Array.isArray(parsed) ? nullableNumber(parsed[index]) : null;
  } catch {
    return null;
  }
}

function numberOr(defaultValue: number) {
  return z.preprocess((value) => {
    if (typeof value === 'string' && value.trim() !== '') return Number(value);
    return value;
  }, z.number()).catch(defaultValue);
}

export const GammaMarketSchema = z.object({
  conditionId: z.string(),
  slug: z.string(),
  title: z.string().optional(),
  question: z.string().optional(),
  category: z.string().nullable().optional(),
  eventSlug: z.string().nullable().optional(),
  events: z.array(z.object({
    slug: z.string().optional(),
    category: z.string().nullable().optional(),
  }).passthrough()).optional(),
  endDate: z.string().nullable().optional(),
  active: z.boolean().optional(),
  closed: z.boolean().optional(),
  yesPrice: z.union([z.number(), z.string()]).nullable().optional(),
  noPrice: z.union([z.number(), z.string()]).nullable().optional(),
  outcomePrices: z.unknown().optional(),
  volume24h: z.union([z.number(), z.string()]).nullable().optional(),
  volume24hr: z.union([z.number(), z.string()]).nullable().optional(),
  liquidity: z.union([z.number(), z.string()]).nullable().optional(),
  liquidityNum: z.number().nullable().optional(),
}).passthrough().transform((m) => ({
  conditionId: m.conditionId,
  slug: m.slug,
  title: m.title ?? m.question ?? m.slug,
  category: m.category ?? m.events?.[0]?.category ?? null,
  eventSlug: m.eventSlug ?? m.events?.[0]?.slug ?? null,
  endDate: m.endDate ?? null,
  active: m.active,
  closed: m.closed,
  yesPrice: nullableNumber(m.yesPrice) ?? parseOutcomePrice(m.outcomePrices, 0),
  noPrice: nullableNumber(m.noPrice) ?? parseOutcomePrice(m.outcomePrices, 1),
  volume24h: nullableNumber(m.volume24h) ?? nullableNumber(m.volume24hr),
  liquidity: nullableNumber(m.liquidityNum) ?? nullableNumber(m.liquidity),
}));

export type GammaMarket = z.infer<typeof GammaMarketSchema>;

export const GammaEventSchema = z.object({
  id: z.union([z.string(), z.number()]).optional(),
  eventId: z.union([z.string(), z.number()]).optional(),
  slug: z.string().optional(),
  title: z.string(),
  category: z.string().nullable().optional(),
  endDate: z.string().nullable().optional(),
  markets: z.array(GammaMarketSchema).optional(),
  tags: z.array(z.object({
    label: z.string().optional(),
    slug: z.string().optional(),
  }).passthrough()).optional(),
}).passthrough().transform((event) => ({
  eventId: String(event.eventId ?? event.id ?? ''),
  slug: event.slug ?? null,
  title: event.title,
  category: event.category ?? event.tags?.[0]?.label ?? null,
  endDate: event.endDate ?? null,
  markets: event.markets ?? [],
}));

export type GammaEvent = z.infer<typeof GammaEventSchema>;

export const PositionSchema = z.object({
  proxyWallet: z.string(),
  asset: z.string(),
  conditionId: z.string(),
  size: numberOr(0),
  avgPrice: numberOr(0),
  initialValue: numberOr(0),
  currentValue: numberOr(0),
  cashPnl: numberOr(0),
  totalBought: numberOr(0),
  realizedPnl: numberOr(0),
  curPrice: numberOr(0),
  outcomeIndex: numberOr(-1),
  outcome: z.string().catch(''),
}).passthrough();

export type Position = z.infer<typeof PositionSchema>;

export const PositionsResponseSchema = PositionSchema.array();

export const UserPositionsSchema = PositionsResponseSchema;

export type UserPositions = z.infer<typeof UserPositionsSchema>;
