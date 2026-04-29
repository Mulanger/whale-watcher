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

export const GammaMarketSchema = z.object({
  conditionId: z.string(),
  slug: z.string(),
  title: z.string(),
  category: z.string().nullable(),
  eventSlug: z.string().nullable(),
  endDate: z.string().nullable(),
  active: z.boolean().optional(),
  closed: z.boolean().optional(),
  yesPrice: z.number().nullable(),
  noPrice: z.number().nullable(),
  volume24h: z.number().nullable(),
  liquidity: z.number().nullable(),
});

export type GammaMarket = z.infer<typeof GammaMarketSchema>;

export const GammaEventSchema = z.object({
  eventId: z.string(),
  title: z.string(),
  category: z.string().nullable(),
  endDate: z.string().nullable(),
});

export type GammaEvent = z.infer<typeof GammaEventSchema>;

export const UserPositionsSchema = z.object({
  user: z.string(),
  positions: z.array(z.object({
    market: z.string(),
    size: z.number(),
    value: z.number(),
  })).optional(),
});

export type UserPositions = z.infer<typeof UserPositionsSchema>;

export const PositionSchema = z.object({
  proxyWallet: z.string(),
  asset: z.string(),
  conditionId: z.string(),
  size: z.number(),
  avgPrice: z.number(),
  totalBought: z.number(),
  realizedPnl: z.number(),
  outcomeIndex: z.number(),
  outcome: z.string(),
});

export type Position = z.infer<typeof PositionSchema>;

export const PositionsResponseSchema = PositionSchema.array();
