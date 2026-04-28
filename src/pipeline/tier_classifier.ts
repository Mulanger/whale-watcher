import type { WhaleTier } from '../db/mongo.js';

export function classifyTier(usd: number): WhaleTier {
  if (usd >= 250_000) return 'mega';
  if (usd >= 100_000) return 'large';
  if (usd >= 25_000) return 'whale';
  if (usd >= 10_000) return 'mini';
  return 'sub';
}
