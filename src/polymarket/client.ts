import { request, Agent } from 'undici';
import { z } from 'zod';
import pRetry from 'p-retry';
import { AbortError } from 'p-retry';
import { loadConfig } from '../config.js';
import { getLogger } from '../logger.js';
import {
  PolymarketTradeSchema,
  GammaMarketSchema,
  GammaEventSchema,
  UserPositionsSchema,
  PositionsResponseSchema,
  type PolymarketTrade,
  type GammaMarket,
  type GammaEvent,
  type UserPositions,
  type Position,
} from './schemas.js';

const agent = new Agent({ keepAliveTimeout: 30_000, connections: 10 });

class RetriableError extends Error {}

async function get<T>(
  baseUrl: string,
  path: string,
  query: Record<string, unknown>,
  schema: z.ZodSchema<T>
): Promise<T> {
  const url = new URL(path, baseUrl);
  for (const [k, v] of Object.entries(query)) {
    if (v != null) url.searchParams.set(k, String(v));
  }

  return pRetry(
    async () => {
      const res = await request(url, { dispatcher: agent, method: 'GET' });
      if (res.statusCode === 429 || res.statusCode >= 500) {
        throw new RetriableError(`status ${res.statusCode}`);
      }
      if (res.statusCode >= 400) {
        throw new AbortError(`status ${res.statusCode}: ${await res.body.text()}`);
      }
      const json = await res.body.json();
      return schema.parse(json);
    },
    { retries: 5, factor: 2, minTimeout: 1000, maxTimeout: 60_000 }
  );
}

export async function getTrades(params: {
  limit?: number;
  offset?: number;
  takerOnly?: boolean;
  filterType?: string;
  filterAmount?: number;
  market?: string;
  side?: string;
}): Promise<PolymarketTrade[]> {
  const config = loadConfig();
  const log = getLogger();

  const query: Record<string, unknown> = {
    limit: params.limit ?? 500,
    offset: params.offset ?? 0,
    takerOnly: params.takerOnly ?? true,
    filterType: params.filterType ?? 'CASH',
    filterAmount: params.filterAmount ?? config.whaleUsdFloor,
  };
  if (params.market) query.market = params.market;
  if (params.side) query.side = params.side;

  log.debug({ query }, 'Fetching trades from Polymarket');
  const result = await get(config.polymarketDataUrl, '/trades', query, PolymarketTradeSchema.array());
  return result;
}

export async function getMarket(conditionId: string): Promise<GammaMarket> {
  const config = loadConfig();
  return get(config.polymarketGammaUrl, `/markets/${conditionId}`, {}, GammaMarketSchema);
}

export async function getEvent(eventId: string): Promise<GammaEvent> {
  const config = loadConfig();
  return get(config.polymarketGammaUrl, `/events/${eventId}`, {}, GammaEventSchema);
}

export async function getUserPositions(user: string): Promise<UserPositions> {
  const config = loadConfig();
  return get(config.polymarketDataUrl, '/positions', { user }, UserPositionsSchema);
}

export async function getPositions(opts: {
  user: string;
  market: string;
  sizeThreshold?: number;
}): Promise<Position[]> {
  const config = loadConfig();
  return get(config.polymarketDataUrl, '/positions', {
    user: opts.user,
    market: opts.market,
    sizeThreshold: opts.sizeThreshold ?? 0,
  }, PositionsResponseSchema);
}
