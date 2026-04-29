import type { EnrichedWhale } from '../db/mongo.js';
import type { PolymarketTrade } from '../polymarket/types.js';
import { getRedis } from './publisher.js';

const PENDING_KEY = 'whale_intent_pending';
const PAYLOAD_PREFIX = 'whale_intent_payload:';
const PAYLOAD_TTL = 60 * 60;

export interface PendingWhalePayload {
  rawTrade: PolymarketTrade;
  enrichedDoc: EnrichedWhale;
  attempts: number;
  buyMissingPositionRetries?: number;
}

export interface PendingWhale {
  id: string;
  payload: PendingWhalePayload;
}

export async function pushPending(
  whaleId: string,
  payload: PendingWhalePayload,
  delaySeconds = 60
): Promise<void> {
  const processAt = Math.floor(Date.now() / 1000) + delaySeconds;
  const redis = await getRedis();

  const pipeline = redis.pipeline();
  pipeline.zadd(PENDING_KEY, processAt, whaleId);
  pipeline.set(`${PAYLOAD_PREFIX}${whaleId}`, JSON.stringify(payload), 'EX', PAYLOAD_TTL);
  await pipeline.exec();
}

export async function popReady(limit = 50): Promise<PendingWhale[]> {
  const now = Math.floor(Date.now() / 1000);
  const redis = await getRedis();
  const ids = await redis.zrangebyscore(PENDING_KEY, '-inf', now, 'LIMIT', 0, limit);
  if (ids.length === 0) return [];

  const payloads = await redis.mget(...ids.map((id) => `${PAYLOAD_PREFIX}${id}`));
  const items: PendingWhale[] = [];

  for (let i = 0; i < ids.length; i++) {
    const raw = payloads[i];
    if (!raw) continue;

    const payload = JSON.parse(raw) as PendingWhalePayload;
    payload.enrichedDoc.ingestedAt = new Date(payload.enrichedDoc.ingestedAt);
    items.push({ id: ids[i], payload });
  }

  return items;
}

export async function ackPending(whaleId: string): Promise<void> {
  const redis = await getRedis();
  const pipeline = redis.pipeline();
  pipeline.zrem(PENDING_KEY, whaleId);
  pipeline.del(`${PAYLOAD_PREFIX}${whaleId}`);
  await pipeline.exec();
}

export async function rescheduleForRetry(
  whaleId: string,
  delaySeconds: number,
  payload?: PendingWhalePayload
): Promise<void> {
  const processAt = Math.floor(Date.now() / 1000) + delaySeconds;
  const redis = await getRedis();
  const pipeline = redis.pipeline();
  pipeline.zadd(PENDING_KEY, processAt, whaleId);
  if (payload) {
    pipeline.set(`${PAYLOAD_PREFIX}${whaleId}`, JSON.stringify(payload), 'EX', PAYLOAD_TTL);
  }
  await pipeline.exec();
}
