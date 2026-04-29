import type { Collection } from 'mongodb';
import type { EnrichedWhale, IntentDiscardDoc } from '../db/mongo.js';
import { getPositions } from '../polymarket/client.js';
import { getLogger } from '../logger.js';
import { publishWhale } from '../redis/publisher.js';
import { ackPending, popReady, rescheduleForRetry, type PendingWhale } from '../redis/pending_queue.js';
import { classifyIntent, shouldShowInFeed, TransientLagError, type TradeIntent } from './intent_classifier.js';

const MAX_ATTEMPTS = 5;
const BUY_MISSING_POSITION_RETRY_LIMIT = 2;

let stopped = false;

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export function stopIntentWorker(): void {
  stopped = true;
}

export async function runIntentWorker(opts: {
  trades: Collection<EnrichedWhale>;
  intentDiscards: Collection<IntentDiscardDoc>;
}): Promise<void> {
  const log = getLogger();
  stopped = false;
  log.info('Intent worker started');

  while (!stopped) {
    try {
      const items = await popReady(50);
      for (const item of items) {
        await processOne(item, opts.trades, opts.intentDiscards);
        await sleep(150);
      }
    } catch (err) {
      log.error({ err }, 'Intent worker loop error');
    }
    await sleep(2000);
  }

  log.info('Intent worker stopped');
}

async function processOne(
  item: PendingWhale,
  trades: Collection<EnrichedWhale>,
  intentDiscards: Collection<IntentDiscardDoc>
): Promise<void> {
  const log = getLogger();
  const { rawTrade, enrichedDoc } = item.payload;
  const wallet = rawTrade.proxyWallet.toLowerCase();
  const conditionId = rawTrade.conditionId;

  let intent: TradeIntent | null = null;
  let positions;

  try {
    positions = await getPositions({ user: wallet, market: conditionId, sizeThreshold: 0 });
  } catch (err) {
    if (await retryOrExhaust(item, `positions request failed: ${stringifyError(err)}`)) return;
    return;
  }

  const positionAfter = positions.find((p) => p.asset === rawTrade.asset) ?? null;

  if (!positionAfter && rawTrade.side === 'BUY') {
    const retries = item.payload.buyMissingPositionRetries ?? 0;
    if (retries < BUY_MISSING_POSITION_RETRY_LIMIT) {
      item.payload.buyMissingPositionRetries = retries + 1;
      item.payload.attempts += 1;
      await rescheduleForRetry(item.id, 30 * (retries + 1), item.payload);
      log.info(
        { id: item.id, attempt: item.payload.attempts, retry: item.payload.buyMissingPositionRetries },
        'Intent retry due to BUY with missing position'
      );
      return;
    }
    log.warn({ id: item.id }, 'Forcing OPEN intent after BUY missing position retries');
    intent = 'OPEN';
  }

  if (!intent) {
    try {
      intent = classifyIntent(
        { side: rawTrade.side, size: rawTrade.size, asset: rawTrade.asset },
        positionAfter
      );
    } catch (err) {
      if (err instanceof TransientLagError) {
        if (await retryOrExhaust(item, 'positions lag detected')) return;
        return;
      }
      throw err;
    }
  }

  if (shouldShowInFeed(intent)) {
    const whaleIntent = intent as 'OPEN' | 'INCREASE';
    const whaleDoc: EnrichedWhale = { ...enrichedDoc, intent: whaleIntent };
    try {
      await trades.insertOne(whaleDoc);
      await publishWhale(whaleDoc);
      log.info({ id: item.id, intent, usd: whaleDoc.usdSize }, 'whale published');
    } catch (err) {
      const duplicate = typeof err === 'object' && err !== null && 'code' in err && (err as { code?: number }).code === 11000;
      if (!duplicate) throw err;
    }
  } else {
    const discardIntent = intent as 'DECREASE' | 'CLOSE';
    await intentDiscards.insertOne({
      _id: item.id,
      wallet,
      conditionId,
      intent: discardIntent,
      side: rawTrade.side,
      usdSize: rawTrade.size * rawTrade.price,
      timestamp: rawTrade.timestamp,
      discardedAt: new Date(),
    });
    log.debug({ id: item.id, intent, usd: enrichedDoc.usdSize }, 'whale filtered');
  }

  await ackPending(item.id);
}

async function retryOrExhaust(item: PendingWhale, reason: string): Promise<boolean> {
  const log = getLogger();
  const nextAttempt = item.payload.attempts + 1;

  if (nextAttempt > MAX_ATTEMPTS) {
    log.warn({ id: item.id, reason, attempts: item.payload.attempts }, 'Intent retries exhausted; discarding');
    await ackPending(item.id);
    return false;
  }

  const delaySeconds = 30 * nextAttempt;
  const payload = { ...item.payload, attempts: nextAttempt };
  await rescheduleForRetry(item.id, delaySeconds, payload);
  log.info({ id: item.id, attempt: nextAttempt, delaySeconds, reason }, 'Intent retry scheduled');
  return true;
}

function stringifyError(err: unknown): string {
  if (err instanceof Error) return err.message;
  return String(err);
}
