import { MongoBulkWriteError, type Collection } from 'mongodb';
import { loadConfig } from '../config.js';
import { getLogger } from '../logger.js';
import { getTrades } from '../polymarket/client.js';
import { synthesizeTradeId } from './dedup.js';
import { normalizeOutcome } from './outcome.js';
import type { TradeEventDoc } from '../db/mongo.js';

const SEEN_SET_MAX_SIZE = 200_000;

export interface AllTradesPollerState {
  tradeEventsIngestedTotal: number;
  lastPollAt: number | null;
  lastError: string | null;
}

export class AllTradesPoller {
  private seenIds = new Set<string>();
  private config = loadConfig();
  private log = getLogger();
  private running = false;
  private intervalId: ReturnType<typeof setInterval> | null = null;
  private state: AllTradesPollerState = {
    tradeEventsIngestedTotal: 0,
    lastPollAt: null,
    lastError: null,
  };

  constructor(private tradeEventsCollection: Collection<TradeEventDoc>) {}

  getState(): AllTradesPollerState {
    return { ...this.state };
  }

  async start(): Promise<void> {
    if (this.running) return;
    this.running = true;
    this.log.info({
      floor: this.config.tradeEventsUsdFloor,
      intervalMs: this.config.allTradesIntervalMs,
      limit: this.config.tradeEventsPageLimit,
    }, 'Starting all-trades poller');
    await this.poll();
    this.intervalId = setInterval(() => this.poll(), this.config.allTradesIntervalMs);
  }

  async stop(): Promise<void> {
    this.running = false;
    if (this.intervalId) {
      clearInterval(this.intervalId);
      this.intervalId = null;
    }
    this.log.info('All-trades poller stopped');
  }

  private async poll(): Promise<void> {
    if (!this.running) return;
    const now = Date.now();

    try {
      const rawTrades = await getTrades({
        limit: this.config.tradeEventsPageLimit,
        takerOnly: true,
        filterType: 'CASH',
        filterAmount: this.config.tradeEventsUsdFloor,
      });

      this.state.lastPollAt = now;
      this.state.lastError = null;

      const toInsert: TradeEventDoc[] = [];
      for (const t of rawTrades) {
        const id = synthesizeTradeId(t);
        if (this.seenIds.has(id)) continue;

        const usd = t.size * t.price;
        if (usd < this.config.tradeEventsUsdFloor) continue;

        toInsert.push({
          _id: id,
          proxyWallet: t.proxyWallet.toLowerCase(),
          pseudonym: t.pseudonym ?? null,
          side: t.side,
          outcome: normalizeOutcome(t.outcome),
          usdSize: usd,
          shares: t.size,
          priceCents: Math.round(t.price * 100),
          priceMillicents: Math.round(t.price * 10_000),
          conditionId: t.conditionId,
          marketSlug: t.slug,
          category: null,
          timestamp: t.timestamp,
          ingestedAt: new Date(),
          isWhale: usd >= this.config.whaleUsdFloor,
        });
        this.addToSeen(id);
      }

      if (toInsert.length > 0) {
        await this.insertTradeEvents(toInsert);
      }

      this.log.info({ count: toInsert.length }, 'all_trades poll complete');
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      this.state.lastError = msg;
      this.log.error({ err: msg }, 'all_trades poll failed');
    }
  }

  private async insertTradeEvents(events: TradeEventDoc[]): Promise<void> {
    try {
      await this.tradeEventsCollection.bulkWrite(
        events.map((event) => ({ insertOne: { document: event } })),
        { ordered: false }
      );
      this.state.tradeEventsIngestedTotal += events.length;
    } catch (err) {
      if (err instanceof MongoBulkWriteError) {
        const writeErrorsArr = Array.isArray(err.writeErrors) ? err.writeErrors : [err.writeErrors];
        const realErrors = writeErrorsArr.filter((e: { code?: number }) => e.code !== 11000);
        if (realErrors.length > 0) {
          throw err;
        }
        return;
      }
      throw err;
    }
  }

  private addToSeen(id: string): void {
    if (this.seenIds.size >= SEEN_SET_MAX_SIZE) {
      const arr = Array.from(this.seenIds);
      this.seenIds = new Set(arr.slice(Math.floor(arr.length / 2)));
    }
    this.seenIds.add(id);
  }
}
