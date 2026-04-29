import { MongoBulkWriteError } from 'mongodb';
import { loadConfig } from '../config.js';
import { getLogger } from '../logger.js';
import { getTrades } from '../polymarket/client.js';
import { enrich } from './enricher.js';
import { synthesizeTradeId } from './dedup.js';
import type { Collection } from 'mongodb';
import type { EnrichedWhale, WhaleTier } from '../db/mongo.js';
import type { PolymarketTrade } from '../polymarket/types.js';

const SEEN_SET_MAX_SIZE = 50_000;

export interface PollerState {
  tradesIngestedTotal: number;
  lastPollAt: number | null;
  lastError: string | null;
}

export class TradesPoller {
  private seenIds = new Set<string>();
  private config = loadConfig();
  private log = getLogger();
  private running = false;
  private intervalId: ReturnType<typeof setInterval> | null = null;
  private state: PollerState = {
    tradesIngestedTotal: 0,
    lastPollAt: null,
    lastError: null,
  };

  constructor(
    private tradesCollection: Collection<EnrichedWhale>,
    private onWhale: (whale: EnrichedWhale, rawTrade: PolymarketTrade) => Promise<void>
  ) {}

  getState(): PollerState {
    return { ...this.state };
  }

  async start(): Promise<void> {
    if (this.running) return;
    this.running = true;
    this.log.info('Starting trades poller');
    await this.poll();
    this.intervalId = setInterval(() => this.poll(), this.config.pollIntervalMs);
  }

  async stop(): Promise<void> {
    this.running = false;
    if (this.intervalId) {
      clearInterval(this.intervalId);
      this.intervalId = null;
    }
    this.log.info('Trades poller stopped');
  }

  private async poll(): Promise<void> {
    if (!this.running) return;

    const now = Date.now();
    try {
      const rawTrades = await getTrades({
        limit: this.config.tradesPageLimit,
        takerOnly: true,
        filterType: 'CASH',
        filterAmount: this.config.whaleUsdFloor,
      });

      this.state.lastPollAt = now;
      this.state.lastError = null;

      const newTrades: Array<{ whale: EnrichedWhale; rawTrade: PolymarketTrade }> = [];

      for (const t of rawTrades) {
        const id = synthesizeTradeId(t);
        if (this.seenIds.has(id)) continue;

        const usd = t.size * t.price;
        if (usd < this.config.whaleUsdFloor) continue;

        const enriched = await enrich(t);
        newTrades.push({ whale: enriched, rawTrade: t });
        this.addToSeen(id);
      }

      if (newTrades.length > 0) {
        await this.insertWhales(newTrades);
      }

      const biggest = newTrades.length > 0
        ? Math.max(...newTrades.map((w) => w.whale.usdSize))
        : 0;
      const tier = biggest >= 250_000 ? 'mega'
        : biggest >= 100_000 ? 'large'
        : biggest >= 25_000 ? 'whale'
        : biggest >= 10_000 ? 'mini'
        : 'sub';

      this.log.info({
        count: newTrades.length,
        biggestUsd: biggest,
        tier,
      }, 'Poll complete');

    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      this.state.lastError = msg;
      this.log.error({ err: msg }, 'Poll failed');
    }
  }

  private async insertWhales(items: Array<{ whale: EnrichedWhale; rawTrade: PolymarketTrade }>): Promise<void> {
    if (this.config.intentClassificationEnabled) {
      for (const item of items) {
        await this.onWhale(item.whale, item.rawTrade);
      }
      this.state.tradesIngestedTotal += items.length;
      return;
    }

    try {
      const whales = items.map((item) => item.whale);
      await this.tradesCollection.bulkWrite(
        whales.map(w => ({ insertOne: { document: w } })),
        { ordered: false }
      );
      this.state.tradesIngestedTotal += whales.length;

      for (const item of items) {
        await this.onWhale(item.whale, item.rawTrade);
      }
    } catch (err) {
      if (err instanceof MongoBulkWriteError) {
        const writeErrorsArr = Array.isArray(err.writeErrors) ? err.writeErrors : [err.writeErrors];
        const realErrors = writeErrorsArr.filter((e: any) => e.code !== 11000);
        if (realErrors.length > 0) {
          throw err;
        }
      } else {
        throw err;
      }
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
