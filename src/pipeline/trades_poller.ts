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

      const candidates: Array<{ id: string; rawTrade: PolymarketTrade }> = [];
      const idsInPoll = new Set<string>();

      for (const t of rawTrades) {
        const id = synthesizeTradeId(t);
        if (this.seenIds.has(id) || idsInPoll.has(id)) continue;

        const usd = t.size * t.price;
        if (usd < this.config.whaleUsdFloor) continue;

        candidates.push({ id, rawTrade: t });
        idsInPoll.add(id);
      }

      const existingIds = await this.findExistingIds(candidates.map((candidate) => candidate.id));
      for (const id of existingIds) {
        this.addToSeen(id);
      }

      const newTrades: Array<{ whale: EnrichedWhale; rawTrade: PolymarketTrade }> = [];
      for (const candidate of candidates) {
        if (existingIds.has(candidate.id)) continue;

        const enriched = await enrich(candidate.rawTrade);
        newTrades.push({ whale: enriched, rawTrade: candidate.rawTrade });
      }

      if (newTrades.length > 0) {
        const handledIds = await this.insertWhales(newTrades);
        for (const id of handledIds) {
          this.addToSeen(id);
        }
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

  private async findExistingIds(ids: string[]): Promise<Set<string>> {
    if (ids.length === 0) return new Set();

    const existing = await this.tradesCollection
      .find(
        { _id: { $in: ids } },
        { projection: { _id: 1 } }
      )
      .toArray();

    return new Set(existing.map((doc) => doc._id));
  }

  private async insertWhales(items: Array<{ whale: EnrichedWhale; rawTrade: PolymarketTrade }>): Promise<string[]> {
    const handledIds: string[] = [];

    if (this.config.intentClassificationEnabled) {
      for (const item of items) {
        await this.onWhale(item.whale, item.rawTrade);
        handledIds.push(item.whale._id);
      }
      this.state.tradesIngestedTotal += items.length;
      return handledIds;
    }

    for (const item of items) {
      try {
        await this.tradesCollection.insertOne(item.whale);
        await this.onWhale(item.whale, item.rawTrade);
        this.state.tradesIngestedTotal += 1;
      } catch (err) {
        if (!isDuplicateKeyError(err)) throw err;
      }
      handledIds.push(item.whale._id);
    }

    return handledIds;
  }

  private addToSeen(id: string): void {
    if (this.seenIds.size >= SEEN_SET_MAX_SIZE) {
      const arr = Array.from(this.seenIds);
      this.seenIds = new Set(arr.slice(Math.floor(arr.length / 2)));
    }
    this.seenIds.add(id);
  }
}

function isDuplicateKeyError(err: unknown): boolean {
  return typeof err === 'object'
    && err !== null
    && 'code' in err
    && (err as { code?: number }).code === 11000;
}
