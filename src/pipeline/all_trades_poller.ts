import { MongoBulkWriteError, type Collection } from 'mongodb';
import { loadConfig } from '../config.js';
import { getLogger } from '../logger.js';
import { getTrades } from '../polymarket/client.js';
import { synthesizeTradeId } from './dedup.js';
import { normalizeOutcome } from './outcome.js';
import type { TradeEventDoc } from '../db/mongo.js';
import type { PolymarketTrade } from '../polymarket/types.js';

const SEEN_SET_MAX_SIZE = 200_000;

export interface AllTradesPollerState {
  tradeEventsIngestedTotal: number;
  lastPollAt: number | null;
  lastError: string | null;
  lastAttempted: number;
  lastInserted: number;
  lastDuplicateSkipped: number;
  skippedOverlappingPolls: number;
}

export class AllTradesPoller {
  private seenIds = new Set<string>();
  private config = loadConfig();
  private log = getLogger();
  private fetchTrades: typeof getTrades;
  private now: () => Date;
  private running = false;
  private pollInProgress = false;
  private intervalId: ReturnType<typeof setInterval> | null = null;
  private state: AllTradesPollerState = {
    tradeEventsIngestedTotal: 0,
    lastPollAt: null,
    lastError: null,
    lastAttempted: 0,
    lastInserted: 0,
    lastDuplicateSkipped: 0,
    skippedOverlappingPolls: 0,
  };

  constructor(
    private tradeEventsCollection: Collection<TradeEventDoc>,
    opts: { fetchTrades?: typeof getTrades; now?: () => Date } = {}
  ) {
    this.fetchTrades = opts.fetchTrades ?? getTrades;
    this.now = opts.now ?? (() => new Date());
  }

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
    await this.pollOnce();
    this.intervalId = setInterval(() => {
      if (this.running) void this.pollOnce();
    }, this.config.allTradesIntervalMs);
  }

  async stop(): Promise<void> {
    this.running = false;
    if (this.intervalId) {
      clearInterval(this.intervalId);
      this.intervalId = null;
    }
    this.log.info('All-trades poller stopped');
  }

  async pollOnce(): Promise<void> {
    if (this.pollInProgress) {
      this.state.skippedOverlappingPolls += 1;
      this.log.warn('all_trades poll skipped because previous poll is still running');
      return;
    }

    this.pollInProgress = true;

    try {
      const rawTrades = await this.fetchTrades({
        limit: this.config.tradeEventsPageLimit,
        takerOnly: true,
        filterType: 'CASH',
        filterAmount: this.config.tradeEventsUsdFloor,
      });

      const candidates: TradeEventDoc[] = [];
      const idsInPoll = new Set<string>();
      for (const t of rawTrades) {
        const event = buildTradeEvent(t, {
          tradeEventsUsdFloor: this.config.tradeEventsUsdFloor,
          whaleUsdFloor: this.config.whaleUsdFloor,
          ingestedAt: this.now(),
        });
        if (!event) continue;
        if (this.seenIds.has(event._id) || idsInPoll.has(event._id)) continue;

        candidates.push(event);
        idsInPoll.add(event._id);
      }

      const existingIds = await this.findExistingIds(candidates.map((event) => event._id));
      for (const id of existingIds) {
        this.addToSeen(id);
      }

      const toInsert = candidates.filter((event) => !existingIds.has(event._id));
      let inserted = 0;
      let duplicateRaceSkipped = 0;
      let handledIds: string[] = [];
      if (toInsert.length > 0) {
        const result = await this.insertTradeEvents(toInsert);
        inserted = result.inserted;
        duplicateRaceSkipped = result.duplicateSkipped;
        handledIds = result.handledIds;
      }
      for (const id of handledIds) {
        this.addToSeen(id);
      }

      const duplicateSkipped = existingIds.size + duplicateRaceSkipped;
      this.state.lastPollAt = Date.now();
      this.state.lastError = null;
      this.state.lastAttempted = candidates.length;
      this.state.lastInserted = inserted;
      this.state.lastDuplicateSkipped = duplicateSkipped;
      this.state.tradeEventsIngestedTotal += inserted;

      this.log.info({
        fetched: rawTrades.length,
        attempted: candidates.length,
        inserted,
        duplicateSkipped,
      }, 'all_trades poll complete');
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      this.state.lastError = msg;
      this.log.error({ err: msg }, 'all_trades poll failed');
    } finally {
      this.pollInProgress = false;
    }
  }

  private async findExistingIds(ids: string[]): Promise<Set<string>> {
    if (ids.length === 0) return new Set();

    const existing = await this.tradeEventsCollection
      .find(
        { _id: { $in: ids } },
        { projection: { _id: 1 } }
      )
      .toArray();

    return new Set(existing.map((doc) => doc._id));
  }

  private async insertTradeEvents(events: TradeEventDoc[]): Promise<{
    inserted: number;
    duplicateSkipped: number;
    handledIds: string[];
  }> {
    try {
      await this.tradeEventsCollection.bulkWrite(
        events.map((event) => ({ insertOne: { document: event } })),
        { ordered: false }
      );
      return {
        inserted: events.length,
        duplicateSkipped: 0,
        handledIds: events.map((event) => event._id),
      };
    } catch (err) {
      if (err instanceof MongoBulkWriteError) {
        const writeErrorsArr = getWriteErrors(err);
        const realErrors = writeErrorsArr.filter((e: { code?: number }) => e.code !== 11000);
        if (realErrors.length > 0) {
          throw err;
        }
        return {
          inserted: events.length - writeErrorsArr.length,
          duplicateSkipped: writeErrorsArr.length,
          handledIds: events.map((event) => event._id),
        };
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

export function buildTradeEvent(
  trade: PolymarketTrade,
  opts: {
    tradeEventsUsdFloor: number;
    whaleUsdFloor: number;
    ingestedAt?: Date;
  }
): TradeEventDoc | null {
  const usd = trade.size * trade.price;
  if (usd < opts.tradeEventsUsdFloor) return null;

  return {
    _id: synthesizeTradeId(trade),
    proxyWallet: trade.proxyWallet.toLowerCase(),
    pseudonym: trade.pseudonym ?? null,
    side: trade.side,
    outcome: normalizeOutcome(trade.outcome),
    usdSize: usd,
    shares: trade.size,
    priceCents: Math.round(trade.price * 100),
    priceMillicents: Math.round(trade.price * 10_000),
    conditionId: trade.conditionId,
    marketSlug: trade.slug,
    category: null,
    timestamp: trade.timestamp,
    ingestedAt: opts.ingestedAt ?? new Date(),
    isWhale: usd >= opts.whaleUsdFloor,
  };
}

function getWriteErrors(err: MongoBulkWriteError): Array<{ code?: number }> {
  const writeErrors = err.writeErrors as unknown;
  if (!writeErrors) return [];
  if (Array.isArray(writeErrors)) return writeErrors as Array<{ code?: number }>;
  if (
    typeof writeErrors === 'object'
    && writeErrors !== null
    && 'values' in writeErrors
    && typeof (writeErrors as { values?: unknown }).values === 'function'
  ) {
    return Array.from((writeErrors as Map<unknown, { code?: number }>).values());
  }
  return [writeErrors as { code?: number }];
}
