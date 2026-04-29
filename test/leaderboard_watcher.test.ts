import type { AddressInfo } from 'node:net';
import { describe, expect, it } from 'vitest';
import { AllTradesPoller, buildTradeEvent } from '../src/pipeline/all_trades_poller.js';
import { synthesizeTradeId } from '../src/pipeline/dedup.js';
import { aggregateDay } from '../src/jobs/aggregate_daily_stats.js';
import { startHealthServer } from '../src/http/health.js';
import type { PolymarketTrade } from '../src/polymarket/types.js';

function makeTrade(overrides: Partial<PolymarketTrade> = {}): PolymarketTrade {
  return {
    proxyWallet: '0xABCDEFabcdefABCDEFabcdefABCDEFabcdefABCD',
    side: 'BUY',
    asset: 'asset1',
    conditionId: '0xcondition',
    size: 10_000,
    price: 0.25,
    timestamp: 1777459000,
    title: 'Test market',
    slug: 'test-market',
    icon: null,
    eventSlug: 'test-event',
    outcome: 'Yes',
    outcomeIndex: 0,
    name: null,
    pseudonym: 'Trader',
    bio: '',
    profileImage: null,
    profileImageOptimized: null,
    transactionHash: '0xhash1',
    ...overrides,
  };
}

class FakeTradeEventsCollection {
  docs = new Map<string, any>();
  bulkWriteCalls = 0;
  lastBulkWriteSize = 0;
  failNextBulkWrite = false;

  find(filter: { _id: { $in: string[] } }) {
    return {
      toArray: async () => filter._id.$in
        .filter((id) => this.docs.has(id))
        .map((id) => ({ _id: id })),
    };
  }

  async bulkWrite(ops: Array<{ insertOne: { document: any } }>) {
    this.bulkWriteCalls += 1;
    this.lastBulkWriteSize = ops.length;

    if (this.failNextBulkWrite) {
      this.failNextBulkWrite = false;
      throw new Error('mongo down');
    }

    for (const op of ops) {
      this.docs.set(op.insertOne.document._id, op.insertOne.document);
    }
  }
}

function aggregateResult(rows: any[]) {
  return {
    toArray: async () => rows,
    async *[Symbol.asyncIterator]() {
      for (const row of rows) yield row;
    },
  };
}

function makeTradeEventsAggregateCollection(events: any[]) {
  return {
    aggregate(pipeline: any[]) {
      const match = pipeline.find((stage) => stage.$match)?.$match;
      const start = match.timestamp.$gte;
      const end = match.timestamp.$lt;
      const rowsByWallet = new Map<string, any>();

      for (const event of events.filter((event) => event.timestamp >= start && event.timestamp < end)) {
        const existing = rowsByWallet.get(event.proxyWallet) ?? {
          _id: event.proxyWallet,
          pseudonym: null,
          volume: 0,
          tradeCount: 0,
          buyVolume: 0,
          sellVolume: 0,
        };
        existing.pseudonym = event.pseudonym;
        existing.volume += event.usdSize;
        existing.tradeCount += 1;
        existing.buyVolume += event.side === 'BUY' ? event.usdSize : 0;
        existing.sellVolume += event.side === 'SELL' ? event.usdSize : 0;
        rowsByWallet.set(event.proxyWallet, existing);
      }

      return aggregateResult([...rowsByWallet.values()]);
    },
  };
}

function makeFeedWhalesAggregateCollection(whales: any[]) {
  return {
    aggregate(pipeline: any[]) {
      const match = pipeline.find((stage) => stage.$match)?.$match;
      const start = match.timestamp.$gte;
      const end = match.timestamp.$lt;
      const rowsByWallet = new Map<string, number>();

      for (const whale of whales.filter((whale) => whale.timestamp >= start && whale.timestamp < end)) {
        const wallet = whale.trader.proxyWallet.toLowerCase();
        rowsByWallet.set(wallet, (rowsByWallet.get(wallet) ?? 0) + 1);
      }

      return aggregateResult([...rowsByWallet.entries()].map(([_id, whaleCount]) => ({ _id, whaleCount })));
    },
  };
}

class FakeTraderDailyStatsCollection {
  docs = new Map<string, any>();

  async bulkWrite(ops: any[]) {
    for (const op of ops) {
      const id = op.updateOne.filter._id;
      this.docs.set(id, { _id: id, ...op.updateOne.update.$set });
    }
  }

  async deleteMany(filter: { date: string; updatedAt: { $lt: Date } }) {
    for (const [id, doc] of this.docs) {
      if (doc.date === filter.date && doc.updatedAt < filter.updatedAt.$lt) {
        this.docs.delete(id);
      }
    }
  }
}

describe('buildTradeEvent', () => {
  it('builds lowercased trade event docs with price precision', () => {
    const trade = makeTrade({ price: 0.999, size: 2_000 });
    const event = buildTradeEvent(trade, {
      tradeEventsUsdFloor: 1_000,
      whaleUsdFloor: 10_000,
      ingestedAt: new Date('2026-04-29T00:00:00.000Z'),
    });

    expect(event).not.toBeNull();
    expect(event?.proxyWallet).toBe(trade.proxyWallet.toLowerCase());
    expect(event?.usdSize).toBe(1998);
    expect(event?.priceCents).toBe(100);
    expect(event?.priceMillicents).toBe(9990);
    expect(event?.isWhale).toBe(false);
  });

  it('returns null below the trade-events floor', () => {
    const trade = makeTrade({ price: 0.2, size: 4_000 });
    const event = buildTradeEvent(trade, {
      tradeEventsUsdFloor: 1_000,
      whaleUsdFloor: 10_000,
    });

    expect(event).toBeNull();
  });
});

describe('AllTradesPoller', () => {
  it('deduplicates repeated trades in the same API response', async () => {
    const trade = makeTrade();
    const collection = new FakeTradeEventsCollection();
    const poller = new AllTradesPoller(collection as any, {
      fetchTrades: async () => [trade, trade],
    });

    await poller.pollOnce();

    expect(collection.bulkWriteCalls).toBe(1);
    expect(collection.lastBulkWriteSize).toBe(1);
    expect(collection.docs.size).toBe(1);
    expect(poller.getState().lastAttempted).toBe(1);
  });

  it('skips existing Mongo docs on cold restart', async () => {
    const trade = makeTrade();
    const id = synthesizeTradeId(trade);
    const collection = new FakeTradeEventsCollection();
    collection.docs.set(id, { _id: id });
    const poller = new AllTradesPoller(collection as any, {
      fetchTrades: async () => [trade],
    });

    await poller.pollOnce();

    expect(collection.bulkWriteCalls).toBe(0);
    expect(poller.getState().lastInserted).toBe(0);
    expect(poller.getState().lastDuplicateSkipped).toBe(1);
  });

  it('does not poison seen IDs after transient insert failure', async () => {
    const trade = makeTrade();
    const collection = new FakeTradeEventsCollection();
    collection.failNextBulkWrite = true;
    const poller = new AllTradesPoller(collection as any, {
      fetchTrades: async () => [trade],
    });

    await poller.pollOnce();
    expect(collection.docs.size).toBe(0);
    expect(poller.getState().lastError).toBe('mongo down');

    await poller.pollOnce();
    expect(collection.docs.size).toBe(1);
    expect(poller.getState().lastError).toBeNull();
  });

  it('skips overlapping polls', async () => {
    const trade = makeTrade();
    const collection = new FakeTradeEventsCollection();
    let resolveFetch: ((trades: PolymarketTrade[]) => void) | null = null;
    const poller = new AllTradesPoller(collection as any, {
      fetchTrades: async () => new Promise<PolymarketTrade[]>((resolve) => {
        resolveFetch = resolve;
      }),
    });

    const firstPoll = poller.pollOnce();
    await poller.pollOnce();
    resolveFetch?.([trade]);
    await firstPoll;

    expect(poller.getState().skippedOverlappingPolls).toBe(1);
    expect(collection.docs.size).toBe(1);
  });
});

describe('aggregateDay', () => {
  it('aggregates trade-event volume but feed-visible whale counts from trades', async () => {
    const day = '2026-04-29';
    const dayStart = Math.floor(Date.parse(`${day}T00:00:00.000Z`) / 1000);
    const walletA = '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa';
    const walletB = '0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb';
    const tradeEvents = makeTradeEventsAggregateCollection([
      { proxyWallet: walletA, pseudonym: 'A', side: 'BUY', usdSize: 1_000, timestamp: dayStart + 10, isWhale: false },
      { proxyWallet: walletA, pseudonym: 'A2', side: 'SELL', usdSize: 2_000, timestamp: dayStart + 20, isWhale: true },
      { proxyWallet: walletB, pseudonym: 'B', side: 'BUY', usdSize: 3_000, timestamp: dayStart + 30, isWhale: true },
    ]);
    const feedWhales = makeFeedWhalesAggregateCollection([
      { trader: { proxyWallet: walletA.toUpperCase() }, timestamp: dayStart + 40 },
      { trader: { proxyWallet: walletA }, timestamp: dayStart + 50 },
      { trader: { proxyWallet: walletB }, timestamp: dayStart - 10 },
    ]);
    const traderDailyStats = new FakeTraderDailyStatsCollection();
    traderDailyStats.docs.set(`${walletA}:stale`, {
      _id: `${walletA}:stale`,
      proxyWallet: walletA,
      date: day,
      updatedAt: new Date('2026-04-28T00:00:00.000Z'),
    });

    const rows = await aggregateDay(
      tradeEvents as any,
      traderDailyStats as any,
      feedWhales as any,
      day
    );

    expect(rows).toBe(2);
    expect(traderDailyStats.docs.has(`${walletA}:stale`)).toBe(false);
    expect(traderDailyStats.docs.get(`${walletA}:${day}`)).toMatchObject({
      volume: 3_000,
      tradeCount: 2,
      buyVolume: 1_000,
      sellVolume: 2_000,
      whaleCount: 2,
      pseudonym: 'A2',
    });
    expect(traderDailyStats.docs.get(`${walletB}:${day}`)).toMatchObject({
      volume: 3_000,
      tradeCount: 1,
      whaleCount: 0,
    });
  });
});

describe('health endpoint', () => {
  it('fails health when leaderboard dependencies are stale or errored', async () => {
    const server = startHealthServer(0, () => ({
      mongoConnected: true,
      redisConnected: true,
      lastPollAt: Date.now(),
      lastPollAge: 0,
      tradesIngestedTotal: 1,
      lastError: null,
      leaderboard: {
        enabled: true,
        ok: false,
        allTrades: {
          lastPollAt: null,
          lastPollAge: Infinity,
          lastError: 'stale',
          tradeEventsIngestedTotal: 0,
          lastAttempted: 0,
          lastInserted: 0,
          lastDuplicateSkipped: 0,
          skippedOverlappingPolls: 0,
          staleAfterMs: 120_000,
        },
      },
    }));
    await new Promise<void>((resolve) => server.once('listening', resolve));
    const address = server.address() as AddressInfo;

    try {
      const response = await fetch(`http://127.0.0.1:${address.port}/health`);
      const body = await response.json();

      expect(response.status).toBe(503);
      expect(body.ok).toBe(false);
      expect(body.leaderboard.enabled).toBe(true);
    } finally {
      server.close();
    }
  });
});
