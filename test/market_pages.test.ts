import { describe, expect, it } from 'vitest';
import { refreshMarketPageSnapshots } from '../src/jobs/refresh_market_page_snapshots.js';

process.env['NODE_ENV'] = 'test';
process.env['MONGO_URI'] = 'mongodb://localhost:27017/polywatch-test';

function aggregateResult(rows: any[]) {
  return {
    toArray: async () => rows,
  };
}

class FakeTradesCollection {
  constructor(private readonly trades: any[]) {}

  aggregate(pipeline: any[]) {
    const match = pipeline.find((stage) => stage.$match)?.$match ?? {};
    const slug = match['market.slug'];

    if (typeof slug === 'string') {
      const rowsByWallet = new Map<string, any>();
      for (const trade of this.trades.filter((trade) => trade.market.slug === slug)) {
        const wallet = trade.trader.proxyWallet.toLowerCase();
        const existing = rowsByWallet.get(wallet) ?? {
          _id: wallet,
          pseudonym: null,
          displayName: null,
          profileImage: null,
          volume: 0,
          tradeCount: 0,
        };
        existing.pseudonym = trade.trader.pseudonym;
        existing.displayName = trade.trader.displayName;
        existing.profileImage = trade.trader.profileImage;
        existing.volume += trade.usdSize;
        existing.tradeCount += 1;
        rowsByWallet.set(wallet, existing);
      }
      return aggregateResult([...rowsByWallet.values()].sort((a, b) => b.volume - a.volume).slice(0, 12));
    }

    const rowsBySlug = new Map<string, any>();
    for (const trade of this.trades) {
      const existing = rowsBySlug.get(trade.market.slug) ?? {
        _id: trade.market.slug,
        slug: trade.market.slug,
        conditionId: trade.market.conditionId,
        title: trade.market.title,
        icon: trade.market.icon,
        category: trade.market.category,
        eventSlug: trade.market.eventSlug,
        polymarketUrl: trade.market.polymarketUrl,
        yesPriceCents: trade.market.yesPriceCents,
        noPriceCents: trade.market.noPriceCents,
        whaleVolume: 0,
        whaleTradeCount: 0,
        uniqueWallets: [],
        biggestTradeUsd: 0,
        latestTradeTs: 0,
        firstTradeTs: Number.MAX_SAFE_INTEGER,
      };
      existing.whaleVolume += trade.usdSize;
      existing.whaleTradeCount += 1;
      existing.uniqueWallets = [...new Set([...existing.uniqueWallets, trade.trader.proxyWallet.toLowerCase()])];
      existing.biggestTradeUsd = Math.max(existing.biggestTradeUsd, trade.usdSize);
      existing.latestTradeTs = Math.max(existing.latestTradeTs, trade.timestamp);
      existing.firstTradeTs = Math.min(existing.firstTradeTs, trade.timestamp);
      rowsBySlug.set(trade.market.slug, existing);
    }

    return aggregateResult([...rowsBySlug.values()].sort((a, b) => b.whaleVolume - a.whaleVolume));
  }
}

class FakeMarketsCollection {
  constructor(private readonly docs: any[]) {}

  async findOne(query: any) {
    const clauses = query.$or ?? [];
    return this.docs.find((doc) => clauses.some((clause: any) => clause.slug === doc.slug || clause._id === doc._id)) ?? null;
  }
}

class FakeSnapshotsCollection {
  docs = new Map<string, any>();

  async findOne(query: any) {
    return this.docs.get(query._id) ?? this.docs.get(query.slug) ?? null;
  }

  async bulkWrite(ops: any[]) {
    for (const op of ops) {
      const doc = op.replaceOne.replacement;
      this.docs.set(doc._id, doc);
    }
    return {};
  }

  async updateMany() {
    return { modifiedCount: 0 };
  }

  async deleteMany() {
    return { deletedCount: 0 };
  }
}

describe('refreshMarketPageSnapshots', () => {
  it('writes indexable market-page snapshots with top wallets and related markets', async () => {
    const now = Math.floor(Date.now() / 1000);
    const trades = new FakeTradesCollection([
      makeTrade('arsenal-win', 'Will Arsenal FC win on 2026-05-05?', '0xA', 20_000, now - 30),
      makeTrade('arsenal-win', 'Will Arsenal FC win on 2026-05-05?', '0xA', 15_000, now - 20),
      makeTrade('arsenal-win', 'Will Arsenal FC win on 2026-05-05?', '0xB', 16_000, now - 10),
      makeTrade('arsenal-total', 'Arsenal FC vs Atletico Madrid: O/U 2.5', '0xC', 70_000, now - 5),
    ]);
    const markets = new FakeMarketsCollection([
      {
        _id: 'condition-ars',
        slug: 'arsenal-win',
        title: 'Will Arsenal FC win on 2026-05-05?',
        icon: 'https://example.com/icon.png',
        category: 'Sports',
        eventSlug: 'arsenal-atletico',
        endDate: null,
        yesPriceCents: 61,
        noPriceCents: 39,
        volume24h: 100_000,
        liquidity: 10_000,
        isActive: true,
      },
    ]);
    const snapshots = new FakeSnapshotsCollection();

    const result = await refreshMarketPageSnapshots(trades as any, markets as any, snapshots as any);
    const snapshot = snapshots.docs.get('arsenal-win');

    expect(result.snapshotsUpdated).toBe(2);
    expect(result.indexableCount).toBe(1);
    expect(snapshot.indexable).toBe(true);
    expect(snapshot.stats.whaleTradeCount).toBe(3);
    expect(snapshot.stats.whaleVolume).toBe(51_000);
    expect(snapshot.topWallets[0]).toMatchObject({ proxyWallet: '0xa', volume: 35_000, tradeCount: 2 });
    expect(snapshot.relatedMarkets[0]).toMatchObject({ slug: 'arsenal-total' });
    expect(snapshot.market.yesPriceCents).toBe(61);
  });
});

function makeTrade(slug: string, title: string, wallet: string, usdSize: number, timestamp: number) {
  return {
    _id: `${slug}-${wallet}-${timestamp}`,
    side: 'BUY',
    usdSize,
    timestamp,
    market: {
      conditionId: `condition-${slug}`,
      slug,
      title,
      icon: null,
      category: 'Sports',
      eventSlug: 'arsenal-atletico',
      yesPriceCents: 60,
      noPriceCents: 40,
      polymarketUrl: `https://polymarket.com/event/arsenal-atletico/${slug}`,
    },
    trader: {
      proxyWallet: wallet,
      pseudonym: wallet,
      displayName: wallet,
      profileImage: null,
    },
  };
}
