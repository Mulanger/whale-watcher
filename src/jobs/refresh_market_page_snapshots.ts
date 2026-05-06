import type { AnyBulkWriteOperation, Collection } from 'mongodb';
import { loadConfig } from '../config.js';
import { getLogger } from '../logger.js';
import type {
  EnrichedWhale,
  MarketDoc,
  MarketPageRelatedSnapshot,
  MarketPageSnapshotDoc,
  MarketPageWalletSnapshot,
} from '../db/mongo.js';

const SECONDS_PER_DAY = 24 * 60 * 60;

interface MarketAggregateRow {
  _id: string;
  slug: string;
  conditionId: string | null;
  title: string;
  icon: string | null;
  category: string | null;
  eventSlug: string | null;
  polymarketUrl: string | null;
  yesPriceCents: number | null;
  noPriceCents: number | null;
  whaleVolume: number;
  whaleTradeCount: number;
  uniqueWallets: string[];
  biggestTradeUsd: number;
  latestTradeTs: number;
  firstTradeTs: number;
}

interface MarketPagesState {
  lastRunAt: number | null;
  lastError: string | null;
  lastSnapshotsUpdated: number;
  lastIndexableCount: number;
  lastStaleCount: number;
  lastPrunedCount: number;
  running: boolean;
}

export interface MarketPagesHandle {
  stop: () => void;
  getState: () => MarketPagesState;
}

export async function refreshMarketPageSnapshots(
  trades: Collection<EnrichedWhale>,
  markets: Collection<MarketDoc>,
  marketPageSnapshots: Collection<MarketPageSnapshotDoc>
): Promise<{
  snapshotsUpdated: number;
  indexableCount: number;
  staleCount: number;
  prunedCount: number;
}> {
  const config = loadConfig();
  const log = getLogger();
  const now = new Date();
  const cutoffTs = Math.floor(Date.now() / 1000) - config.marketPagesLookbackDays * SECONDS_PER_DAY;
  const pruneBeforeTs = Math.floor(Date.now() / 1000) - config.marketPagesPruneAfterDays * SECONDS_PER_DAY;

  const rows = await trades.aggregate<MarketAggregateRow>([
    {
      $match: {
        timestamp: { $gte: cutoffTs },
        usdSize: { $gte: config.whaleUsdFloor },
        'market.slug': { $type: 'string', $ne: '' },
      },
    },
    { $sort: { timestamp: -1 } },
    {
      $group: {
        _id: '$market.slug',
        slug: { $first: '$market.slug' },
        conditionId: { $first: '$market.conditionId' },
        title: { $first: '$market.title' },
        icon: { $first: '$market.icon' },
        category: { $first: '$market.category' },
        eventSlug: { $first: '$market.eventSlug' },
        polymarketUrl: { $first: '$market.polymarketUrl' },
        yesPriceCents: { $first: '$market.yesPriceCents' },
        noPriceCents: { $first: '$market.noPriceCents' },
        whaleVolume: { $sum: '$usdSize' },
        whaleTradeCount: { $sum: 1 },
        uniqueWallets: { $addToSet: { $toLower: '$trader.proxyWallet' } },
        biggestTradeUsd: { $max: '$usdSize' },
        latestTradeTs: { $max: '$timestamp' },
        firstTradeTs: { $min: '$timestamp' },
      },
    },
    { $sort: { whaleVolume: -1 } },
  ], { allowDiskUse: true }).toArray();

  const allRelatedInputs = rows.map((row) => ({
    slug: row.slug,
    title: row.title || row.slug,
    icon: row.icon,
    eventSlug: row.eventSlug,
    whaleVolume: row.whaleVolume,
    whaleTradeCount: row.whaleTradeCount,
  }));

  let indexableCount = 0;
  const ops: AnyBulkWriteOperation<MarketPageSnapshotDoc>[] = [];

  for (const row of rows) {
    const marketMeta = await findMarketMeta(markets, row);
    const stats = {
      whaleVolume: Number(row.whaleVolume || 0),
      whaleTradeCount: Number(row.whaleTradeCount || 0),
      uniqueWhales: row.uniqueWallets.filter(Boolean).length,
      biggestTradeUsd: Number(row.biggestTradeUsd || 0),
      latestTradeTs: Number(row.latestTradeTs || 0),
      firstTradeTs: Number(row.firstTradeTs || 0),
    };
    const indexable = stats.whaleTradeCount >= config.marketPagesMinTrades
      && stats.whaleVolume >= config.marketPagesMinVolumeUsd;
    if (indexable) indexableCount += 1;

    const [topWallets, previous] = await Promise.all([
      getTopWalletsForMarket(trades, row.slug, cutoffTs),
      marketPageSnapshots.findOne({ _id: row.slug }, { projection: { lastQualifiedAt: 1 } }),
    ]);

    const doc: MarketPageSnapshotDoc = {
      _id: row.slug,
      slug: row.slug,
      market: {
        slug: row.slug,
        conditionId: marketMeta?._id ?? row.conditionId ?? null,
        title: marketMeta?.title ?? row.title ?? row.slug,
        icon: marketMeta?.icon ?? row.icon ?? null,
        category: marketMeta?.category ?? row.category ?? null,
        eventSlug: marketMeta?.eventSlug ?? row.eventSlug ?? null,
        polymarketUrl: row.polymarketUrl ?? buildPolymarketUrl(row.eventSlug, row.slug),
        endDate: marketMeta?.endDate ?? null,
        active: marketMeta?.isActive ?? null,
        yesPriceCents: marketMeta?.yesPriceCents ?? row.yesPriceCents ?? null,
        noPriceCents: marketMeta?.noPriceCents ?? row.noPriceCents ?? null,
        volume24h: marketMeta?.volume24h ?? null,
        liquidity: marketMeta?.liquidity ?? null,
      },
      stats,
      topWallets,
      relatedMarkets: buildRelatedMarkets(allRelatedInputs, row.slug),
      indexable,
      indexingReason: indexable
        ? `${config.marketPagesMinTrades}+ whale trades and $${config.marketPagesMinVolumeUsd.toLocaleString()}+ tracked whale volume`
        : 'Known market below current indexing thresholds',
      source: 'market_page_worker',
      lookbackDays: config.marketPagesLookbackDays,
      refreshedAt: now,
      lastQualifiedAt: indexable ? now : previous?.lastQualifiedAt ?? null,
      staleAt: null,
    };

    ops.push({
      replaceOne: {
        filter: { _id: row.slug },
        replacement: doc,
        upsert: true,
      },
    });
  }

  if (ops.length) {
    await marketPageSnapshots.bulkWrite(ops, { ordered: false });
  }

  const staleResult = await marketPageSnapshots.updateMany(
    { refreshedAt: { $lt: now }, staleAt: null },
    {
      $set: {
        indexable: false,
        indexingReason: 'No qualifying whale activity inside the current market-page lookback window',
        staleAt: now,
      },
    }
  );

  const pruneResult = await marketPageSnapshots.deleteMany({
    indexable: false,
    'stats.latestTradeTs': { $lt: pruneBeforeTs },
  });

  log.info({
    snapshotsUpdated: rows.length,
    indexableCount,
    staleCount: staleResult.modifiedCount,
    prunedCount: pruneResult.deletedCount,
  }, 'Market page snapshots refreshed');

  return {
    snapshotsUpdated: rows.length,
    indexableCount,
    staleCount: staleResult.modifiedCount,
    prunedCount: pruneResult.deletedCount,
  };
}

export function startMarketPageSnapshotsJob(
  trades: Collection<EnrichedWhale>,
  markets: Collection<MarketDoc>,
  marketPageSnapshots: Collection<MarketPageSnapshotDoc>
): MarketPagesHandle {
  const config = loadConfig();
  const log = getLogger();
  const state: MarketPagesState = {
    lastRunAt: null,
    lastError: null,
    lastSnapshotsUpdated: 0,
    lastIndexableCount: 0,
    lastStaleCount: 0,
    lastPrunedCount: 0,
    running: false,
  };

  const run = async () => {
    if (state.running) return;
    state.running = true;
    try {
      const result = await refreshMarketPageSnapshots(trades, markets, marketPageSnapshots);
      state.lastRunAt = Date.now();
      state.lastError = null;
      state.lastSnapshotsUpdated = result.snapshotsUpdated;
      state.lastIndexableCount = result.indexableCount;
      state.lastStaleCount = result.staleCount;
      state.lastPrunedCount = result.prunedCount;
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      state.lastError = message;
      log.error({ err }, 'Market page snapshots job failed');
    } finally {
      state.running = false;
    }
  };

  void run();
  const interval = setInterval(run, config.marketPagesIntervalMs);
  return {
    stop: () => clearInterval(interval),
    getState: () => ({ ...state }),
  };
}

async function findMarketMeta(
  markets: Collection<MarketDoc>,
  row: MarketAggregateRow
): Promise<MarketDoc | null> {
  return markets.findOne({
    $or: [
      { slug: row.slug },
      ...(row.conditionId ? [{ _id: row.conditionId }] : []),
    ],
  });
}

async function getTopWalletsForMarket(
  trades: Collection<EnrichedWhale>,
  slug: string,
  cutoffTs: number
): Promise<MarketPageWalletSnapshot[]> {
  const rows = await trades.aggregate<{
    _id: string;
    pseudonym: string | null;
    displayName: string | null;
    profileImage: string | null;
    volume: number;
    tradeCount: number;
  }>([
    {
      $match: {
        timestamp: { $gte: cutoffTs },
        usdSize: { $gte: loadConfig().whaleUsdFloor },
        'market.slug': slug,
        'trader.proxyWallet': { $type: 'string', $ne: '' },
      },
    },
    { $sort: { timestamp: 1 } },
    {
      $group: {
        _id: { $toLower: '$trader.proxyWallet' },
        pseudonym: { $last: '$trader.pseudonym' },
        displayName: { $last: '$trader.displayName' },
        profileImage: { $last: '$trader.profileImage' },
        volume: { $sum: '$usdSize' },
        tradeCount: { $sum: 1 },
      },
    },
    { $sort: { volume: -1, _id: 1 } },
    { $limit: 12 },
  ], { allowDiskUse: true }).toArray();

  return rows.map((row, index) => ({
    rank: index + 1,
    proxyWallet: row._id,
    pseudonym: row.pseudonym ?? null,
    displayName: row.displayName ?? null,
    profileImage: row.profileImage ?? null,
    volume: row.volume,
    tradeCount: row.tradeCount,
    avgTrade: row.volume / Math.max(1, row.tradeCount),
  }));
}

function buildRelatedMarkets(
  markets: Array<{
    slug: string;
    title: string;
    icon: string | null;
    eventSlug: string | null;
    whaleVolume: number;
    whaleTradeCount: number;
  }>,
  currentSlug: string
): MarketPageRelatedSnapshot[] {
  const current = markets.find((market) => market.slug === currentSlug);
  if (!current) return [];

  const currentTokens = tokenizeMarketTitle(current.title);
  return markets
    .filter((market) => market.slug !== currentSlug)
    .map((market) => {
      const sameEventScore = current.eventSlug && market.eventSlug === current.eventSlug ? 5 : 0;
      const score = sameEventScore + sharedTokenScore(currentTokens, tokenizeMarketTitle(market.title));
      return {
        slug: market.slug,
        title: market.title,
        icon: market.icon,
        eventSlug: market.eventSlug,
        whaleVolume: market.whaleVolume,
        whaleTradeCount: market.whaleTradeCount,
        score,
      };
    })
    .filter((market) => market.score > 0)
    .sort((a, b) => b.score - a.score || b.whaleVolume - a.whaleVolume)
    .slice(0, 6);
}

function tokenizeMarketTitle(title: string): Set<string> {
  return new Set(
    title
      .toLowerCase()
      .replace(/[^a-z0-9\s]/g, ' ')
      .split(/\s+/)
      .filter((token) => token.length >= 4 && !stopTokens.has(token))
  );
}

function sharedTokenScore(a: Set<string>, b: Set<string>): number {
  let score = 0;
  for (const token of a) {
    if (b.has(token)) score += 1;
  }
  return score;
}

function buildPolymarketUrl(eventSlug: string | null, slug: string): string | null {
  if (!slug) return null;
  return eventSlug
    ? `https://polymarket.com/event/${eventSlug}/${slug}`
    : `https://polymarket.com/market/${slug}`;
}

const stopTokens = new Set([
  'will',
  'price',
  'above',
  'below',
  'market',
  'returns',
  'normal',
  'before',
  'after',
  'over',
  'under',
]);
