import { MongoClient, Collection, Db as Database } from 'mongodb';
import { loadConfig } from '../config.js';
import { getLogger } from '../logger.js';

let _client: MongoClient | null = null;
let _db: Database | null = null;

export interface EnrichedWhale {
  _id: string;
  intent?: 'OPEN' | 'INCREASE';
  tier: WhaleTier;
  side: 'BUY' | 'SELL';
  outcome: string;
  usdSize: number;
  shares: number;
  priceCents: number;
  priceMillicents?: number;
  timestamp: number;
  ingestedAt: Date;
  market: {
    conditionId: string;
    slug: string;
    title: string;
    icon: string | null;
    category: string | null;
    eventSlug: string | null;
    yesPriceCents: number | null;
    noPriceCents: number | null;
    polymarketUrl: string;
  };
  trader: {
    proxyWallet: string;
    pseudonym: string | null;
    displayName: string | null;
    profileImage: string | null;
    vol30d: number | null;
    winRate: number | null;
    tradeCount: number | null;
  };
  transactionHash: string;
  raw: unknown;
}

export interface IntentDiscardDoc {
  _id: string;
  wallet: string;
  conditionId: string;
  intent: 'DECREASE' | 'CLOSE';
  side: 'BUY' | 'SELL';
  usdSize: number;
  timestamp: number;
  discardedAt: Date;
}

export interface MarketDoc {
  _id: string;
  slug: string;
  title: string;
  icon: string | null;
  category: string | null;
  eventSlug: string | null;
  endDate: Date | null;
  yesPriceCents: number | null;
  noPriceCents: number | null;
  volume24h: number | null;
  liquidity: number | null;
  isActive: boolean;
  refreshedAt: Date;
}

export interface MarketPageWalletSnapshot {
  rank: number;
  proxyWallet: string;
  pseudonym: string | null;
  displayName: string | null;
  profileImage: string | null;
  volume: number;
  tradeCount: number;
  avgTrade: number;
}

export interface MarketPageRelatedSnapshot {
  slug: string;
  title: string;
  icon: string | null;
  eventSlug: string | null;
  whaleVolume: number;
  whaleTradeCount: number;
  score: number;
}

export interface MarketPageSnapshotDoc {
  _id: string;
  slug: string;
  market: {
    slug: string;
    conditionId: string | null;
    title: string;
    icon: string | null;
    category: string | null;
    eventSlug: string | null;
    polymarketUrl: string | null;
    endDate: Date | null;
    active: boolean | null;
    yesPriceCents: number | null;
    noPriceCents: number | null;
    volume24h: number | null;
    liquidity: number | null;
  };
  stats: {
    whaleVolume: number;
    whaleTradeCount: number;
    uniqueWhales: number;
    biggestTradeUsd: number;
    latestTradeTs: number;
    firstTradeTs: number;
  };
  topWallets: MarketPageWalletSnapshot[];
  relatedMarkets: MarketPageRelatedSnapshot[];
  indexable: boolean;
  indexingReason: string;
  source: 'market_page_worker';
  lookbackDays: number;
  refreshedAt: Date;
  lastQualifiedAt: Date | null;
  staleAt: Date | null;
  prunedAt?: Date | null;
}

export interface TraderDoc {
  _id: string;
  pseudonym: string | null;
  displayName: string | null;
  profileImage: string | null;
  vol30d: number | null;
  winRate: number | null;
  tradeCount: number | null;
  totalPnl: number | null;
  refreshedAt: Date;
}

export interface TradeEventDoc {
  _id: string;
  proxyWallet: string;
  pseudonym: string | null;
  side: 'BUY' | 'SELL';
  outcome: string;
  usdSize: number;
  shares: number;
  priceCents: number;
  priceMillicents?: number;
  conditionId: string;
  marketSlug: string;
  category: string | null;
  timestamp: number;
  ingestedAt: Date;
  isWhale: boolean;
}

export interface TraderDailyStatsDoc {
  _id: string;
  proxyWallet: string;
  pseudonym: string | null;
  date: string;
  volume: number;
  tradeCount: number;
  buyVolume: number;
  sellVolume: number;
  whaleCount: number;
  updatedAt: Date;
}

export interface TraderPageIndexDoc {
  _id: string;
  proxyWallet: string;
  pseudonym: string | null;
  displayName: string | null;
  profileImage: string | null;
  firstSeenTs: number;
  lastSeenTs: number;
  firstLeaderboardAt: number;
  lastLeaderboardAt: number;
  bestRank: number;
  bestRankWindow: '1d' | '7d' | '30d' | '365d';
  bestVolume: number;
  tradeCount: number;
  whaleCount: number;
  indexable: boolean;
  source: 'trader_page_worker';
  updatedAt: Date;
}

export type WhaleTier = 'mega' | 'large' | 'whale' | 'mini' | 'sub';

export async function connectMongo(): Promise<{
  client: MongoClient;
  db: Database;
  trades: Collection<EnrichedWhale>;
  markets: Collection<MarketDoc>;
  traders: Collection<TraderDoc>;
  intentDiscards: Collection<IntentDiscardDoc>;
  tradeEvents: Collection<TradeEventDoc>;
  traderDailyStats: Collection<TraderDailyStatsDoc>;
  marketPageSnapshots: Collection<MarketPageSnapshotDoc>;
  traderPageIndex: Collection<TraderPageIndexDoc>;
}> {
  const config = loadConfig();
  const log = getLogger();

  if (_client && _db) {
    return {
      client: _client,
      db: _db,
      trades: _db.collection<EnrichedWhale>('trades'),
      markets: _db.collection<MarketDoc>('markets'),
      traders: _db.collection<TraderDoc>('traders'),
      intentDiscards: _db.collection<IntentDiscardDoc>('intent_discards'),
      tradeEvents: _db.collection<TradeEventDoc>('trade_events'),
      traderDailyStats: _db.collection<TraderDailyStatsDoc>('trader_daily_stats'),
      marketPageSnapshots: _db.collection<MarketPageSnapshotDoc>('market_page_snapshots'),
      traderPageIndex: _db.collection<TraderPageIndexDoc>('trader_page_index'),
    };
  }

  log.info('Connecting to MongoDB...');
  _client = new MongoClient(config.mongoUri);
  await _client.connect();
  _db = _client.db(config.mongoDb);

  log.info('MongoDB connected');

  return {
    client: _client,
    db: _db,
    trades: _db.collection<EnrichedWhale>('trades'),
    markets: _db.collection<MarketDoc>('markets'),
    traders: _db.collection<TraderDoc>('traders'),
    intentDiscards: _db.collection<IntentDiscardDoc>('intent_discards'),
    tradeEvents: _db.collection<TradeEventDoc>('trade_events'),
    traderDailyStats: _db.collection<TraderDailyStatsDoc>('trader_daily_stats'),
    marketPageSnapshots: _db.collection<MarketPageSnapshotDoc>('market_page_snapshots'),
    traderPageIndex: _db.collection<TraderPageIndexDoc>('trader_page_index'),
  };
}

export async function closeMongo(): Promise<void> {
  if (_client) {
    await _client.close();
    _client = null;
    _db = null;
  }
}

export function isMongoConnected(): boolean {
  return _client !== null;
}
