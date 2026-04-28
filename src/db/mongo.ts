import { MongoClient, Collection, Db as Database } from 'mongodb';
import { loadConfig } from '../config.js';
import { getLogger } from '../logger.js';

let _client: MongoClient | null = null;
let _db: Database | null = null;

export interface EnrichedWhale {
  _id: string;
  tier: WhaleTier;
  side: 'BUY' | 'SELL';
  outcome: 'YES' | 'NO';
  usdSize: number;
  shares: number;
  priceCents: number;
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

export interface MarketDoc {
  _id: string;
  slug: string;
  title: string;
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

export type WhaleTier = 'mega' | 'large' | 'whale' | 'mini' | 'sub';

export async function connectMongo(): Promise<{
  client: MongoClient;
  db: Database;
  trades: Collection<EnrichedWhale>;
  markets: Collection<MarketDoc>;
  traders: Collection<TraderDoc>;
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
