import type { AnyBulkWriteOperation, Collection } from 'mongodb';
import { loadConfig } from '../config.js';
import { getLogger } from '../logger.js';
import type { EnrichedWhale, TraderPageIndexDoc } from '../db/mongo.js';
import { getNewYorkWindow } from '../shared/ny_session.js';

type LeaderboardWindow = '1d' | '7d' | '30d' | '365d';

const WINDOW_DAYS: Record<LeaderboardWindow, number> = {
  '1d': 1,
  '7d': 7,
  '30d': 30,
  '365d': 365,
};

const LEADERBOARD_WINDOWS = Object.keys(WINDOW_DAYS) as LeaderboardWindow[];

interface TraderWindowRow {
  _id: string;
  pseudonym: string | null;
  displayName: string | null;
  profileImage: string | null;
  volume: number;
  tradeCount: number;
  whaleCount: number;
  firstSeenTs: number;
  lastSeenTs: number;
}

interface CandidateTrader {
  proxyWallet: string;
  pseudonym: string | null;
  displayName: string | null;
  profileImage: string | null;
  firstSeenTs: number;
  lastSeenTs: number;
  bestRank: number;
  bestRankWindow: LeaderboardWindow;
  bestVolume: number;
  tradeCount: number;
  whaleCount: number;
}

interface TraderPageIndexState {
  lastRunAt: number | null;
  lastError: string | null;
  lastIndexedCount: number;
  lastCandidateCount: number;
  running: boolean;
}

export interface TraderPageIndexHandle {
  stop: () => void;
  getState: () => TraderPageIndexState;
}

export async function refreshTraderPageIndex(
  trades: Collection<EnrichedWhale>,
  traderPageIndex: Collection<TraderPageIndexDoc>
): Promise<{ candidates: number; indexed: number }> {
  const config = loadConfig();
  const log = getLogger();
  const candidates = new Map<string, CandidateTrader>();

  for (const windowId of LEADERBOARD_WINDOWS) {
    const rows = await aggregateLeaderboardWindow(trades, windowId, config.traderPagesIndexLimit);
    rows.forEach((row, index) => {
      const proxyWallet = row._id.toLowerCase();
      const rank = index + 1;
      const existing = candidates.get(proxyWallet);
      const next: CandidateTrader = {
        proxyWallet,
        pseudonym: row.pseudonym ?? existing?.pseudonym ?? null,
        displayName: row.displayName ?? existing?.displayName ?? null,
        profileImage: row.profileImage ?? existing?.profileImage ?? null,
        firstSeenTs: Math.min(row.firstSeenTs, existing?.firstSeenTs ?? row.firstSeenTs),
        lastSeenTs: Math.max(row.lastSeenTs, existing?.lastSeenTs ?? row.lastSeenTs),
        bestRank: existing ? Math.min(existing.bestRank, rank) : rank,
        bestRankWindow: existing && existing.bestRank <= rank ? existing.bestRankWindow : windowId,
        bestVolume: Math.max(row.volume, existing?.bestVolume ?? 0),
        tradeCount: Math.max(row.tradeCount, existing?.tradeCount ?? 0),
        whaleCount: Math.max(row.whaleCount, existing?.whaleCount ?? 0),
      };
      candidates.set(proxyWallet, next);
    });
  }

  const wallets = Array.from(candidates.keys());
  if (!wallets.length) {
    log.info('Trader page index refresh found no candidates');
    return { candidates: 0, indexed: 0 };
  }

  const previousDocs = await traderPageIndex
    .find({ _id: { $in: wallets } })
    .project<TraderPageIndexDoc>({
      _id: 1,
      proxyWallet: 1,
      pseudonym: 1,
      displayName: 1,
      profileImage: 1,
      firstSeenTs: 1,
      lastSeenTs: 1,
      firstLeaderboardAt: 1,
      lastLeaderboardAt: 1,
      bestRank: 1,
      bestRankWindow: 1,
      bestVolume: 1,
      tradeCount: 1,
      whaleCount: 1,
      indexable: 1,
      source: 1,
      updatedAt: 1,
    })
    .toArray();
  const previousByWallet = new Map(previousDocs.map((doc) => [doc._id, doc]));
  const now = new Date();
  const nowTs = Math.floor(now.getTime() / 1000);
  const ops: AnyBulkWriteOperation<TraderPageIndexDoc>[] = [];

  for (const candidate of candidates.values()) {
    const previous = previousByWallet.get(candidate.proxyWallet);
    const bestRank = previous ? Math.min(previous.bestRank, candidate.bestRank) : candidate.bestRank;
    const keepPreviousBestWindow = previous && previous.bestRank <= candidate.bestRank;
    const doc: TraderPageIndexDoc = {
      _id: candidate.proxyWallet,
      proxyWallet: candidate.proxyWallet,
      pseudonym: candidate.pseudonym ?? previous?.pseudonym ?? null,
      displayName: candidate.displayName ?? previous?.displayName ?? null,
      profileImage: candidate.profileImage ?? previous?.profileImage ?? null,
      firstSeenTs: previous ? Math.min(previous.firstSeenTs, candidate.firstSeenTs) : candidate.firstSeenTs,
      lastSeenTs: previous ? Math.max(previous.lastSeenTs, candidate.lastSeenTs) : candidate.lastSeenTs,
      firstLeaderboardAt: previous?.firstLeaderboardAt ?? nowTs,
      lastLeaderboardAt: nowTs,
      bestRank,
      bestRankWindow: keepPreviousBestWindow ? previous.bestRankWindow : candidate.bestRankWindow,
      bestVolume: Math.max(previous?.bestVolume ?? 0, candidate.bestVolume),
      tradeCount: Math.max(previous?.tradeCount ?? 0, candidate.tradeCount),
      whaleCount: Math.max(previous?.whaleCount ?? 0, candidate.whaleCount),
      indexable: true,
      source: 'trader_page_worker',
      updatedAt: now,
    };

    ops.push({
      replaceOne: {
        filter: { _id: candidate.proxyWallet },
        replacement: doc,
        upsert: true,
      },
    });
  }

  if (ops.length) {
    await traderPageIndex.bulkWrite(ops, { ordered: false });
  }

  log.info({
    candidates: candidates.size,
    indexed: ops.length,
  }, 'Trader page index refreshed');

  return { candidates: candidates.size, indexed: ops.length };
}

export function startTraderPageIndexJob(
  trades: Collection<EnrichedWhale>,
  traderPageIndex: Collection<TraderPageIndexDoc>
): TraderPageIndexHandle {
  const config = loadConfig();
  const log = getLogger();
  const state: TraderPageIndexState = {
    lastRunAt: null,
    lastError: null,
    lastIndexedCount: 0,
    lastCandidateCount: 0,
    running: false,
  };

  const run = async () => {
    if (state.running) return;
    state.running = true;
    try {
      const result = await refreshTraderPageIndex(trades, traderPageIndex);
      state.lastRunAt = Date.now();
      state.lastError = null;
      state.lastIndexedCount = result.indexed;
      state.lastCandidateCount = result.candidates;
    } catch (err) {
      state.lastError = err instanceof Error ? err.message : String(err);
      log.error({ err }, 'Trader page index job failed');
    } finally {
      state.running = false;
    }
  };

  void run();
  const interval = setInterval(run, config.traderPagesIntervalMs);
  return {
    stop: () => clearInterval(interval),
    getState: () => ({ ...state }),
  };
}

async function aggregateLeaderboardWindow(
  trades: Collection<EnrichedWhale>,
  windowId: LeaderboardWindow,
  limit: number
): Promise<TraderWindowRow[]> {
  const config = loadConfig();
  const session = getNewYorkWindow(WINDOW_DAYS[windowId]);

  return trades.aggregate<TraderWindowRow>([
    {
      $match: {
        timestamp: { $gte: session.startTs, $lt: session.endTs },
        usdSize: { $gte: config.whaleUsdFloor },
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
        whaleCount: { $sum: 1 },
        firstSeenTs: { $min: '$timestamp' },
        lastSeenTs: { $max: '$timestamp' },
      },
    },
    { $sort: { volume: -1, _id: 1 } },
    { $limit: limit },
  ], { allowDiskUse: true }).toArray();
}
