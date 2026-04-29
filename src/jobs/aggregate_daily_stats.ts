import type { AnyBulkWriteOperation, Collection } from 'mongodb';
import { loadConfig } from '../config.js';
import { getLogger } from '../logger.js';
import type { EnrichedWhale, TradeEventDoc, TraderDailyStatsDoc } from '../db/mongo.js';

const SECONDS_PER_DAY = 24 * 60 * 60;

function formatUtcDay(unixSeconds: number): string {
  return new Date(unixSeconds * 1000).toISOString().slice(0, 10);
}

function dayStartUnixFromDay(dayUtc: string): number {
  const ms = Date.parse(`${dayUtc}T00:00:00.000Z`);
  return Math.floor(ms / 1000);
}

function getTodayUtc(): string {
  return new Date().toISOString().slice(0, 10);
}

function getYesterdayUtc(): string {
  const now = new Date();
  const utcMidnight = Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate());
  return formatUtcDay(Math.floor((utcMidnight - SECONDS_PER_DAY * 1000) / 1000));
}

async function flushOps(
  traderDailyStats: Collection<TraderDailyStatsDoc>,
  ops: AnyBulkWriteOperation<TraderDailyStatsDoc>[]
): Promise<void> {
  if (ops.length === 0) return;
  await traderDailyStats.bulkWrite(ops, { ordered: false });
  ops.length = 0;
}

export async function aggregateDay(
  tradeEvents: Collection<TradeEventDoc>,
  traderDailyStats: Collection<TraderDailyStatsDoc>,
  trades: Collection<EnrichedWhale>,
  dayUtc: string
): Promise<number> {
  const log = getLogger();
  const dayStart = dayStartUnixFromDay(dayUtc);
  const dayEnd = dayStart + SECONDS_PER_DAY;
  const runStartedAt = new Date();

  const feedWhaleCounts = await trades.aggregate<{
    _id: string;
    whaleCount: number;
  }>([
    { $match: { timestamp: { $gte: dayStart, $lt: dayEnd } } },
    {
      $group: {
        _id: { $toLower: '$trader.proxyWallet' },
        whaleCount: { $sum: 1 },
      },
    },
  ]).toArray();
  const whaleCountByWallet = new Map(
    feedWhaleCounts
      .filter((doc) => typeof doc._id === 'string' && doc._id.length > 0)
      .map((doc) => [doc._id, doc.whaleCount])
  );

  const cursor = tradeEvents.aggregate<{
    _id: string;
    pseudonym: string | null;
    volume: number;
    tradeCount: number;
    buyVolume: number;
    sellVolume: number;
  }>([
    { $match: { timestamp: { $gte: dayStart, $lt: dayEnd } } },
    { $sort: { timestamp: 1 } },
    {
      $group: {
        _id: '$proxyWallet',
        pseudonym: { $last: '$pseudonym' },
        volume: { $sum: '$usdSize' },
        tradeCount: { $sum: 1 },
        buyVolume: { $sum: { $cond: [{ $eq: ['$side', 'BUY'] }, '$usdSize', 0] } },
        sellVolume: { $sum: { $cond: [{ $eq: ['$side', 'SELL'] }, '$usdSize', 0] } },
      },
    },
  ], { allowDiskUse: true });

  const ops: AnyBulkWriteOperation<TraderDailyStatsDoc>[] = [];
  let rows = 0;
  for await (const doc of cursor) {
    ops.push({
      updateOne: {
        filter: { _id: `${doc._id}:${dayUtc}` },
        update: {
          $set: {
            proxyWallet: doc._id,
            pseudonym: doc.pseudonym ?? null,
            date: dayUtc,
            volume: doc.volume,
            tradeCount: doc.tradeCount,
            buyVolume: doc.buyVolume,
            sellVolume: doc.sellVolume,
            whaleCount: whaleCountByWallet.get(doc._id) ?? 0,
            updatedAt: runStartedAt,
          },
        },
        upsert: true,
      },
    });
    rows += 1;

    if (ops.length >= 1000) {
      await flushOps(traderDailyStats, ops);
    }
  }

  await flushOps(traderDailyStats, ops);
  await traderDailyStats.deleteMany({
    date: dayUtc,
    updatedAt: { $lt: runStartedAt },
  });

  log.info({ dayUtc, rows }, 'trader_daily_stats aggregated');
  return rows;
}

export interface DailyAggregatorState {
  lastRunAt: number | null;
  lastError: string | null;
  lastRowsUpdated: number;
  lastAggregatedDays: string[];
  running: boolean;
}

export interface DailyAggregatorHandle {
  stop: () => void;
  getState: () => DailyAggregatorState;
}

export function startDailyAggregator(
  tradeEvents: Collection<TradeEventDoc>,
  traderDailyStats: Collection<TraderDailyStatsDoc>,
  trades: Collection<EnrichedWhale>
): DailyAggregatorHandle {
  const config = loadConfig();
  const log = getLogger();
  const state: DailyAggregatorState = {
    lastRunAt: null,
    lastError: null,
    lastRowsUpdated: 0,
    lastAggregatedDays: [],
    running: false,
  };

  const run = async () => {
    if (state.running) {
      log.warn('daily aggregation skipped because previous run is still running');
      return;
    }

    state.running = true;
    const today = getTodayUtc();
    const yesterday = getYesterdayUtc();
    try {
      const yesterdayRows = await aggregateDay(tradeEvents, traderDailyStats, trades, yesterday);
      const todayRows = await aggregateDay(tradeEvents, traderDailyStats, trades, today);
      state.lastRunAt = Date.now();
      state.lastError = null;
      state.lastRowsUpdated = yesterdayRows + todayRows;
      state.lastAggregatedDays = [yesterday, today];
    } catch (err) {
      state.lastError = err instanceof Error ? err.message : String(err);
      log.error({ err }, 'daily aggregation job failed');
    } finally {
      state.running = false;
    }
  };

  void run();
  const intervalId = setInterval(run, config.dailyAggregatorIntervalMs);

  return {
    stop: () => clearInterval(intervalId),
    getState: () => ({ ...state, lastAggregatedDays: [...state.lastAggregatedDays] }),
  };
}
