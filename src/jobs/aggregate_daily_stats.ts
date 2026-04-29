import type { AnyBulkWriteOperation, Collection } from 'mongodb';
import { loadConfig } from '../config.js';
import { getLogger } from '../logger.js';
import type { TradeEventDoc, TraderDailyStatsDoc } from '../db/mongo.js';

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
  dayUtc: string
): Promise<void> {
  const log = getLogger();
  const dayStart = dayStartUnixFromDay(dayUtc);
  const dayEnd = dayStart + SECONDS_PER_DAY;
  const runStartedAt = new Date();

  const cursor = tradeEvents.aggregate<{
    _id: string;
    pseudonym: string | null;
    volume: number;
    tradeCount: number;
    buyVolume: number;
    sellVolume: number;
    whaleCount: number;
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
        whaleCount: { $sum: { $cond: ['$isWhale', 1, 0] } },
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
            whaleCount: doc.whaleCount,
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
}

export function startDailyAggregator(
  tradeEvents: Collection<TradeEventDoc>,
  traderDailyStats: Collection<TraderDailyStatsDoc>
): ReturnType<typeof setInterval> {
  const config = loadConfig();
  const log = getLogger();

  const run = async () => {
    const today = getTodayUtc();
    const yesterday = getYesterdayUtc();
    try {
      await aggregateDay(tradeEvents, traderDailyStats, yesterday);
      await aggregateDay(tradeEvents, traderDailyStats, today);
    } catch (err) {
      log.error({ err }, 'daily aggregation job failed');
    }
  };

  void run();
  return setInterval(run, config.dailyAggregatorIntervalMs);
}
