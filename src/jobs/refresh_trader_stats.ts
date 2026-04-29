import type { Collection } from 'mongodb';
import { getLogger } from '../logger.js';
import { getUserPositions } from '../polymarket/client.js';
import type { EnrichedWhale, TraderDoc } from '../db/mongo.js';

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

export async function refreshTraderStats(
  trades: Collection<EnrichedWhale>,
  traders: Collection<TraderDoc>
): Promise<void> {
  const log = getLogger();

  const cutoff = Math.floor(Date.now() / 1000) - 86400;

  const recentTraders = await trades.aggregate([
    { $match: { timestamp: { $gte: cutoff } } },
    { $group: { _id: '$trader.proxyWallet' } },
  ]).toArray();

  log.info({ count: recentTraders.length }, 'Refreshing trader stats');

  for (const { _id: proxyWallet } of recentTraders) {
    try {
      const positions = await getUserPositions(proxyWallet);

      let vol30d = 0;
      let tradeCount = 0;
      let totalPnl = 0;
      let wins = 0;

      for (const pos of positions) {
        const currentValue = pos.currentValue || (pos.size * pos.curPrice);
        const pnl = pos.cashPnl + pos.realizedPnl;

        vol30d += Math.abs(currentValue);
        tradeCount += 1;
        totalPnl += pnl;
        if (pnl > 0) wins++;
      }

      const winRate = tradeCount > 0 ? wins / tradeCount : null;

      await traders.updateOne(
        { _id: proxyWallet.toLowerCase() },
        {
          $set: {
            vol30d,
            winRate,
            tradeCount,
            totalPnl,
            refreshedAt: new Date(),
          },
        },
        { upsert: true }
      );
    } catch (e) {
      log.warn({ proxyWallet, err: e }, 'Trader stats refresh failed');
    }
    await sleep(150);
  }
}

export function startRefreshTraderStatsJob(
  trades: Collection<EnrichedWhale>,
  traders: Collection<TraderDoc>
): ReturnType<typeof setInterval> {
  const log = getLogger();
  const run = async () => {
    try {
      await refreshTraderStats(trades, traders);
    } catch (err) {
      log.error({ err }, 'Refresh trader stats job failed');
    }
  };

  run();
  return setInterval(run, 5 * 60_000);
}
