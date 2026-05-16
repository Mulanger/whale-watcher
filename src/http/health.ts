import { createServer } from 'node:http';
import type { SourceTradeSnapshot } from '../pipeline/source_trade.js';

export interface SourceTradeHealthStatus {
  floor: number;
  latestSourceTrade: SourceTradeSnapshot | null;
  latestSourceTradeAge: number;
  staleAfterMs: number;
  stale: boolean;
}

export interface UpstreamHealthStatus {
  ok: boolean;
  dataApiWhales: SourceTradeHealthStatus;
  dataApiTradeEvents?: SourceTradeHealthStatus;
}

export interface LeaderboardHealthStatus {
  enabled: boolean;
  ok: boolean;
  allTrades?: {
    lastPollAt: number | null;
    lastPollAge: number;
    lastError: string | null;
    tradeEventsIngestedTotal: number;
    lastAttempted: number;
    lastInserted: number;
    lastDuplicateSkipped: number;
    skippedOverlappingPolls: number;
    staleAfterMs: number;
  };
  dailyAggregator?: {
    lastRunAt: number | null;
    lastRunAge: number;
    lastError: string | null;
    lastRowsUpdated: number;
    lastAggregatedDays: string[];
    running: boolean;
    staleAfterMs: number;
  };
}

export interface MarketPagesHealthStatus {
  enabled: boolean;
  ok: boolean;
  lastRunAt?: number | null;
  lastRunAge?: number;
  lastError?: string | null;
  lastSnapshotsUpdated?: number;
  lastIndexableCount?: number;
  lastStaleCount?: number;
  lastPrunedCount?: number;
  running?: boolean;
  staleAfterMs?: number;
}

export interface TraderPagesHealthStatus {
  enabled: boolean;
  ok: boolean;
  lastRunAt?: number | null;
  lastRunAge?: number;
  lastError?: string | null;
  lastIndexedCount?: number;
  lastCandidateCount?: number;
  running?: boolean;
  staleAfterMs?: number;
}

export interface HealthStatus {
  mongoConnected: boolean;
  redisConnected: boolean;
  lastPollAt: number | null;
  lastPollAge: number;
  tradesIngestedTotal: number;
  lastError: string | null;
  upstream?: UpstreamHealthStatus;
  leaderboard?: LeaderboardHealthStatus;
  marketPages?: MarketPagesHealthStatus;
  traderPages?: TraderPagesHealthStatus;
}

export function startHealthServer(
  port: number,
  getStatus: () => HealthStatus
): ReturnType<typeof createServer> {
  const server = createServer((req, res) => {
    if (req.url === '/health') {
      const h = getStatus();
      const now = Date.now();
      const lastPollAge = h.lastPollAt ? now - h.lastPollAt : Infinity;
      const leaderboardOk = h.leaderboard?.ok ?? true;
      const marketPagesOk = h.marketPages?.ok ?? true;
      const traderPagesOk = h.traderPages?.ok ?? true;
      const ok = h.mongoConnected && h.redisConnected && lastPollAge < 30_000 && leaderboardOk && marketPagesOk && traderPagesOk;

      res.statusCode = ok ? 200 : 503;
      res.setHeader('content-type', 'application/json');
      res.setHeader('cache-control', 'no-store');
      res.setHeader('x-content-type-options', 'nosniff');
      res.end(JSON.stringify({
        ok,
        mongoConnected: h.mongoConnected,
        redisConnected: h.redisConnected,
        lastPollAt: h.lastPollAt,
        lastPollAge,
        tradesIngestedTotal: h.tradesIngestedTotal,
        lastError: h.lastError,
        upstream: h.upstream,
        leaderboard: h.leaderboard,
        marketPages: h.marketPages,
        traderPages: h.traderPages,
      }));
    } else {
      res.statusCode = 404;
      res.end();
    }
  });

  server.listen(port);
  return server;
}
