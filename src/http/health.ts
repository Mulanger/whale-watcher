import { createServer } from 'node:http';

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

export interface HealthStatus {
  mongoConnected: boolean;
  redisConnected: boolean;
  lastPollAt: number | null;
  lastPollAge: number;
  tradesIngestedTotal: number;
  lastError: string | null;
  leaderboard?: LeaderboardHealthStatus;
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
      const ok = h.mongoConnected && h.redisConnected && lastPollAge < 30_000 && leaderboardOk;

      res.statusCode = ok ? 200 : 503;
      res.setHeader('content-type', 'application/json');
      res.end(JSON.stringify({
        ok,
        mongoConnected: h.mongoConnected,
        redisConnected: h.redisConnected,
        lastPollAt: h.lastPollAt,
        lastPollAge,
        tradesIngestedTotal: h.tradesIngestedTotal,
        lastError: h.lastError,
        leaderboard: h.leaderboard,
      }));
    } else {
      res.statusCode = 404;
      res.end();
    }
  });

  server.listen(port);
  return server;
}
