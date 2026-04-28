import { createServer } from 'node:http';
import type { PollerState } from '../pipeline/trades_poller.js';

export interface HealthStatus {
  mongoConnected: boolean;
  redisConnected: boolean;
  lastPollAt: number | null;
  lastPollAge: number;
  tradesIngestedTotal: number;
  lastError: string | null;
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
      const ok = h.mongoConnected && h.redisConnected && lastPollAge < 30_000;

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
      }));
    } else {
      res.statusCode = 404;
      res.end();
    }
  });

  server.listen(port);
  return server;
}
