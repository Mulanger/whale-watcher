import type { AddressInfo } from 'node:net';
import { describe, expect, it } from 'vitest';
import { startHealthServer } from '../src/http/health.js';

describe('health upstream status', () => {
  it('reports stale upstream data without failing process health', async () => {
    const server = startHealthServer(0, () => ({
      mongoConnected: true,
      redisConnected: true,
      lastPollAt: Date.now(),
      lastPollAge: 0,
      tradesIngestedTotal: 1,
      lastError: null,
      upstream: {
        ok: false,
        dataApiWhales: {
          floor: 10_000,
          latestSourceTrade: {
            id: 'source1',
            timestamp: Math.floor(Date.now() / 1000) - 3600,
            usdSize: 12_000,
            side: 'BUY',
            slug: 'stale-market',
            transactionHash: '0xhash',
          },
          latestSourceTradeAge: 3_600_000,
          staleAfterMs: 900_000,
          stale: true,
        },
      },
      leaderboard: { enabled: false, ok: true },
    }));
    await new Promise<void>((resolve) => server.once('listening', resolve));
    const address = server.address() as AddressInfo;

    try {
      const response = await fetch(`http://127.0.0.1:${address.port}/health`);
      const body = await response.json();

      expect(response.status).toBe(200);
      expect(body.ok).toBe(true);
      expect(body.upstream.ok).toBe(false);
      expect(body.upstream.dataApiWhales.stale).toBe(true);
    } finally {
      server.close();
    }
  });
});
