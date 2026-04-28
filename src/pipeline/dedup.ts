import { createHash } from 'node:crypto';
import type { PolymarketTrade } from '../polymarket/types.js';

export function synthesizeTradeId(t: PolymarketTrade): string {
  const key = `${t.transactionHash}:${t.asset}:${t.proxyWallet}:${t.timestamp}:${t.size}:${t.price}`;
  return createHash('sha1').update(key).digest('hex').slice(0, 24);
}
