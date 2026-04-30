import type { PolymarketTrade } from '../polymarket/types.js';
import { synthesizeTradeId } from './dedup.js';

export interface SourceTradeSnapshot {
  id: string;
  timestamp: number;
  usdSize: number;
  side: 'BUY' | 'SELL';
  slug: string;
  transactionHash: string;
}

export function snapshotSourceTrade(trade: PolymarketTrade): SourceTradeSnapshot {
  return {
    id: synthesizeTradeId(trade),
    timestamp: trade.timestamp,
    usdSize: trade.size * trade.price,
    side: trade.side,
    slug: trade.slug,
    transactionHash: trade.transactionHash,
  };
}

export function sourceTradeAgeMs(
  sourceTrade: SourceTradeSnapshot | null,
  nowMs = Date.now()
): number {
  return sourceTrade ? nowMs - sourceTrade.timestamp * 1000 : Infinity;
}
