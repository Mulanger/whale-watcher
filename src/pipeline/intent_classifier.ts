export type TradeIntent = 'OPEN' | 'INCREASE' | 'DECREASE' | 'CLOSE';

export const EPSILON = 0.01;

export class TransientLagError extends Error {
  constructor(message = 'positions endpoint has not caught up to trade settlement') {
    super(message);
    this.name = 'TransientLagError';
  }
}

export function classifyIntent(
  trade: { side: 'BUY' | 'SELL'; size: number; asset: string },
  positionAfter: { asset: string; size: number } | null
): TradeIntent {
  const sharesAfter = positionAfter?.size ?? 0;
  const sharesBefore = trade.side === 'BUY'
    ? sharesAfter - trade.size
    : sharesAfter + trade.size;

  if (sharesBefore < -EPSILON) {
    throw new TransientLagError();
  }

  if (trade.side === 'BUY') {
    if (sharesBefore <= EPSILON) return 'OPEN';
    return 'INCREASE';
  }

  if (sharesAfter <= EPSILON) return 'CLOSE';
  return 'DECREASE';
}

export function shouldShowInFeed(intent: TradeIntent): boolean {
  return intent === 'OPEN' || intent === 'INCREASE';
}
