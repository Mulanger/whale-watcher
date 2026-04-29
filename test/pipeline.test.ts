import { describe, it, expect } from 'vitest';
import { classifyTier } from '../src/pipeline/tier_classifier.js';
import { synthesizeTradeId } from '../src/pipeline/dedup.js';
import { classifyIntent, shouldShowInFeed, TransientLagError } from '../src/pipeline/intent_classifier.js';
import { normalizeOutcome } from '../src/pipeline/outcome.js';
import { PositionsResponseSchema } from '../src/polymarket/schemas.js';

describe('classifyTier', () => {
  it('returns mega for >= 250k', () => {
    expect(classifyTier(250_000)).toBe('mega');
    expect(classifyTier(500_000)).toBe('mega');
  });

  it('returns large for >= 100k', () => {
    expect(classifyTier(100_000)).toBe('large');
    expect(classifyTier(249_999)).toBe('large');
  });

  it('returns whale for >= 25k', () => {
    expect(classifyTier(25_000)).toBe('whale');
    expect(classifyTier(99_999)).toBe('whale');
  });

  it('returns mini for >= 10k', () => {
    expect(classifyTier(10_000)).toBe('mini');
    expect(classifyTier(24_999)).toBe('mini');
  });

  it('returns sub for < 10k', () => {
    expect(classifyTier(9_999)).toBe('sub');
    expect(classifyTier(1)).toBe('sub');
  });
});

describe('synthesizeTradeId', () => {
  it('produces deterministic 24-char hex IDs', () => {
    const trade = {
      proxyWallet: '0x6af75d4e4aaf700450efbac3708cce1665810ff1',
      side: 'BUY' as const,
      asset: 'asset123',
      conditionId: '0xdd22472e552920b8438158ea7238bfadfa4f736aa4cee91a6b86c39ead110917',
      size: 1160952.0,
      price: 0.42,
      timestamp: 1735689600,
      title: 'Will Bitcoin hit $200K by end of 2026?',
      slug: 'will-bitcoin-hit-200k-by-end-of-2026',
      icon: null,
      eventSlug: 'bitcoin-200k-2026',
      outcome: 'Yes',
      outcomeIndex: 0,
      name: null,
      pseudonym: 'Mean-Record',
      bio: '',
      profileImage: null,
      profileImageOptimized: null,
      transactionHash: '0x40b774108c44bf3182002f3a296c8e5642ba91f304d45243e60ade35a636b58d',
    };

    const id1 = synthesizeTradeId(trade);
    const id2 = synthesizeTradeId(trade);

    expect(id1).toBe(id2);
    expect(id1).toHaveLength(24);
    expect(/^[0-9a-f]{24}$/.test(id1)).toBe(true);
  });

  it('produces different IDs for different trades', () => {
    const trade1 = {
      proxyWallet: '0x6af75d4e4aaf700450efbac3708cce1665810ff1',
      side: 'BUY' as const,
      asset: 'asset1',
      conditionId: '0xdd22472e552920b8438158ea7238bfadfa4f736aa4cee91a6b86c39ead110917',
      size: 1160952.0,
      price: 0.42,
      timestamp: 1735689600,
      title: 'Trade 1',
      slug: 'trade-1',
      icon: null,
      eventSlug: 'event-1',
      outcome: 'Yes',
      outcomeIndex: 0,
      name: null,
      pseudonym: 'Trader1',
      bio: '',
      profileImage: null,
      profileImageOptimized: null,
      transactionHash: '0x40b774108c44bf3182002f3a296c8e5642ba91f304d45243e60ade35a636b58d',
    };

    const trade2 = { ...trade1, asset: 'asset2' };

    expect(synthesizeTradeId(trade1)).not.toBe(synthesizeTradeId(trade2));
  });
});

describe('normalizeOutcome', () => {
  it('normalizes binary outcomes and preserves sports labels', () => {
    expect(normalizeOutcome('Yes')).toBe('YES');
    expect(normalizeOutcome('no')).toBe('NO');
    expect(normalizeOutcome('Oilers')).toBe('Oilers');
  });
});

describe('classifyIntent', () => {
  it('classifies BUY from zero as OPEN', () => {
    const intent = classifyIntent(
      { side: 'BUY', size: 100, asset: 'a1' },
      { asset: 'a1', size: 100 }
    );
    expect(intent).toBe('OPEN');
    expect(shouldShowInFeed(intent)).toBe(true);
  });

  it('classifies BUY with existing shares as INCREASE', () => {
    const intent = classifyIntent(
      { side: 'BUY', size: 100, asset: 'a1' },
      { asset: 'a1', size: 150 }
    );
    expect(intent).toBe('INCREASE');
    expect(shouldShowInFeed(intent)).toBe(true);
  });

  it('classifies SELL that leaves shares as DECREASE', () => {
    const intent = classifyIntent(
      { side: 'SELL', size: 100, asset: 'a1' },
      { asset: 'a1', size: 30 }
    );
    expect(intent).toBe('DECREASE');
    expect(shouldShowInFeed(intent)).toBe(false);
  });

  it('classifies SELL to zero as CLOSE', () => {
    const intent = classifyIntent(
      { side: 'SELL', size: 100, asset: 'a1' },
      { asset: 'a1', size: 0 }
    );
    expect(intent).toBe('CLOSE');
    expect(shouldShowInFeed(intent)).toBe(false);
  });

  it('treats exact zero boundaries as OPEN/CLOSE', () => {
    const openIntent = classifyIntent(
      { side: 'BUY', size: 50, asset: 'a1' },
      { asset: 'a1', size: 50 }
    );
    expect(openIntent).toBe('OPEN');

    const closeIntent = classifyIntent(
      { side: 'SELL', size: 50, asset: 'a1' },
      { asset: 'a1', size: 0 }
    );
    expect(closeIntent).toBe('CLOSE');
  });

  it('throws TransientLagError for impossible negative pre-trade shares', () => {
    expect(() => classifyIntent(
      { side: 'BUY', size: 100, asset: 'a1' },
      { asset: 'a1', size: 80 }
    )).toThrow(TransientLagError);
  });

  it('classifies SELL with no remaining position as CLOSE', () => {
    const intent = classifyIntent(
      { side: 'SELL', size: 100, asset: 'a1' },
      null
    );
    expect(intent).toBe('CLOSE');
  });
});

describe('PositionsResponseSchema', () => {
  it('parses Polymarket positions array responses', () => {
    const parsed = PositionsResponseSchema.parse([
      {
        proxyWallet: '0xd4140031e313f8d850740a80d2ee6653c925a4db',
        asset: '1910830010387565971650098373488592514702818137344973088263643820608151819241',
        conditionId: '0x5db999fad322cea2914535aae5517060c3f80ad6d8c0231cde2124a434d16846',
        size: 12381.5451,
        avgPrice: '0.7134',
        initialValue: 8834.0219,
        currentValue: 8357.5429,
        cashPnl: -476.479,
        totalBought: 17693.4951,
        realizedPnl: 250.8348,
        curPrice: 0.675,
        outcomeIndex: 1,
        outcome: 'No',
        title: 'Will the U.S. invade Iran before 2027?',
      },
    ]);

    expect(parsed).toHaveLength(1);
    expect(parsed[0].size).toBe(12381.5451);
    expect(parsed[0].avgPrice).toBe(0.7134);
    expect(parsed[0].currentValue).toBe(8357.5429);
  });
});
