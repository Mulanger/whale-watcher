export function normalizeOutcome(outcome: string): string {
  const normalized = outcome.trim().toUpperCase();
  if (normalized === 'YES' || normalized === 'NO') {
    return normalized;
  }
  return outcome;
}
