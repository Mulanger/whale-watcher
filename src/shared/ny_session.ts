const NEW_YORK_TIMEZONE = 'America/New_York';

interface ZonedParts {
  year: number;
  month: number;
  day: number;
  hour: number;
  minute: number;
  second: number;
}

export interface NewYorkWindow {
  timezone: typeof NEW_YORK_TIMEZONE;
  dateKey: string;
  startTs: number;
  endTs: number;
  nextResetTs: number;
  days: number;
}

const formatter = new Intl.DateTimeFormat('en-US', {
  timeZone: NEW_YORK_TIMEZONE,
  year: 'numeric',
  month: '2-digit',
  day: '2-digit',
  hour: '2-digit',
  minute: '2-digit',
  second: '2-digit',
  hour12: false,
});

function getZonedParts(date: Date): ZonedParts {
  const parts = formatter.formatToParts(date);
  const value = (type: string) => Number(parts.find((part) => part.type === type)?.value);
  const rawHour = value('hour');

  return {
    year: value('year'),
    month: value('month'),
    day: value('day'),
    hour: rawHour === 24 ? 0 : rawHour,
    minute: value('minute'),
    second: value('second'),
  };
}

function dateKey(parts: Pick<ZonedParts, 'year' | 'month' | 'day'>): string {
  return [
    String(parts.year).padStart(4, '0'),
    String(parts.month).padStart(2, '0'),
    String(parts.day).padStart(2, '0'),
  ].join('-');
}

function addLocalDays(parts: Pick<ZonedParts, 'year' | 'month' | 'day'>, days: number): Pick<ZonedParts, 'year' | 'month' | 'day'> {
  const date = new Date(Date.UTC(parts.year, parts.month - 1, parts.day + days, 12, 0, 0));
  return {
    year: date.getUTCFullYear(),
    month: date.getUTCMonth() + 1,
    day: date.getUTCDate(),
  };
}

function zonedTimeToUtcMs(parts: ZonedParts): number {
  const targetAsUtc = Date.UTC(parts.year, parts.month - 1, parts.day, parts.hour, parts.minute, parts.second);
  let guess = targetAsUtc;

  for (let attempt = 0; attempt < 3; attempt += 1) {
    const actualParts = getZonedParts(new Date(guess));
    const actualAsUtc = Date.UTC(
      actualParts.year,
      actualParts.month - 1,
      actualParts.day,
      actualParts.hour,
      actualParts.minute,
      actualParts.second
    );
    const delta = targetAsUtc - actualAsUtc;
    if (delta === 0) break;
    guess += delta;
  }

  return guess;
}

export function getNewYorkWindow(days: number, nowMs = Date.now()): NewYorkWindow {
  const safeDays = Math.max(1, Math.floor(days));
  const nowParts = getZonedParts(new Date(nowMs));
  const startDay = addLocalDays(nowParts, -(safeDays - 1));
  const tomorrow = addLocalDays(nowParts, 1);
  const startMs = zonedTimeToUtcMs({ ...startDay, hour: 0, minute: 0, second: 0 });
  const endMs = zonedTimeToUtcMs({ ...tomorrow, hour: 0, minute: 0, second: 0 });

  return {
    timezone: NEW_YORK_TIMEZONE,
    dateKey: dateKey(nowParts),
    startTs: Math.floor(startMs / 1000),
    endTs: Math.floor(endMs / 1000),
    nextResetTs: Math.floor(endMs / 1000),
    days: safeDays,
  };
}
