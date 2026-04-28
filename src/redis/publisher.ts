import { Redis } from 'ioredis';
import { loadConfig } from '../config.js';
import { getLogger } from '../logger.js';

let _redis: Redis | null = null;

export async function connectRedis(): Promise<Redis> {
  if (_redis) return _redis;

  const config = loadConfig();
  const log = getLogger();

  log.info('Connecting to Redis...');
  _redis = new Redis(config.redisUrl, {
    lazyConnect: true,
  });

  await _redis.connect();
  log.info('Redis connected');

  return _redis;
}

export async function publishWhale(whale: unknown): Promise<void> {
  const config = loadConfig();
  const redis = await connectRedis();
  await redis.publish(config.redisChannel, JSON.stringify(whale));
}

export async function closeRedis(): Promise<void> {
  if (_redis) {
    await _redis.quit();
    _redis = null;
  }
}

export function isRedisConnected(): boolean {
  return _redis !== null && _redis.status === 'ready';
}
