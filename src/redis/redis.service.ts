import { Injectable, Logger, OnModuleDestroy } from '@nestjs/common';
import Redis from 'ioredis';

@Injectable()
export class RedisService implements OnModuleDestroy {
  private readonly logger = new Logger(RedisService.name);
  private readonly client: Redis | null;

  constructor() {
    const url = process.env.REDIS_URL;
    if (!url) {
      this.client = null;
      this.logger.warn('REDIS_URL is not set. Running without Redis cache.');
      return;
    }

    this.client = new Redis(url, { lazyConnect: true, maxRetriesPerRequest: 1 });
    this.client.connect().catch((error) => {
      this.logger.warn(`Redis connection failed: ${String(error)}`);
    });
  }

  get raw(): Redis | null {
    return this.client;
  }

  async get(key: string): Promise<string | null> {
    if (!this.client) return null;
    return this.client.get(key);
  }

  async set(key: string, value: string, ttlSeconds?: number): Promise<void> {
    if (!this.client) return;
    if (ttlSeconds) {
      await this.client.set(key, value, 'EX', ttlSeconds);
      return;
    }
    await this.client.set(key, value);
  }

  async onModuleDestroy(): Promise<void> {
    await this.client?.quit();
  }
}
