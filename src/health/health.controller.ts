import { Controller, Get } from '@nestjs/common';
import { PrismaService } from '../prisma/prisma.service';
import { RedisService } from '../redis/redis.service';

@Controller()
export class HealthController {
  constructor(
    private readonly prisma: PrismaService,
    private readonly redis: RedisService,
  ) {}

  @Get('healthz')
  async healthz() {
    let database = 'down';
    let redis = this.redis.raw ? 'configured' : 'disabled';

    try {
      await this.prisma.$queryRaw`SELECT 1`;
      database = 'up';
    } catch {
      database = 'down';
    }

    try {
      if (this.redis.raw) {
        await this.redis.raw.ping();
        redis = 'up';
      }
    } catch {
      redis = 'down';
    }

    return {
      ok: database === 'up',
      service: 'altahhan-whatsapp-bot',
      database,
      redis,
      timestamp: new Date().toISOString(),
    };
  }
}
