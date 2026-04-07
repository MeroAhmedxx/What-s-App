import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import configuration from './config/configuration';
import { PrismaModule } from './prisma/prisma.module';
import { RedisModule } from './redis/redis.module';
import { WebhooksModule } from './webhooks/webhooks.module';
import { WhatsAppModule } from './whatsapp/whatsapp.module';
import { ConversationsModule } from './conversations/conversations.module';
import { CatalogModule } from './catalog/catalog.module';
import { BranchesModule } from './branches/branches.module';
import { FaqModule } from './faq/faq.module';
import { HandoffModule } from './handoff/handoff.module';
import { CartModule } from './cart/cart.module';
import { OrdersModule } from './orders/orders.module';
import { HealthModule } from './health/health.module';

@Module({
  imports: [
    ConfigModule.forRoot({ isGlobal: true, load: [configuration] }),
    PrismaModule,
    RedisModule,
    WebhooksModule,
    WhatsAppModule,
    ConversationsModule,
    CatalogModule,
    BranchesModule,
    FaqModule,
    HandoffModule,
    CartModule,
    OrdersModule,
    HealthModule,
  ],
})
export class AppModule {}
