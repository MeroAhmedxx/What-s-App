import { Module } from '@nestjs/common';
import { ConversationsService } from './conversations.service';
import { IntentRouterService } from './intent-router.service';
import { ResponseBuilderService } from './response-builder.service';
import { CatalogModule } from '../catalog/catalog.module';
import { BranchesModule } from '../branches/branches.module';
import { FaqModule } from '../faq/faq.module';
import { HandoffModule } from '../handoff/handoff.module';

@Module({
  imports: [CatalogModule, BranchesModule, FaqModule, HandoffModule],
  providers: [ConversationsService, IntentRouterService, ResponseBuilderService],
  exports: [ConversationsService],
})
export class ConversationsModule {}
