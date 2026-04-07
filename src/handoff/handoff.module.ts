import { Module } from '@nestjs/common';
import { HandoffService } from './handoff.service';

@Module({
  providers: [HandoffService],
  exports: [HandoffService],
})
export class HandoffModule {}
