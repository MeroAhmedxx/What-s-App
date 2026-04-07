import { Module } from '@nestjs/common';
import { FaqService } from './faq.service';

@Module({
  providers: [FaqService],
  exports: [FaqService],
})
export class FaqModule {}
