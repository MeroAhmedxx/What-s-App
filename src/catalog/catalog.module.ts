import { Module } from '@nestjs/common';
import { HttpModule } from '@nestjs/axios';
import { CatalogService } from './catalog.service';
import { WooService } from './woo.service';
import { CatalogController } from './catalog.controller';

@Module({
  imports: [HttpModule],
  providers: [CatalogService, WooService],
  controllers: [CatalogController],
  exports: [CatalogService, WooService],
})
export class CatalogModule {}
