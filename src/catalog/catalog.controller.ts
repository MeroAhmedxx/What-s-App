import { Controller, Get, Param, Post, Query } from '@nestjs/common';
import { CatalogService } from './catalog.service';

@Controller('catalog')
export class CatalogController {
  constructor(private readonly catalogService: CatalogService) {}

  @Get('categories')
  async getCategories() {
    return this.catalogService.listCategories();
  }

  @Get('products')
  async getProducts(@Query('q') q?: string, @Query('limit') limit?: string) {
    return this.catalogService.searchProducts(q || '', Number(limit || 10));
  }

  @Get('products/:id')
  async getProduct(@Param('id') id: string) {
    return this.catalogService.getProductById(id);
  }

  @Post('sync')
  async sync() {
    return this.catalogService.syncProductsFromWooCommerce();
  }
}
