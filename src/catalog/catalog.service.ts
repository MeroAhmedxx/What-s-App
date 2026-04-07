import { Injectable } from '@nestjs/common';
import { Product, StockStatus } from '@prisma/client';
import { PrismaService } from '../prisma/prisma.service';
import { WooService } from './woo.service';

@Injectable()
export class CatalogService {
  constructor(
    private readonly prisma: PrismaService,
    private readonly wooService: WooService,
  ) {}

  async listCategories() {
    return this.prisma.category.findMany({ where: { isActive: true }, orderBy: { nameAr: 'asc' } });
  }

  async searchProducts(query: string, limit = 10): Promise<Product[]> {
    const normalized = query.trim();
    if (!normalized) {
      return this.prisma.product.findMany({
        where: { isActive: true },
        take: limit,
        orderBy: [{ lastSyncedAt: 'desc' }, { nameAr: 'asc' }],
      });
    }

    return this.prisma.product.findMany({
      where: {
        isActive: true,
        OR: [
          { nameAr: { contains: normalized, mode: 'insensitive' } },
          { nameEn: { contains: normalized, mode: 'insensitive' } },
          { sku: { contains: normalized, mode: 'insensitive' } },
          { slug: { contains: normalized, mode: 'insensitive' } },
        ],
      },
      take: limit,
      orderBy: [{ lastSyncedAt: 'desc' }, { nameAr: 'asc' }],
    });
  }

  async getProductById(id: string): Promise<Product | null> {
    return this.prisma.product.findUnique({ where: { id } });
  }

  async syncProductsFromWooCommerce(): Promise<{ synced: number; categories: number }> {
    const remoteProducts = await this.wooService.fetchProducts();
    let synced = 0;
    const categoryIds = new Set<number>();

    for (const item of remoteProducts) {
      const product = await this.prisma.product.upsert({
        where: { wooId: item.id },
        update: {
          sku: item.sku || null,
          slug: item.slug || null,
          nameAr: item.name,
          nameEn: item.name,
          descriptionAr: item.short_description || item.description || null,
          descriptionEn: item.short_description || item.description || null,
          imageUrl: item.images?.[0]?.src || null,
          regularPrice: item.regular_price || null,
          salePrice: item.sale_price || null,
          stockStatus: this.mapStockStatus(item.stock_status),
          isActive: item.status === 'publish',
          lastSyncedAt: new Date(),
        },
        create: {
          wooId: item.id,
          sku: item.sku || null,
          slug: item.slug || null,
          nameAr: item.name,
          nameEn: item.name,
          descriptionAr: item.short_description || item.description || null,
          descriptionEn: item.short_description || item.description || null,
          imageUrl: item.images?.[0]?.src || null,
          regularPrice: item.regular_price || null,
          salePrice: item.sale_price || null,
          stockStatus: this.mapStockStatus(item.stock_status),
          isActive: item.status === 'publish',
          lastSyncedAt: new Date(),
        },
      });

      if (item.categories?.length) {
        await this.prisma.productCategory.deleteMany({ where: { productId: product.id } });

        for (const category of item.categories) {
          categoryIds.add(category.id);
          const savedCategory = await this.prisma.category.upsert({
            where: { wooId: category.id },
            update: {
              nameAr: category.name,
              nameEn: category.name,
              slug: category.slug || null,
              isActive: true,
            },
            create: {
              wooId: category.id,
              nameAr: category.name,
              nameEn: category.name,
              slug: category.slug || null,
              isActive: true,
            },
          });

          await this.prisma.productCategory.create({
            data: { productId: product.id, categoryId: savedCategory.id },
          });
        }
      }

      synced += 1;
    }

    return { synced, categories: categoryIds.size };
  }

  private mapStockStatus(status?: string): StockStatus {
    if (status === 'instock') return StockStatus.IN_STOCK;
    if (status === 'onbackorder') return StockStatus.ON_BACKORDER;
    return StockStatus.OUT_OF_STOCK;
  }
}
