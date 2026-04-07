import { Injectable, NotFoundException } from '@nestjs/common';
import { PrismaService } from '../prisma/prisma.service';

@Injectable()
export class CartService {
  constructor(private readonly prisma: PrismaService) {}

  async getOrCreateActiveCart(customerId: string) {
    const existing = await this.prisma.cart.findFirst({
      where: { customerId, status: 'ACTIVE' },
      include: { items: true },
      orderBy: { createdAt: 'desc' },
    });

    if (existing) return existing;

    return this.prisma.cart.create({
      data: { customerId, status: 'ACTIVE' },
      include: { items: true },
    });
  }

  async getCart(cartId: string) {
    const cart = await this.prisma.cart.findUnique({
      where: { id: cartId },
      include: { items: { include: { product: true } } },
    });

    if (!cart) throw new NotFoundException('Cart not found');
    return cart;
  }

  async addItem(cartId: string, productId: string, quantity: number) {
    const cart = await this.prisma.cart.findUnique({ where: { id: cartId } });
    if (!cart) throw new NotFoundException('Cart not found');

    const product = await this.prisma.product.findUnique({ where: { id: productId } });
    if (!product) throw new NotFoundException('Product not found');

    const unitPrice = Number(product.salePrice ?? product.regularPrice ?? 0);
    const existing = await this.prisma.cartItem.findFirst({ where: { cartId, productId } });

    if (existing) {
      await this.prisma.cartItem.update({
        where: { id: existing.id },
        data: { quantity: existing.quantity + quantity, unitPrice },
      });
    } else {
      await this.prisma.cartItem.create({
        data: {
          cartId,
          productId,
          quantity,
          unitPrice,
        },
      });
    }

    return this.recalculateCart(cartId);
  }

  private async recalculateCart(cartId: string) {
    const items = await this.prisma.cartItem.findMany({ where: { cartId } });
    const subtotal = items.reduce((sum, item) => sum + Number(item.unitPrice) * item.quantity, 0);
    const shippingFee = subtotal > 1500 ? 0 : 75;
    const total = subtotal + shippingFee;

    return this.prisma.cart.update({
      where: { id: cartId },
      data: { subtotal, shippingFee, total },
      include: { items: { include: { product: true } } },
    });
  }
}
