import { Injectable, NotFoundException } from '@nestjs/common';
import { PaymentMethod } from '@prisma/client';
import { PrismaService } from '../prisma/prisma.service';

@Injectable()
export class OrdersService {
  constructor(private readonly prisma: PrismaService) {}

  async createCodOrder(input: {
    customerId: string;
    cartId: string;
    governorate?: string;
    city?: string;
    addressLine?: string;
    notes?: string;
  }) {
    const cart = await this.prisma.cart.findUnique({
      where: { id: input.cartId },
      include: { items: true },
    });

    if (!cart) throw new NotFoundException('Cart not found');

    const order = await this.prisma.order.create({
      data: {
        customerId: input.customerId,
        cartId: input.cartId,
        paymentMethod: PaymentMethod.COD,
        governorate: input.governorate || null,
        city: input.city || null,
        addressLine: input.addressLine || null,
        notes: input.notes || null,
        subtotal: cart.subtotal,
        shippingFee: cart.shippingFee,
        total: cart.total,
        items: {
          create: cart.items.map((item) => ({
            productId: item.productId,
            quantity: item.quantity,
            unitPrice: item.unitPrice,
          })),
        },
      },
      include: { items: true },
    });

    await this.prisma.cart.update({
      where: { id: cart.id },
      data: { status: 'CHECKED_OUT' },
    });

    return order;
  }

  async getOrder(id: string) {
    const order = await this.prisma.order.findUnique({
      where: { id },
      include: { items: { include: { product: true } }, customer: true },
    });

    if (!order) throw new NotFoundException('Order not found');
    return order;
  }

  async lookupOrder(input: { phone?: string; wooOrderId?: string }) {
    if (input.wooOrderId) {
      return this.prisma.order.findFirst({
        where: { wooOrderId: Number(input.wooOrderId) },
        include: { items: { include: { product: true } }, customer: true },
      });
    }

    if (input.phone) {
      return this.prisma.order.findFirst({
        where: { customer: { phoneE164: input.phone } },
        include: { items: { include: { product: true } }, customer: true },
        orderBy: { createdAt: 'desc' },
      });
    }

    return null;
  }
}
