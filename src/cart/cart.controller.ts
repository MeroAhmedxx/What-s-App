import { Body, Controller, Get, Param, Post } from '@nestjs/common';
import { AddCartItemDto } from './dto/add-cart-item.dto';
import { CartService } from './cart.service';

@Controller('cart')
export class CartController {
  constructor(private readonly cartService: CartService) {}

  @Post(':customerId')
  async getOrCreate(@Param('customerId') customerId: string) {
    return this.cartService.getOrCreateActiveCart(customerId);
  }

  @Get(':cartId')
  async getCart(@Param('cartId') cartId: string) {
    return this.cartService.getCart(cartId);
  }

  @Post(':cartId/items')
  async addItem(@Param('cartId') cartId: string, @Body() body: AddCartItemDto) {
    return this.cartService.addItem(cartId, body.productId, body.quantity);
  }
}
