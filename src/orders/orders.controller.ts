import { Body, Controller, Get, Param, Post, Query } from '@nestjs/common';
import { CreateCodOrderDto } from './dto/create-cod-order.dto';
import { OrdersService } from './orders.service';

@Controller('orders')
export class OrdersController {
  constructor(private readonly ordersService: OrdersService) {}

  @Post('cod')
  async createCod(@Body() body: CreateCodOrderDto) {
    return this.ordersService.createCodOrder(body);
  }

  @Get(':id')
  async getOne(@Param('id') id: string) {
    return this.ordersService.getOrder(id);
  }

  @Get()
  async lookup(@Query('phone') phone?: string, @Query('wooOrderId') wooOrderId?: string) {
    return this.ordersService.lookupOrder({ phone, wooOrderId });
  }
}
