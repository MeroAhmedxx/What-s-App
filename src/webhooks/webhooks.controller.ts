import { Body, Controller, Get, HttpCode, Post, Query, Res } from '@nestjs/common';
import { Response } from 'express';
import { WebhooksService } from './webhooks.service';

@Controller('webhooks/whatsapp')
export class WebhooksController {
  constructor(private readonly webhooksService: WebhooksService) {}

  @Get()
  verify(
    @Query('hub.mode') mode: string,
    @Query('hub.verify_token') verifyToken: string,
    @Query('hub.challenge') challenge: string,
    @Res() res: Response,
  ) {
    if (!this.webhooksService.verify(mode, verifyToken)) {
      return res.status(403).send('Forbidden');
    }

    return res.status(200).send(challenge);
  }

  @Post()
  @HttpCode(200)
  async receive(@Body() payload: unknown) {
    await this.webhooksService.handleIncoming(payload);
    return { received: true };
  }
}
