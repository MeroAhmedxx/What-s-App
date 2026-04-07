import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import axios from 'axios';

@Injectable()
export class WhatsAppService {
  private readonly logger = new Logger(WhatsAppService.name);

  constructor(private readonly configService: ConfigService) {}

  async sendText(to: string, body: string): Promise<void> {
    const accessToken = this.configService.get<string>('meta.accessToken');
    const phoneNumberId = this.configService.get<string>('meta.phoneNumberId');
    const apiVersion = this.configService.get<string>('meta.apiVersion') || 'v21.0';

    if (!accessToken || !phoneNumberId) {
      this.logger.warn(`Meta credentials are missing. Skipping outbound send to ${to}.`);
      return;
    }

    const url = `https://graph.facebook.com/${apiVersion}/${phoneNumberId}/messages`;

    await axios.post(
      url,
      {
        messaging_product: 'whatsapp',
        recipient_type: 'individual',
        to,
        type: 'text',
        text: { preview_url: false, body },
      },
      {
        headers: {
          Authorization: `Bearer ${accessToken}`,
          'Content-Type': 'application/json',
        },
      },
    );
  }
}
