import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { ConversationsService } from '../conversations/conversations.service';
import { WhatsAppService } from '../whatsapp/whatsapp.service';

interface MetaMessage {
  id?: string;
  from?: string;
  text?: { body?: string };
  interactive?: {
    button_reply?: { id?: string; title?: string };
    list_reply?: { id?: string; title?: string };
  };
  button?: { text?: string };
  type?: string;
}

@Injectable()
export class WebhooksService {
  private readonly logger = new Logger(WebhooksService.name);

  constructor(
    private readonly configService: ConfigService,
    private readonly conversationsService: ConversationsService,
    private readonly whatsAppService: WhatsAppService,
  ) {}

  verify(mode: string, verifyToken: string): boolean {
    const expectedToken = this.configService.get<string>('meta.verifyToken');
    return mode === 'subscribe' && Boolean(expectedToken) && verifyToken === expectedToken;
  }

  async handleIncoming(payload: unknown): Promise<void> {
    const messages = this.extractMessages(payload);
    for (const message of messages) {
      const phone = message.from;
      const text = this.extractText(message);
      if (!phone || !text) continue;

      const reply = await this.conversationsService.handleIncomingMessage({
        phone,
        text,
        messageId: message.id,
        rawPayload: payload,
      });

      try {
        await this.whatsAppService.sendText(phone, reply.text);
      } catch (error) {
        this.logger.error(`Failed to send WhatsApp reply: ${String(error)}`);
      }
    }
  }

  private extractText(message: MetaMessage): string | null {
    return (
      message.text?.body?.trim() ||
      message.interactive?.button_reply?.id ||
      message.interactive?.button_reply?.title ||
      message.interactive?.list_reply?.id ||
      message.interactive?.list_reply?.title ||
      message.button?.text ||
      null
    );
  }

  private extractMessages(payload: unknown): MetaMessage[] {
    const data = payload as {
      entry?: Array<{
        changes?: Array<{
          value?: {
            messages?: MetaMessage[];
          };
        }>;
      }>;
    };

    return (
      data.entry?.flatMap((entry) =>
        (entry.changes || []).flatMap((change) => change.value?.messages || []),
      ) || []
    );
  }
}
