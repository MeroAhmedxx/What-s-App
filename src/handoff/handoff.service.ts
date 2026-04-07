import { Injectable } from '@nestjs/common';
import { HandoffStatus, TicketPriority } from '@prisma/client';
import { PrismaService } from '../prisma/prisma.service';
import { SupportedLanguage } from '../conversations/types';

@Injectable()
export class HandoffService {
  constructor(private readonly prisma: PrismaService) {}

  async createTicket(input: {
    customerId: string;
    conversationId: string;
    reason: string;
    transcript?: unknown;
  }): Promise<void> {
    await this.prisma.supportTicket.create({
      data: {
        customerId: input.customerId,
        conversationId: input.conversationId,
        topic: input.reason,
        priority: input.reason === 'complaint' ? TicketPriority.HIGH : TicketPriority.MEDIUM,
        transcriptJson: input.transcript as object | undefined,
      },
    });

    await this.prisma.conversation.update({
      where: { id: input.conversationId },
      data: { handoffStatus: HandoffStatus.REQUESTED },
    });
  }

  buildAgentReply(language: SupportedLanguage, reason: string): string {
    if (language === 'AR') {
      return `تمام، حولت طلبك لخدمة العملاء (${reason}). هيتواصل معاك فريق الدعم في أقرب وقت.`;
    }

    return `Done. I flagged your request for customer support (${reason}). An agent should follow up with you shortly.`;
  }
}
