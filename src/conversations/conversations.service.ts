import { Injectable } from '@nestjs/common';
import { Conversation, Customer, Language, MessageDirection, MessageType } from '@prisma/client';
import { PrismaService } from '../prisma/prisma.service';
import { HandoffService } from '../handoff/handoff.service';
import { IntentRouterService } from './intent-router.service';
import { ResponseBuilderService } from './response-builder.service';
import { BotReply, IncomingMessageContext, SupportedLanguage } from './types';

@Injectable()
export class ConversationsService {
  constructor(
    private readonly prisma: PrismaService,
    private readonly handoffService: HandoffService,
    private readonly intentRouter: IntentRouterService,
    private readonly responseBuilder: ResponseBuilderService,
  ) {}

  async handleIncomingMessage(input: IncomingMessageContext): Promise<BotReply> {
    const customer = await this.upsertCustomer(input.phone);
    const conversation = await this.findOrCreateActiveConversation(customer.id);

    await this.prisma.message.create({
      data: {
        conversationId: conversation.id,
        direction: MessageDirection.INBOUND,
        type: MessageType.TEXT,
        externalId: input.messageId,
        text: input.text,
        payloadJson: input.rawPayload as object | undefined,
      },
    });

    await this.prisma.conversation.update({
      where: { id: conversation.id },
      data: { lastUserMessageAt: new Date() },
    });

    const selectedLanguage = this.intentRouter.detectLanguage(input.text);
    if (selectedLanguage) {
      await this.prisma.customer.update({
        where: { id: customer.id },
        data: { preferredLanguage: selectedLanguage === 'AR' ? Language.AR : Language.EN },
      });
      const menu = this.responseBuilder.buildMainMenu(selectedLanguage);
      await this.logOutboundMessage(conversation.id, menu.text);
      return menu;
    }

    const language = this.resolveLanguage(customer.preferredLanguage);
    if (!customer.preferredLanguage) {
      const text = this.responseBuilder.buildLanguageSelection();
      await this.logOutboundMessage(conversation.id, text);
      return { text, language: 'AR' };
    }

    const intent = this.intentRouter.detectIntent(input.text);
    await this.prisma.conversation.update({
      where: { id: conversation.id },
      data: { currentIntent: intent },
    });

    const reply = await this.responseBuilder.buildIntentReply(intent, language, input.text);

    if (reply.handoff) {
      await this.handoffService.createTicket({
        customerId: customer.id,
        conversationId: conversation.id,
        reason: intent,
        transcript: input.rawPayload,
      });
    }

    await this.logOutboundMessage(conversation.id, reply.text);
    return reply;
  }

  private async upsertCustomer(phone: string): Promise<Customer> {
    return this.prisma.customer.upsert({
      where: { phoneE164: phone },
      update: {},
      create: { phoneE164: phone },
    });
  }

  private async findOrCreateActiveConversation(customerId: string): Promise<Conversation> {
    const existing = await this.prisma.conversation.findFirst({
      where: { customerId, status: 'ACTIVE' },
      orderBy: { startedAt: 'desc' },
    });

    if (existing) return existing;

    return this.prisma.conversation.create({
      data: { customerId, status: 'ACTIVE' },
    });
  }

  private resolveLanguage(language: Language | null): SupportedLanguage {
    return language === Language.EN ? 'EN' : 'AR';
  }

  private async logOutboundMessage(conversationId: string, text: string): Promise<void> {
    await this.prisma.message.create({
      data: {
        conversationId,
        direction: MessageDirection.OUTBOUND,
        type: MessageType.TEXT,
        text,
      },
    });

    await this.prisma.conversation.update({
      where: { id: conversationId },
      data: { lastBotMessageAt: new Date() },
    });
  }
}
