import { Injectable } from '@nestjs/common';
import { CatalogService } from '../catalog/catalog.service';
import { BranchesService } from '../branches/branches.service';
import { FaqService } from '../faq/faq.service';
import { BotIntent, BotReply, SupportedLanguage } from './types';

@Injectable()
export class ResponseBuilderService {
  constructor(
    private readonly catalogService: CatalogService,
    private readonly branchesService: BranchesService,
    private readonly faqService: FaqService,
  ) {}

  buildLanguageSelection(): string {
    return [
      'أهلاً بيك في تمور الطحان 🌴',
      'Welcome to Al Tahhan Dates 🌴',
      '',
      'Please choose your language / اختار اللغة:',
      '1) عربي',
      '2) English',
      '',
      'Reply with: 1 / 2 or عربي / English',
    ].join('\n');
  }

  buildMainMenu(language: SupportedLanguage): BotReply {
    if (language === 'AR') {
      return {
        language,
        text: [
          'أهلاً بيك في تمور الطحان 🌴',
          'اختار من القائمة:',
          '1) المنتجات',
          '2) التوصيل والشحن',
          '3) الفروع ومنافذ البيع',
          '4) خدمة العملاء',
          '5) التحدث مع موظف',
          '',
          'اكتب اسم المنتج أو سؤالك مباشرة.',
        ].join('\n'),
      };
    }

    return {
      language,
      text: [
        'Welcome to Al Tahhan Dates 🌴',
        'Choose from the menu:',
        '1) Products',
        '2) Delivery & Shipping',
        '3) Branches & Stores',
        '4) Customer Support',
        '5) Talk to an Agent',
        '',
        'You can also type a product name or ask a question directly.',
      ].join('\n'),
    };
  }

  async buildIntentReply(intent: BotIntent, language: SupportedLanguage, userText: string): Promise<BotReply> {
    switch (intent) {
      case 'main_menu':
        return this.buildMainMenu(language);
      case 'search_product': {
        const products = await this.catalogService.searchProducts(userText, 3);
        if (!products.length) {
          return {
            language,
            text:
              language === 'AR'
                ? 'لسه ملقتش منتج مطابق في الكاش الحالي. شغل مزامنة الكتالوج أو ابعت اسم المنتج بشكل أوضح.'
                : 'I could not find a close product in the current cache yet. Run catalog sync or send a clearer product name.',
          };
        }
        const header = language === 'AR' ? 'أقرب منتجات لطلبك:' : 'Closest products I found:';
        const lines = products.map((p, index) => {
          const name = language === 'AR' ? p.nameAr : p.nameEn || p.nameAr;
          const price = p.salePrice ?? p.regularPrice ?? 'Ask support';
          const suffix = p.stockStatus === 'IN_STOCK'
            ? language === 'AR' ? 'متاح' : 'In stock'
            : language === 'AR' ? 'غير متاح' : 'Out of stock';
          return `${index + 1}) ${name} - ${price} EGP - ${suffix}`;
        });
        return { language, text: [header, ...lines].join('\n') };
      }
      case 'find_branch': {
        const branches = await this.branchesService.listActiveBranches();
        const header = language === 'AR' ? 'منافذ البيع المتاحة:' : 'Available branches:';
        const lines = branches.slice(0, 4).map((b, index) => `${index + 1}) ${b.name} - ${b.address}`);
        return { language, text: [header, ...lines].join('\n') };
      }
      case 'ask_shipping':
        return { language, text: this.faqService.getShippingReply(language) };
      case 'ask_payment':
        return { language, text: this.faqService.getPaymentReply(language) };
      case 'talk_to_agent':
        return {
          language,
          handoff: true,
          text: `${this.faqService.getSupportReply(language)}\n\n${language === 'AR' ? 'تم تحويل طلبك لفريق الدعم.' : 'Your request has been escalated to the support team.'}`,
        };
      case 'complaint':
        return {
          language,
          handoff: true,
          text: language === 'AR'
            ? 'آسفين على المشكلة. تم تصعيد الشكوى فورًا لفريق خدمة العملاء.'
            : 'Sorry about that. Your complaint has been escalated to customer support immediately.',
        };
      default:
        return {
          language,
          text:
            language === 'AR'
              ? 'ممكن تساعدني باسم المنتج أو تختار من القائمة: المنتجات / التوصيل / الفروع / خدمة العملاء.'
              : 'Please help me with a product name or choose from the menu: products / delivery / branches / support.',
        };
    }
  }
}
