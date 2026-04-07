import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { SupportedLanguage } from '../conversations/types';

@Injectable()
export class FaqService {
  constructor(private readonly configService: ConfigService) {}

  getShippingReply(language: SupportedLanguage): string {
    if (language === 'AR') {
      return [
        'التوصيل متاح لكل مصر.',
        'المدة التقريبية داخل القاهرة والجيزة والإسكندرية: 1 إلى 3 أيام.',
        'باقي المحافظات: 3 إلى 5 أيام.',
        'رسوم الشحن تحدد عند تأكيد الطلب حسب المنطقة.',
      ].join('\n');
    }

    return [
      'Delivery is available across Egypt.',
      'Estimated delivery for Cairo, Giza, and Alexandria: 1 to 3 days.',
      'Other governorates: 3 to 5 days.',
      'Shipping fees are confirmed based on the delivery area.',
    ].join('\n');
  }

  getPaymentReply(language: SupportedLanguage): string {
    if (language === 'AR') {
      return [
        'طرق الدفع المتاحة في النسخة الحالية:',
        '- كاش عند الاستلام',
        '- لينك دفع أونلاين من المتجر',
        'ولو احتجت مساعدة، نقدر نحولك لخدمة العملاء.',
      ].join('\n');
    }

    return [
      'Supported payment methods in this version:',
      '- Cash on delivery',
      '- Online checkout link from the store',
      'We can also connect you to customer support if needed.',
    ].join('\n');
  }

  getSupportReply(language: SupportedLanguage): string {
    const hotline = language === 'AR'
      ? this.configService.get<string>('app.hotlineAr') || '19682'
      : this.configService.get<string>('app.hotlineEn') || '19682';

    if (language === 'AR') {
      return [
        'تقدر تتواصل مع خدمة العملاء مباشرة.',
        `الهوت لاين: ${hotline}`,
        'أو ابعت رسالتك هنا وسنحوّلها لفريق الدعم.',
      ].join('\n');
    }

    return [
      'You can contact customer support directly.',
      `Hotline: ${hotline}`,
      'Or send your message here and we will route it to the support team.',
    ].join('\n');
  }
}
