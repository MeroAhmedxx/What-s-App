import { Injectable } from '@nestjs/common';
import { BotIntent, SupportedLanguage } from './types';

@Injectable()
export class IntentRouterService {
  detectLanguage(text: string): SupportedLanguage | null {
    const normalized = text.trim().toLowerCase();
    if (['1', 'ar', 'arabic', 'عربي', 'العربية'].includes(normalized)) return 'AR';
    if (['2', 'en', 'english', 'انجليزي', 'إنجليزي'].includes(normalized)) return 'EN';
    return null;
  }

  detectIntent(text: string): BotIntent {
    const normalized = text.trim().toLowerCase();

    if (!normalized || ['menu', 'start', 'ابدأ', 'القائمة', 'menu please', '0'].includes(normalized)) {
      return 'main_menu';
    }

    if (['1', 'products', 'المنتجات'].includes(normalized)) return 'search_product';
    if (['2', 'delivery', 'shipping', 'التوصيل', 'الشحن'].includes(normalized)) return 'ask_shipping';
    if (['3', 'branches', 'stores', 'الفروع', 'المنافذ'].includes(normalized)) return 'find_branch';
    if (['4', 'support', 'customer support', 'خدمة العملاء'].includes(normalized)) return 'talk_to_agent';
    if (['5', 'agent', 'human', 'موظف', 'مندوب'].includes(normalized)) return 'talk_to_agent';

    if (/branch|store|فرع|منفذ|مكان/.test(normalized)) return 'find_branch';
    if (/ship|delivery|شحن|توصيل/.test(normalized)) return 'ask_shipping';
    if (/pay|payment|دفع|كاش|فيزا/.test(normalized)) return 'ask_payment';
    if (/agent|human|support|customer service|خدمة العملاء|موظف|مندوب/.test(normalized)) return 'talk_to_agent';
    if (/complaint|problem|refund|شكوى|مشكلة|استرجاع/.test(normalized)) return 'complaint';
    if (/product|date|تمر|عجوة|medjool|سكري|خلاص|category|قسم/.test(normalized)) return 'search_product';

    return 'unknown';
  }
}
