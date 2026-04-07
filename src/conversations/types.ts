export type SupportedLanguage = 'AR' | 'EN';

export type BotIntent =
  | 'select_language'
  | 'browse_categories'
  | 'search_product'
  | 'find_branch'
  | 'ask_shipping'
  | 'ask_payment'
  | 'talk_to_agent'
  | 'complaint'
  | 'main_menu'
  | 'unknown';

export interface IncomingMessageContext {
  phone: string;
  text: string;
  messageId?: string;
  rawPayload?: unknown;
}

export interface BotReply {
  text: string;
  language: SupportedLanguage;
  handoff?: boolean;
}
