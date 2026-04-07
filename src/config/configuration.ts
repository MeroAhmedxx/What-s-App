export default () => ({
  app: {
    port: Number(process.env.PORT || 3000),
    baseUrl: process.env.APP_BASE_URL || 'http://localhost:3000',
    env: process.env.NODE_ENV || 'development',
    hotlineAr: process.env.HOTLINE_AR || process.env.HOTLINE || '19682',
    hotlineEn: process.env.HOTLINE_EN || process.env.HOTLINE || '19682',
  },
  meta: {
    verifyToken: process.env.META_VERIFY_TOKEN,
    accessToken: process.env.META_ACCESS_TOKEN,
    phoneNumberId: process.env.META_PHONE_NUMBER_ID,
    apiVersion: process.env.META_API_VERSION || 'v21.0',
  },
  woo: {
    baseUrl: process.env.WOO_BASE_URL,
    consumerKey: process.env.WOO_CONSUMER_KEY,
    consumerSecret: process.env.WOO_CONSUMER_SECRET,
    apiVersion: process.env.WOO_API_VERSION || 'wc/v3',
  },
  redis: {
    url: process.env.REDIS_URL,
  },
});
