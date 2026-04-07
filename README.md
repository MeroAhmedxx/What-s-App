# Altahhan WhatsApp Bot

Production-oriented NestJS starter backend for a bilingual WhatsApp commerce and support bot for **Al Tahhan Dates**.

## What is included
- WhatsApp Cloud API webhook verification and inbound handling
- Bilingual conversation engine (Arabic / English)
- Rule-based intent routing for commerce + support
- WooCommerce product sync service with category sync
- Product catalog endpoints
- Branches endpoint with basic filtering
- Cart and COD order flows
- Order lookup endpoint
- Health endpoint
- Prisma schema for customers, conversations, messages, products, carts, orders, tickets
- Redis-ready session/cache layer

## Main flows
- Language selection
- Main menu
- Product search
- Shipping FAQ
- Payment FAQ
- Branch lookup
- Talk to an agent
- Complaint escalation
- COD order creation
- Order lookup by phone or Woo order id

## Quick start
1. Copy `.env.example` to `.env`
2. Fill in Meta WhatsApp credentials
3. Fill in WooCommerce API credentials
4. Start infrastructure: `docker compose up -d`
5. Install dependencies: `npm install`
6. Generate Prisma client: `npx prisma generate`
7. Run migrations: `npx prisma migrate dev --name init`
8. Start the app: `npm run start:dev`

## Important endpoints
- `GET /api/healthz`
- `GET /api/webhooks/whatsapp` webhook verification
- `POST /api/webhooks/whatsapp` inbound messages
- `GET /api/catalog/categories`
- `GET /api/catalog/products?q=تمر`
- `GET /api/catalog/products/:id`
- `POST /api/catalog/sync` sync products from WooCommerce
- `GET /api/branches`
- `GET /api/branches?governorate=Giza&city=Zayed`
- `POST /api/cart/:customerId`
- `POST /api/cart/:cartId/items`
- `POST /api/orders/cod`
- `GET /api/orders?phone=%2B2010xxxxxxx`
- `GET /api/orders?wooOrderId=1234`

## Production notes
- Prices should not be hardcoded in the bot.
- WooCommerce should remain the source of truth for catalog, price, and stock.
- WhatsApp interactive buttons/lists can be added next, and the webhook is already ready to read interactive replies.
- Human handoff currently creates an internal support ticket record; connect it next to your CRM/helpdesk for full live support routing.

## What still needs your real production data
- real Meta WhatsApp credentials
- real WooCommerce credentials
- real branch list and hotline numbers
- real FAQ copy and policies
- agent handoff integration target
- final checkout link/payment integration if you want non-COD checkout inside the bot flow
