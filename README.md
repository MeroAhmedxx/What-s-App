# Altahhan WhatsApp Bot

Production-oriented NestJS backend for a bilingual WhatsApp commerce and support bot for **Al Tahhan Dates**.

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
- Prisma schema **and initial migration** for customers, conversations, messages, products, carts, orders, tickets
- Redis-ready session/cache layer
- Railway-ready startup script that waits for Postgres, runs migrations, then starts the app

## Quick start (local)
1. Copy `.env.example` to `.env`
2. Fill in Meta WhatsApp credentials
3. Fill in WooCommerce API credentials
4. Start infrastructure: `docker compose up -d`
5. Install dependencies: `npm install`
6. Generate Prisma client: `npx prisma generate`
7. Run migrations: `npx prisma migrate deploy`
8. Start the app: `npm run start:dev`

## Railway deployment
This repository is prepared for Railway using `nixpacks.toml`.

### Build phase
Railway should only build the app:
- `npm install`
- `npx prisma generate`
- `npm run build`

### Runtime start
Railway should start the app with:

```bash
bash ./scripts/start-production.sh
```

The startup script will:
1. generate the Prisma client
2. wait until the database is reachable
3. run `prisma migrate deploy`
4. start the Nest app

### Important Railway note
Do **not** run Prisma migrations in a Docker build step or custom build command.
`postgres.railway.internal` is intended for Railway runtime networking, not image build-time.

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
