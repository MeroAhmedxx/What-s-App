# TradeFlow CRM - Altahhan Edition - V11 Final

## What was added in this round

- Public **Intro page** at `/` for management-facing presentation.
- New public **Product Tour** page at `/tour` for demo flow and product storytelling.
- New **unified workspace search** at `/workspace-search`.
- Stronger **EN / AR translation hooks** with explicit translation attributes on key pages.
- Better **header navigation** with Intro, Product Tour, Search, and clearer workspace separation.
- Sharper **dashboard storytelling** for management presentation.
- Stronger **Outreach Hub** page with lead-to-email flow and auto-email roadmap.
- Safer **bootstrap admin setup**:
  - if no existing DB/admin exists and no `INITIAL_ADMIN_PASSWORD` is set,
  - the app now generates a one-time password,
  - and writes it to `bootstrap_admin.txt`.

## Main routes to use in demo

- `/` -> intro page
- `/tour` -> product tour
- `/login` -> login
- `/dashboard` -> executive dashboard
- `/email-tool` -> outreach hub
- `/workspace-search?q=...` -> unified search

## Best demo order

1. Intro page
2. Product Tour
3. Login
4. Dashboard
5. Leads
6. Outreach Hub
7. Shipments
8. Trade Library

## Important setup note

For production, set:

- `INITIAL_ADMIN_USERNAME`
- `INITIAL_ADMIN_PASSWORD`
- database environment variables
- mail profile / bridge settings

If `INITIAL_ADMIN_PASSWORD` is not set on first run, check `bootstrap_admin.txt` after startup.
