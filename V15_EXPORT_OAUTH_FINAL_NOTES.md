# V15 Export + OAuth Final

This build continues from V13 and closes the biggest remaining product gaps without breaking the current CRM/Outreach flow.

## What was added

### 1) Export Engine
New routes:
- `/export`
- `/export/products`
- `/export/markets`
- `/export/agent`

New data layer:
- `export_products`
- `export_markets`
- `data/export_catalog.json`

The export catalog is seeded automatically on first run.

### 2) Product Settings / white-label layer
New route:
- `/product-settings`

This edits `product_profile.json` from inside the app so the same core can be rebranded for another company later.

### 3) Microsoft 365 OAuth groundwork
New routes:
- `/campaign-v2/oauth/microsoft/start`
- `/campaign-v2/oauth/microsoft/callback`

Profiles can now be connected to an OAuth account instead of only manual tokens.

### 4) Inbox Sync for replies
New route:
- `/campaign-v2/inbox-sync`

This pulls recent inbox messages through Microsoft Graph, matches sender emails against campaign recipients / sequence enrollments, logs reply events, and creates a high-priority task for follow-up.

### 5) Graph token usage improved
Website sending with `microsoft_graph` profiles can now try the linked OAuth account token first, then refresh it if a refresh token exists.

### 6) Unified Search expanded
Workspace search now scans:
- export products
- export markets

## What was tested locally
Tested successfully with a fresh auto-created SQLite DB:
- `/`
- `/dashboard`
- `/email-tool`
- `/campaign-v2/profiles`
- `/campaign-v2/inbox-sync`
- `/export`
- `/export/products`
- `/export/markets`
- `/export/agent`
- `/product-settings`
- `/workspace-search?q=Date`

Also tested:
- POST `/export/products/add`
- POST `/product-settings`

## What still needs real credentials to go fully live
These parts are implemented but need your actual Microsoft 365 / Azure values:
- `MICROSOFT_CLIENT_ID`
- `MICROSOFT_CLIENT_SECRET`
- `MICROSOFT_TENANT_ID`
- `MICROSOFT_REDIRECT_URI`

For OpenAI-backed agent generation, set:
- `OPENAI_API_KEY`

## Important packaging note
This zip is delivered **clean**:
- no committed SQLite DB
- no bootstrap admin text file
- no `__pycache__`

On first run, the app will generate the DB and bootstrap admin note again automatically.
