# TradeFlow CRM — Altahhan Edition — V12 Ultimate

## What this release adds
- A stronger Outreach Hub with **Campaigns + Sequences + Replies & Suppression** in one workspace.
- **Auto email sequences** with 1 to 3 steps, delay-hours per step, enrollment of leads, and a run-due-steps action.
- **Stop-on-reply / stop-on-click / stop-on-open** logic for sequences.
- **Manual reply sync groundwork** so users can record replies, bounces, and unsubscribes from the mailbox and instantly stop future outreach.
- **Suppression list** with reactivation support.
- **Public unsubscribe link** generation inside sent emails.
- **Tracking v2** for opens and clicks on campaigns and sequences.
- **Unified outreach navigation**: Campaigns, Sequences, Replies & Suppression, Profiles, Templates, Logs, Tracking, Bridge.
- Deeper **workspace search** now includes campaign V2 items and sequences.
- Stronger bilingual coverage for the new outreach navigation and main sequence / suppression concepts.

## Product value
This release moves the email area from a sending screen into a more sellable outreach product:
- one-off campaigns for offers and catalogs
- repeatable sequences for follow-up
- reply / bounce / unsubscribe control
- safer outreach behavior for future clients

## Important note
This ZIP is cleaned for delivery. Generated runtime files such as the local SQLite DB, bootstrap password file, and `__pycache__` are not included.
On first run, the app will generate the bootstrap admin account again if needed.

## Recommended next phase after V12
- Microsoft Graph or Gmail sync for real reply ingestion
- bounce webhooks / provider events
- lead verification / enrichment
- company-level multi-tenant data model
- stronger locale / country packs
