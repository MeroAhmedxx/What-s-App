# Altahhan CRM V4 — MySQL + SmarterASP

This package includes the final V4 additions requested:
- Chat attachments: image + PDF
- Reply in chat
- Admin-created chat channels
- Current Clients tab
- Morocco clients file bundled and importable
- Client invoices / attachments
- Popup + sound notifications
- Weekly / monthly reports with Excel export

## Default login
- Username: `admin`
- Password: `Admin@123`

## Key pages
- `/chat`
- `/current-clients`
- `/reports`

## SmarterASP publish
1. Create the MySQL database in SmarterASP.
2. Upload all files to the site root.
3. Import `mysql_schema.sql`.
4. Import `mysql_seed.sql`.
5. Edit `crm_settings.json` with your MySQL values.
6. Ensure these paths are writable:
   - `uploads/`
   - `uploads/chat/`
   - `uploads/clients/`
   - `logs/`
   - `crm_settings.json`
7. Confirm the Python path in `web.config` matches SmarterASP Python.
8. Browse the site and test login.

## Optional
If you want to re-sync the bundled Morocco file later, login as admin and open:
- Current Clients → `Sync Morocco file`
