Altahhan CRM Final Enterprise Build

What is included
- MySQL-ready FastAPI CRM for SmarterASP/IIS via wfastcgi
- Leads, tasks, announcements, campaigns, tracking, users
- Chat with channels, reply, image/PDF upload
- Current Clients tab seeded from Morocco clients.xlsx
- Client invoices with attachment upload
- Popup + sound notifications
- Weekly / monthly reporting with Excel export
- Global Agreements tab with searchable official trade agreements and Egypt trade-law resources

Default login
- username: admin
- password: Admin@123

Main routes
- /dashboard
- /leads
- /current-clients
- /chat
- /reports
- /agreements

MySQL deployment
1. Create a SmarterASP site that supports Python.
2. Create the MySQL database.
3. Upload all files to the website root.
4. Import mysql_schema.sql.
5. Import mysql_seed.sql.
6. Edit crm_settings.json with your MySQL and domain values.
7. Confirm web.config Python path matches the server Python path.
8. Give write permission to uploads/, logs/, and crm_settings.json.
9. Browse the site and check logs/wfastcgi.log if IIS shows an error.

Important note about trade-law content
- The Global Agreements module is seeded with a curated official reference set.
- It is meant to be searchable and expandable, not a legal substitute.
- Add more official PDF links/records later by inserting rows into trade_reference_items.
