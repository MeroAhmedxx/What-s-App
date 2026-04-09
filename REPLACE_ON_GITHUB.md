Replace these files in your GitHub repo root:

- app.py
- requirements.txt
- render.yaml
- templates/login.html

Then do this:
1. Remove any remaining `Admin@123` text from all README files.
2. Make the GitHub repository Private.
3. In Render, add these Environment Variables:
   - CRM_DB_URL
   - SECRET_KEY
   - TRACKING_BASE_URL
   - COOKIE_SECURE=true
   - RENDER=true
   - INITIAL_ADMIN_USERNAME
   - INITIAL_ADMIN_PASSWORD
4. Deploy latest commit.
5. Log in with the admin account you set in Render.
6. Create a normal employee account and stop sharing admin.

Notes:
- This package blocks local SQLite fallback on Render.
- It hides the database URL from `/server-info`.
- It removes the default login hint from `login.html`.
- It hardens cookies for HTTPS.
- It bootstraps the first admin from environment variables on a new database.
