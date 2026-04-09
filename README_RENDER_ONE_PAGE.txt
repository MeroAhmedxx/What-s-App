Altahhan CRM - Render quick deploy

1) Create a GitHub repository.
2) Upload all project files to the repository.
3) Sign in to Render with GitHub.
4) New > Web Service > pick the repository.
5) Render will detect render.yaml automatically.
6) Click Create Web Service.
7) Wait for build to finish.

Important:
- This package uses SQLite by default unless CRM_DB_URL is set.
- On Render free plan, local files and SQLite are not durable.
- Good for testing/demo, not for permanent production data.
