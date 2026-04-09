# Host Deployment Package

## Recommended host setup
- Python 3.11
- MySQL database
- Install from `requirements.txt`
- Set the public domain from **Domain & Images** inside the app
- Set DB server credentials from **DB Settings** inside the app

## Option A: Gunicorn / VPS / PaaS
```bash
pip install -r requirements.txt
gunicorn -k uvicorn.workers.UvicornWorker -w 2 -b 0.0.0.0:8000 app:app
```

## Option B: Passenger shared hosting
Use `passenger_wsgi.py` as the startup file after installing requirements.

## Upload checklist
- upload the full folder contents
- keep `templates/`, `static/`, and `uploads/` together with `app.py`
- create write permission for `uploads/` and `crm_settings.json`
- for hosted MySQL switch DB driver to **MySQL Server** from the in-app DB Settings page

## Important
When you change DB or domain settings from the app, the app writes them into `crm_settings.json`. Keep that file writable by the hosting user.


## SmarterASP.NET deployment
- SmarterASP.NET offers Python hosting on Windows Server and supports Python 3.x on eligible plans.
- IIS Python apps are configured through `web.config`; Microsoft documents both HttpPlatformHandler and FastCGI, while noting HttpPlatformHandler is preferred and FastCGI via wfastcgi remains an option.
- This package includes `iis_wsgi.py` plus `web.config` so the FastAPI ASGI app is exposed as WSGI through `a2wsgi` for IIS/FastCGI.
- If SmarterASP uses a different Python path than `C:\Python313\python.exe`, edit the `scriptProcessor` line in `web.config` from the hosting control panel path shown by the provider.
- Create a writable `logs/` folder and keep `uploads/` and `crm_settings.json` writable.
