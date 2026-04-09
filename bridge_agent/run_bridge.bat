@echo off
cd /d %~dp0
if not exist agent.py (echo agent.py not found & pause & exit /b 1)
if "%BASE_URL%"=="" set BASE_URL=https://your-render-app.onrender.com
if "%CRM_DEVICE_TOKEN%"=="" set CRM_DEVICE_TOKEN=PASTE_DEVICE_TOKEN_HERE
python -m pip install -r requirements.txt
python agent.py
pause
