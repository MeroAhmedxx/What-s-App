@echo off
setlocal
cd /d %~dp0
if not exist .venv\Scripts\activate.bat (
  echo Virtual environment was not found. Run setup.bat first.
  pause
  exit /b 1
)
call .venv\Scripts\activate.bat

for /f %%i in ('powershell -NoProfile -Command "$ips = Get-NetIPAddress -AddressFamily IPv4 ^| Where-Object { $_.IPAddress -notlike '127.*' -and $_.IPAddress -notlike '169.254*' -and $_.PrefixOrigin -ne 'WellKnown' }; ($ips ^| Sort-Object InterfaceMetric ^| Select-Object -First 1 -ExpandProperty IPAddress)"') do set LAN_IP=%%i
if "%LAN_IP%"=="" set LAN_IP=127.0.0.1
set TRACKING_BASE_URL=http://%LAN_IP%:8000

echo.
echo =========================================
echo   Altahhan CRM Release 3 Production
echo =========================================
echo This PC URL  : http://127.0.0.1:8000
echo Office URL   : http://%LAN_IP%:8000
echo Health check : http://%LAN_IP%:8000/healthz
echo =========================================
echo.

python -m uvicorn app:app --host 0.0.0.0 --port 8000
pause
