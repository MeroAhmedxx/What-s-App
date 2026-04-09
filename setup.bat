@echo off
setlocal
cd /d %~dp0
where python >nul 2>nul
if errorlevel 1 (
  echo Python was not found. Install Python 3.11+ and enable Add Python to PATH.
  pause
  exit /b 1
)

echo [1/5] Creating virtual environment...
python -m venv .venv
if errorlevel 1 (
  echo Failed to create virtual environment.
  pause
  exit /b 1
)
call .venv\Scripts\activate.bat
if errorlevel 1 (
  echo Failed to activate virtual environment.
  pause
  exit /b 1
)

echo [2/5] Upgrading pip...
python -m pip install --upgrade pip setuptools wheel
if errorlevel 1 (
  echo Failed to upgrade pip.
  pause
  exit /b 1
)

echo [3/5] Installing requirements...
pip install -r requirements.txt
if errorlevel 1 (
  echo Failed to install requirements.
  pause
  exit /b 1
)

echo [4/5] Installing Outlook bridge package (optional)...
pip install pywin32
if errorlevel 1 (
  echo pywin32 could not be installed. Outlook sending will not work until pywin32 is installed.
)

echo [5/5] Opening Windows Firewall TCP 8000 rule (optional but recommended)...
netsh advfirewall firewall add rule name="Altahhan CRM 8000" dir=in action=allow protocol=TCP localport=8000 >nul 2>nul

echo Setup finished.
echo Then run: run_server.bat
pause
