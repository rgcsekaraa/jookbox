@echo off
setlocal
cd /d "%~dp0"

where docker >nul 2>nul
if errorlevel 1 (
  echo Docker Desktop is required. Install Docker Desktop for Windows, then run this launcher again.
  pause
  exit /b 1
)

set APP_PORT=6789
set ISAIBOX_CACHE_LIMIT_GB=20
if exist .env (
  for /f "tokens=1,2 delims==" %%A in (.env) do (
    if /I "%%A"=="APP_PORT" set APP_PORT=%%B
    if /I "%%A"=="ISAIBOX_CACHE_LIMIT_GB" set ISAIBOX_CACHE_LIMIT_GB=%%B
  )
)

if not exist app\data mkdir app\data
if not exist app\exports mkdir app\exports
if not exist app\.cache\audio mkdir app\.cache\audio

if not exist app\data\masstamilan.duckdb (
  echo Missing packaged database: app\data\masstamilan.duckdb
  pause
  exit /b 1
)

if not exist app\dist\index.html (
  echo Missing packaged frontend build: app\dist\index.html
  pause
  exit /b 1
)

docker compose up -d --build
echo Waiting for isaibox on http://127.0.0.1:%APP_PORT% ...
set READY=0
for /l %%I in (1,1,90) do (
  curl -fsS "http://127.0.0.1:%APP_PORT%/api/health" >nul 2>nul
  if not errorlevel 1 (
    set READY=1
    goto :warmup
  )
  timeout /t 1 >nul
)

:warmup
if "%READY%"=="0" (
  echo isaibox did not become ready in time.
  pause
  exit /b 1
)

curl -fsS -X POST "http://127.0.0.1:%APP_PORT%/api/warmup" -H "Content-Type: application/json" -d "{\"limit\":24}" >nul
start "" "http://127.0.0.1:%APP_PORT%/"
echo isaibox local is running on http://127.0.0.1:%APP_PORT%/
echo Frontend container port: 5173
echo Backend container port: 6060
echo Cache limit: %ISAIBOX_CACHE_LIMIT_GB% GB
pause
