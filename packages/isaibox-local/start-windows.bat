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

docker compose up -d --build
echo Waiting for isaibox on http://127.0.0.1:%APP_PORT% ...
set READY=0
for /l %%I in (1,1,60) do (
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
echo Cache limit: %ISAIBOX_CACHE_LIMIT_GB% GB
pause
