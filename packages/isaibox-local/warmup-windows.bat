@echo off
setlocal
cd /d "%~dp0"

set APP_PORT=6789
if exist .env (
  for /f "tokens=1,2 delims==" %%A in (.env) do (
    if /I "%%A"=="APP_PORT" set APP_PORT=%%B
  )
)

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

curl -fsS -X POST "http://127.0.0.1:%APP_PORT%/api/warmup" -H "Content-Type: application/json" -d "{\"limit\":24}"
echo.
echo Warmup complete.
pause
