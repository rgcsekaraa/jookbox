@echo off
setlocal
cd /d "%~dp0"

set APP_PORT=6789
if exist .env (
  for /f "tokens=1,2 delims==" %%A in (.env) do (
    if /I "%%A"=="APP_PORT" set APP_PORT=%%B
  )
)

curl -fsS -X POST "http://127.0.0.1:%APP_PORT%/api/cache/trim" -H "Content-Type: application/json" -d "{\"force\":true}"
echo.
echo Cache trim complete.
pause
