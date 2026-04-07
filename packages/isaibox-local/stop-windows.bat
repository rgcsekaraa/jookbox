@echo off
setlocal
cd /d "%~dp0"

where docker >nul 2>nul
if errorlevel 1 (
  echo Docker Desktop is required to stop this package.
  pause
  exit /b 1
)

docker compose down
echo isaibox local stopped.
pause
