@echo off
setlocal
cd /d "%~dp0"
taskkill /f /im client.exe /t >nul 2>&1

echo [1/2] Building...
go build -o client.exe .

if %ERRORLEVEL% NEQ 0 (
    echo Build failed!
    pause
    exit /b %ERRORLEVEL%
)

echo [2/2] Running with dev config...
client.exe -config config.dev.json

pause
