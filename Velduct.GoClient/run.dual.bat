@echo off
setlocal
cd /d "%~dp0"
taskkill /f /im client.exe /t >nul 2>&1

echo [1/3] Building...
go build -o client.exe .

if %ERRORLEVEL% NEQ 0 (
    echo Build failed!
    pause
    exit /b %ERRORLEVEL%
)

echo [2/3] Starting Client 1 (config.dev.json  ^| G:\TestShared1 ^| port 6060)...
start "VelductClient1" client.exe -config config.dev.json

echo [3/3] Starting Client 2 (config.dev2.json ^| G:\TestShared2 ^| port 6061)...
start "VelductClient2" client.exe -config config.dev2.json

echo Both clients started.
