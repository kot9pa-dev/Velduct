@echo off
setlocal
cd /d "%~dp0"
taskkill /f /im main.exe /t >nul 2>&1
taskkill /f /im go.exe /t >nul 2>&1
taskkill /f /im client.exe /t >nul 2>&1

echo [1/2] Checking dependencies and building...
go mod tidy
go build -o client.exe main.go

if %ERRORLEVEL% NEQ 0 (
    echo Build failed!
    pause
    exit /b %ERRORLEVEL%
)

echo [2/2] Running Go Client...
REM Для дев-конфига: client.exe -config config.dev.json
REM Или через env:  set CONFIG_PATH=config.dev.json && client.exe
client.exe

pause
