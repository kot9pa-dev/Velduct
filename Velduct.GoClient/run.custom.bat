@echo off
setlocal
cd /d "%~dp0"

REM -------------------------------------------------------
REM Пример запуска с полным override через переменные среды.
REM Редактируй значения ниже под своё окружение.
REM config.json при этом НЕ нужен — env vars перекроют всё.
REM -------------------------------------------------------

set SERVER_URL=ws://192.168.1.10:5050/ws
set JWT_KEY=your-secret-key-minimum-32-characters
set JWT_ISSUER=my-client-name

REM Опционально — дополнительная настройка
REM set MAX_CREDITS=8
REM set CHUNK_SIZE_BYTES=4194304
REM set PPROF_ADDRESS=

taskkill /f /im client.exe /t >nul 2>&1

echo [1/2] Building...
go build -o client.exe .

if %ERRORLEVEL% NEQ 0 (
    echo Build failed!
    pause
    exit /b %ERRORLEVEL%
)

echo [2/2] Running with env override...
echo   SERVER_URL  = %SERVER_URL%
echo   JWT_ISSUER  = %JWT_ISSUER%
client.exe

pause
