@echo off
chcp 65001 > nul
cd /d %~dp0

if not exist logs mkdir logs

:LOOP
echo [%date% %time%] AlpacaTrader 시작 >> logs\trader.log
python alpaca_trader.py >> logs\trader.log 2>&1
echo [%date% %time%] 종료 (코드=%errorlevel%) -- 60초 후 재시작... >> logs\trader.log
timeout /t 60 /nobreak > nul
goto LOOP
