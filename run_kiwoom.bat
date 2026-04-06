@echo off
chcp 65001 > nul
cd /d "%~dp0"

:LOOP
echo [%date% %time%] KiwoomTrader 시작
python kiwoom_trader.py
echo [%date% %time%] 종료 (종료코드=%errorlevel%) -- 60초 후 재시작...
timeout /t 60 /nobreak > nul
goto LOOP
