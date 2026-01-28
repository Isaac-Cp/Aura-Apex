@echo off
title IPTV Lead Gen Bot
color 0A

:loop
echo Starting Telegram Bot...
python aura_apex_supreme.py
echo.
echo ⚠️ Bot stopped or crashed!
echo 🔄 Restarting in 10 seconds... (Press Ctrl+C to Exit)
timeout /t 10
goto loop
