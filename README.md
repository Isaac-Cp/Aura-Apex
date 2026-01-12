# Telegram IPTV Lead Gen Bot (2026 Pro Edition)

## Setup
1. **Install Dependencies**: `pip install telethon cryptg python-dotenv`
2. **Configuration**: Fill in `.env` with `API_ID`, `API_HASH`, and `PHONE_NUMBER`.
3. **Run**: `python main.py`

## Features
- **Intent-Scoring**: Only alerts on high-quality leads (Score >= 2).
- **Golden Lead Filter**: Marks urgent leads (asap, today) with 🔴.
- **High-Traffic Flag**: Marks leads during prime time/weekends with ⚡.
- **Auto-Discovery**: Safely joins 5 relevant groups/day.
- **Conversational Ghost**: Auto-reacts to messages to maintain "Human" status.

## Professional Lead Response (The 2026 Script)
When you receive an alert, **do not** send a generic spam message. Use this "Community Member" approach:

> "Hey [Name]! I just saw your message in the [Group Name] group. I’m also a member there. I actually went through the same struggle with [Problem mentioned] until I switched to a stable 4K setup. If you're still looking, I can get you a trial line to test for tonight's game. No pressure, just happy to help a fellow group member!"

## Reputation & Safety Protocol
To avoid bans in 2026:
- **Warm-Up**: For the first week, manually reply to 2-3 normal discussions for every group the bot joins.
- **Profile**: Ensure you have a professional Bio (e.g., "Streaming Consultant") and a real Profile Picture.
- **Privacy**: Hide your Phone Number in Telegram Settings.

## Final Checklist
- [ ] **Spam Check**: Message `@SpamBot` on Telegram to ensure no restrictions.
- **VPS Deployment**: Use the systemd service described below for 24/7 uptime.

## Persistence (VPS/Linux)
Create `/etc/systemd/system/telegram_bot.service`:
```ini
[Unit]
Description=Telegram IPTV Bot
After=network.target

[Service]
User=root
WorkingDirectory=/path/to/bot
ExecStart=/usr/bin/python3 main.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```
