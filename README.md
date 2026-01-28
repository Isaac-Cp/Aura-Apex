# Aura Apex Supreme (2026 Lead Sniper)

Short, human-like Telegram outreach for IPTV leads using:
- Logic-based lead scoring (pain/rebrand/competitor/commercial)
- Groq AI “Aiden” prompt with style mimicry
- FloodWait-safe DM wrapper and rate caps

## Setup
- Python 3.10+
- Install dependencies: `pip install -r requirements.txt`
- Create `.env` (see Environment). Do not commit secrets.

## Environment
- API_ID, API_HASH, PHONE_NUMBER or SESSION_STRING
- GROQ_API_KEY (optional for AI DMs)
- MARKET: en-US | en-UK | es-ES | it-IT | de-DE | fr-FR
- AURA_MODE: production | testing
- SKIP_WARMUP: 1 | 0
- STOP_OUTREACH: 1 | 0 (set 1 to pause all DMs)

## Run
- Start: `python aura_apex_supreme.py`
- Health: `/health` in private chat
- Export DB: `/export` in private chat

## Safety
- DM caps vary by account/premium status
- FloodWait errors auto-sleep; PeerFlood stops outreach
- No links in first message; always reference the group/problem

## Notes
- `.gitignore` excludes `.env`, DB, session files, and runtime stats
- Market keywords extend scouting; scoring drives AI tone and outreach
