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
- CURATOR_CHANNEL_ID or TARGET_CHANNEL_ID, or CHANNEL_INVITE_LINK (for curator posting)
- CURATOR_AUDIT_DM: 1 | 0 (send curator daily audit to Saved Messages)

## Run
- Start both services: `python main.py`
- Run sniper only: `python aura_apex_supreme.py`
- Health: `/health` in private chat
- Export DB: `/export` in private chat
 
## Admin Commands
- `/status` in private chat: CPU load, Rich Groups, Unique DMs, Spam Shielded, QC Groups, Day
- `/find <query>` in private chat: searches `search_index.json` and returns short matches
 
## High-Value Auto‑DM
- The bot identifies high‑value leads in groups using intent and market signals
- It queues a handshake and sends a short DM automatically during human hours
- DM caps and FloodWait safeguards are applied per account trust state
 
### Configuration
- Ensure `STOP_OUTREACH=0` and `AURA_MODE=production` in `.env`
- Set `MARKET` to match your audience (e.g., `en-US`)
- Provide `SESSION_STRING` for stable login; `GROQ_API_KEY` enables AI composition
 
### Session Generation
- Regenerate a session string: `python generate_session.py`
- Copy the printed string into `.env` as `SESSION_STRING` (single line)
 
### DM Examples
- “hey — saw your note in the match thread. likely a dns/portal handshake issue. want a quick fix?”
- “quick one — buffering every few mins usually means hardware accel fighting the OS. switch decoder to software and test. want the steps?”
- “yo — if your xtream login keeps expiring, reduce epg refresh and pin dns. want a stable test line?” 

## Safety
- DM caps vary by account/premium status
- FloodWait errors auto-sleep; PeerFlood stops outreach
- No links in first message; always reference the group/problem

## Performance
- `cryptg` enabled if present for C-based encryption acceleration
- Entity saving disabled to reduce SQLite overhead (`client.session.save_entities = False`)
- Lowered fetch limits and static UA fallback reduce CPU and I/O
 - Batched write queue using `aiosqlite` minimizes blocking database operations
 
## Notes
- `.gitignore` excludes `.env`, DB, session files, and runtime stats
- Market keywords extend scouting; scoring drives AI tone and outreach
- Curator posts use the Aiden persona and auto‑watermark images with `brand/ax_logo.png`
- Toggle curator daily audit DMs via `CURATOR_AUDIT_DM=1|0`
- Lead capture writes to `leads.json` when group messages include: buffer, rebrand, dns help, provider down, looking for fix
