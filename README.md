# Aura Apex Supreme Telegram Bot

## Overview
- Purpose: Discover high‑intent leads in groups and send compliant, context‑aware DMs.
- Core strategy: Scout‑then‑Strike with strict qualification gates and anti‑ban safeguards.
- Login: Persistent via Telethon StringSession; no interactive codes required in production.

## Architecture
- Entrypoint: [aura_apex_supreme.py](file:///c:/Users/owner/Desktop/Telegram%20Bot/aura_apex_supreme.py)
- Support:
  - Config & env: [config.py](file:///c:/Users/owner/Desktop/Telegram%20Bot/config.py)
  - Utilities & state: [aura_core.py](file:///c:/Users/owner/Desktop/Telegram%20Bot/aura_core.py)
  - Keep‑alive web server: [keep_alive.py](file:///c:/Users/owner/Desktop/Telegram%20Bot/keep_alive.py)
  - Session generator (local): [session_gen.py](file:///c:/Users/owner/Desktop/Telegram%20Bot/session_gen.py)
- Deployment:
  - Dockerfile builder: [Dockerfile](file:///c:/Users/owner/Desktop/Telegram%20Bot/Dockerfile)
  - Web service command: [Procfile](file:///c:/Users/owner/Desktop/Telegram%20Bot/Procfile)

## Environment
- .env keys:
  - API_ID, API_HASH, PHONE_NUMBER
  - SESSION_STRING (recommended; generated locally, pasted in server env)
  - GROQ_API_KEY (optional, AI composing)
  - MARKET (optional: en-UK, en-US, es-ES, it-IT, de-DE, fr-FR)
  - SKIP_WARMUP (optional: set to '1' to bypass initial warm-up wait)
  - AURA_MODE (optional: 'testing' for faster intervals, 'production' for safety)
  - AURA_WARMUP_SECONDS (optional: override default warm-up duration)
- Koyeb:
  - Set env in Service → Settings → Environment variables and files
  - Exposed port: 8080

## Login Flow
- Prefers SESSION_STRING:
  - Client init switches to `StringSession(SESSION_STRING)` if present
  - Start: `await client.start()` (no code)
- Fallback (local/dev):
  - Phone + `_code_callback` reading `WAITING_FOR_CODE` or TELEGRAM_CODE
- References: [init & start](file:///c:/Users/owner/Desktop/Telegram%20Bot/aura_apex_supreme.py#L466-L477), [file:///c:/Users/owner/Desktop/Telegram%20Bot/aura_apex_supreme.py#L1455-L1470]

## Keep‑Alive Web Server
- Flask app for health and login code submission
  - `/` returns a status string
  - `/health` returns 200; use for uptime monitors
  - `/code` accepts POSTed login codes when not using SESSION_STRING
- References: [keep_alive.py](file:///c:/Users/owner/Desktop/Telegram%20Bot/keep_alive.py)

## Discovery & Queuing
- Listens to group messages; scores intent using keyword matrices and market additions.
- Queues a handshake for high intent or urgency with a human‑like delay (5–15 min).
- References: [watchlist/handshake queue](file:///c:/Users/owner/Desktop/Telegram%20Bot/aura_apex_supreme.py#L1016-L1052)

## Qualification Gates
- Required before DM:
  - @username present
  - Profile photo present
  - Mutual groups count ≥1 (contextual DM)
  - Last seen “Recently” or within 4 days
  - Not opted out; not in active conversation
  - Human hours window 09:00–21:00 local; below daily cap
- References: [gates](file:///c:/Users/owner/Desktop/Telegram%20Bot/aura_apex_supreme.py#L1094-L1140)

## DM Compose & Language
- Persona: expert / peer / concise; AI via Groq when configured, else curated templates.
- Context: References group title and recent chatter (problems/competitors).
- Language: Detects Cyrillic or snippet language and translates with deep_translator.
- Spintax: Ensures ≥30% difference vs last DM via synonym swaps and minor endings.
- References: [compose](file:///c:/Users/owner/Desktop/Telegram%20Bot/aura_apex_supreme.py#L1159-L1213), [language helpers](file:///c:/Users/owner/Desktop/Telegram%20Bot/aura_apex_supreme.py#L480-L508), [spintax](file:///c:/Users/owner/Desktop/Telegram%20Bot/aura_apex_supreme.py#L485-L525)
- Rebrand CTA: Briefly introduces app rebranding aligned to lead intent, explicitly mentioning visual branding (logo, color scheme). Ends with player question (e.g., TiviMate) to tie the rebrand to a concrete setup path. References: detection and injection [aura_apex_supreme.py:L1167-L1171](file:///c:/Users/owner/Desktop/Telegram%20Bot/aura_apex_supreme.py#L1167-L1171), persona prompts [aura_apex_supreme.py:L1172-L1188](file:///c:/Users/owner/Desktop/Telegram%20Bot/aura_apex_supreme.py#L1172-L1188), draft reinforcement [aura_apex_supreme.py:L1199-L1204](file:///c:/Users/owner/Desktop/Telegram%20Bot/aura_apex_supreme.py#L1199-L1204), non‑AI fallback [aura_apex_supreme.py:L1205-L1216](file:///c:/Users/owner/Desktop/Telegram%20Bot/aura_apex_supreme.py#L1205-L1216)

## Daily Caps & Throttling
- Cap: 25 DMs if account is Premium, else 10
- Human‑like pacing between DMs: 15–20 minutes
- FloodWait handling with backoff
- References: [cap logic](file:///c:/Users/owner/Desktop/Telegram%20Bot/aura_apex_supreme.py#L1085-L1092)

## Human Handoff
- On private reply:
  - Marks conversation responded/converted
  - Alerts “me” to take over; stops automated replies for that lead
- References: [inbound DM handler](file:///c:/Users/owner/Desktop/Telegram%20Bot/aura_apex_supreme.py#L956-L991)

## Commands
- /health: status report
- /sleep <hours>: pause outreach
- /reset_persona: refresh persona hooks
- /export: send the SQLite DB to “Saved Messages”
- References: [commands](file:///c:/Users/owner/Desktop/Telegram%20Bot/aura_apex_supreme.py#L1143-L1189)

## Data & Persistence
- SQLite tables:
  - leads, joined_groups, prospects, keywords, activity_log
- JSON stats: `supreme_stats.json` tracks counters and QC groups
- References: [DB init](file:///c:/Users/owner/Desktop/Telegram%20Bot/aura_apex_supreme.py#L238-L296)

## Deployment (Koyeb)
- Source: GitHub with Dockerfile builder
- Web service listens on 8080; health routed to `/health`
- Free tier considerations:
  - Scale‑to‑zero on idle traffic; keep alive via external uptime monitor hitting `/health`
- References: [Dockerfile](file:///c:/Users/owner/Desktop/Telegram%20Bot/Dockerfile), [Procfile](file:///c:/Users/owner/Desktop/Telegram%20Bot/Procfile)

## Troubleshooting
- “Deep sleep”: add uptime monitor every 5 minutes to `/health`
- “Privacy forbids DM”: gate logs “privacy/invalid peer” and skips
- FloodWait: automatic backoff and retry

## Security
- Do not commit secrets; use env variables in Koyeb settings
- SESSION_STRING is sensitive; treat it like a password

## Local Session Generation
- Run: `python session_gen.py` locally
- Submit login code via `/code` or TELEGRAM_CODE env; copy printed SESSION_STRING
- References: [session_gen.py](file:///c:/Users/owner/Desktop/Telegram%20Bot/session_gen.py)
