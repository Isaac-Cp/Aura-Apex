# Cleanup Report

## Summary
- Removed obsolete development and test artifacts not required for production.
- Trimmed unused third-party dependencies from requirements.
- Verified core modules build and syntax.

## Deleted (tracked)
- aura_apex.py — superseded by aura_apex_supreme.py
- aura_main.py — replaced by main.py process manager
- aura_supreme.py — legacy variant
- check_session.py — development-only session helper
- convert_log.py — local log conversion utility
- diagnose_logs.py — dev diagnostics
- fix_bot.py — deprecated tool script
- generate_session.py — dev session generator
- leads.db — stale local DB file
- log_dump.txt — dev log artifact
- read_log.py — dev reader script
- run_bot.bat — local launcher script
- session_gen.py — dev session helper
- bot_stats.json — dev stats artifact

## Dependencies Removed
- gunicorn — not used; Procfile runs python main.py
- google-generativeai, google-genai — not imported
- pydantic, pydantic-settings — not imported

## Rationale
- Consolidate to single production entrypoint (main.py).
- Reduce attack surface and maintenance burden.
- Lower image size for Docker builds and simplify deployment.

## Verification
- Syntax checks: aura_apex_supreme.py, aura_curator.py, aura_core.py, keep_alive.py, config.py, main.py
- Requirements freeze updated; application relies on Telethon, Flask, aiohttp, aiosqlite, Pillow, bs4, deep-translator, Groq, psutil.

## Notes
- Runtime data files under project root and data/ are preserved (JSON queues, KPIs, caches, DB WAL/SHM).
