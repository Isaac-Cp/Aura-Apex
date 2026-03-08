# Operations

## Environment
- AURA_MODE=production|testing
- CURATOR_TZ=UTC (e.g., Europe/London)
- CURATOR_CONCURRENCY=3..5
- CURATOR_AI_MAX_PER_MIN=8 (cap LLM calls/min)
- CURATOR_BROWSER_FALLBACK=true (requires Playwright)
- CURATOR_HEARTBEAT_DM=true
- APEX_AI_MAX_PER_MIN=12
- SENTRY_DSN=... (optional)
- SENTRY_TRACES=0.0..1.0 (optional)

## Optional Dependencies
- TLS impersonation: `pip install curl_cffi`
  - Enabled automatically when installed.
- Browser fallback: `pip install playwright` then `playwright install chromium`
  - Enable via `CURATOR_BROWSER_FALLBACK=true`.

## CI
- GitHub Actions runs ruff, mypy (ignore missing imports), and pytest.

## Notes
- Feature flag sources in data/rules.json via `ENABLED_SOURCES`.
- Example:
- ```
- {
-   "ENABLED_SOURCES": ["TROYPOINT","IPTVWire","Guru99","AFTVnews","TorrentFreak","Reddit r/DetailedIPTV","Reddit r/TiviMate","Reddit r/IPTV"]
- }
- ```
- SQLite uses WAL. Indexes created for hot paths during init.
