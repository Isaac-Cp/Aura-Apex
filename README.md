# Aura Apex Supreme

Production-ready Telegram automation suite for discovery, outreach, and content curation.

## Prerequisites
- Python 3.11+ on Windows
- A Telegram API ID/API Hash
- A session string or phone login
- Optional: GROQ_API_KEY for prompt brief generation
- Optional: IMAGE_GEN_ENDPOINT + IMAGE_GEN_API_KEY for AI image generation

## Setup
1. Create a virtual environment and install dependencies:
   - `python -m venv .venv && .\.venv\Scripts\Activate.ps1`
   - `pip install -r requirements.txt`
2. Copy `.env.example` to `.env` and fill values. Do not commit `.env`.
3. Verify syntax:
   - `python -m py_compile aura_apex_supreme.py aura_curator.py`

## Running
- Outreach bot:
  - `python aura_apex_supreme.py`
  - Env flags:
    - `STOP_OUTREACH=1` to disable DMs
    - `RUN_PERF_40M=1` to enable 40‑minute performance monitor
- Curator:
  - `python -c "import asyncio; from aura_curator import curator_loop; asyncio.run(curator_loop())"`

## Image Generation
- Configure `IMAGE_GEN_ENDPOINT` to an HTTP API returning image bytes or JSON `{ image_base64 }`.
- If not set, curator uses branded Pillow templates.

## Safety
- `.gitignore` excludes `.env`, logs, perf files, and image artifacts.
- Do not commit secrets.

## Commands
- `/source_kpi` in DM to get discovery source KPI report
- `/health` bot status

