# Aura Apex Ecosystem

Advanced Telegram automation for IPTV content curation and prospect outreach.

## Components

- **main.py**: Process manager that keeps all bots running.
- **aura_curator.py**: Content discovery and channel management bot.
- **aura_apex_supreme.py**: Prospect identification and outreach bot.
- **aura_core.py**: Shared utilities and business logic.
- **config.py**: Configuration loader.
- **keep_alive.py**: Web server for health checks and auth callbacks.

## Setup

1. Create a `.env` file with your credentials (API_ID, API_HASH, PHONE_NUMBER, etc.).
2. Install dependencies: `pip install -r requirements.txt`.
3. Run the manager: `python main.py`.

## Data Storage

All data is stored in the `data/` directory and `gold_leads.db`.
