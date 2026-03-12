# 🛰️ Aura Apex Ecosystem (V2.1)

An advanced, autonomous Telegram automation suite designed for IPTV content curation, technical prospect identification, and AI-driven outreach.

## 🚀 Overview

The Aura Apex Ecosystem is a multi-modular system powered by Llama 3.3 (via Groq) and specialized technical logic. It handles the entire lifecycle of a Telegram-based IPTV brand—from sourcing industry news to engaging high-value prospects.

### 🏗️ Core Architecture

- **`main.py`**: The central Process Manager. It monitors sub-processes (`aura_curator.py`, `aura_apex_supreme.py`) and ensures 24/7 uptime by automatically restarting crashed modules.
- **`aura_curator.py`**: The "News Engine". It scrapes technical IPTV sources (TroyPoint, IPTVWire, Reddit, etc.), classifies content, generates AI-enhanced summaries, and posts them to your channel during scheduled market windows.
- **`aura_apex_supreme.py`**: The "Outreach Engine". It identifies potential leads in monitored groups using technical sentiment analysis, performs "Heuristic Handshakes" (reactions), and sends personalized, technical DMs.
- **`keep_alive.py`**: A Flask-based Dashboard and health monitor. Provides real-time stats, log access, and a web interface for lead management.
- **`aura_core.py`**: Shared logic, including the 2026 Lead Scoring engine, high-value topic detection, and database utilities.
- **`config.py`**: Dynamic rules engine that reloads dashboard settings (keywords, tags, sources) without requiring a restart.

## 🛠️ Key Features

- **Lead Scoring (2026 Engine)**: Evaluates messages based on technical pain points (buffering, ISP throttling, MTU issues) rather than simple keywords.
- **Aiden Persona**: AI outreach uses a specialized "technical peer" persona—helpful, skeptical, and focused on hardcoded stability.
- **Market Windows**: Curator posts are scheduled according to industry-standard "hot times" (e.g., 9:00 AM News, 7:00 PM Fixes).
- **Fortress Hardening**: Implements hardware spoofing, randomized growth patterns, and "Heuristic Handshakes" to protect Telegram accounts from spam filters.
- **Dynamic Rules**: All filtering and classification keywords are managed via the web dashboard and updated in real-time.

## 📥 Installation

1.  **Clone & Install**:
    ```bash
    git clone <repo-url>
    pip install -r requirements.txt
    ```
2.  **Environment Setup**:
    Configure the `.env` file with your API credentials:
    - `API_ID`, `API_HASH`, `SESSION_STRING` (Telethon)
    - `GROQ_API_KEY` (AI Content)
    - `AURA_MODE` (Set to `production` or `testing`)
    - `AURA_AI_SAVINGS` (Set to `1` to optimize API costs)

3.  **Run**:
    ```bash
    python main.py
    ```

## 📊 Dashboard

Access the management interface at `http://localhost:8080`.
- **Overview**: Real-time conversion rates and lead tracking.
- **Intelligence**: Adjust keywords and scouting sources on the fly.
- **Logs**: Monitor system health and AI activity.

## 🛡️ Security & Privacy

- **Single-Contact Enforcement**: The system strictly ensures no lead is messaged more than once across its lifetime.
- **Data Hardening**: All lead data is stored in an encrypted-at-rest SQLite database (`gold_leads.db`).
- **Stealth Mode**: outreach includes randomized delays and "quiet hours" to mimic human behavior.

---
**Maintained by**: Aura Apex Technical Team (2026 Lifecycle)
