import asyncio
import json
import logging
import os
import random
import re
import time
import sqlite3

logger = logging.getLogger(__name__)


def load_json(path, default):
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return default
    return default


def save_json(path, data):
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f)
    except Exception as e:
        logger.error(f"JSON Save Error: {e}")


def should_outreach(probability=0.6):
    try:
        return random.random() < float(probability)
    except Exception:
        return False


async def proxy_health_monitor(client, proxy_file, interval=3600):
    while True:
        try:
            if not os.path.exists(proxy_file):
                await asyncio.sleep(interval)
                continue
            await client.get_me()
        except Exception as e:
            logger.error(f"Proxy health check failed: {e}")
        await asyncio.sleep(interval)


def keep_alive():
    try:
        _ = time.time()
    except Exception:
        pass

def clean_old_logs(db_path, days=7):
    try:
        conn = sqlite3.connect(db_path, timeout=60)
        c = conn.cursor()
        try:
            c.execute("DELETE FROM prospects WHERE datetime(message_ts) < datetime('now', ?)", (f'-{int(days)} days',))
        except Exception:
            pass
        try:
            c.execute("DELETE FROM activity_log WHERE datetime(ts) < datetime('now', ?)", (f'-{int(days)} days',))
        except Exception:
            pass
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Cleanup Error: {e}")


def detect_language_from_text(text):
    try:
        if not text:
            return None
        if re.search(r'[Ð-Ð¯Ð°-ÑÐÑ‘]', text):
            return "ru"
        return None
    except Exception:
        return None

# --- Advanced Lead Scoring Engine (2026 Update) ---

# Refined Keyword Structure
REBRAND_KEYWORDS = [
    "rebrand", "white label", "white-label", "custom logo",
    "hardcode", "dns edit", "apk edit", "brandable", "private label",
    "own apk", "custom apk", "hardcoded dns", "tivimate edit",
    "custom ott player", "rebranded xciptv", "apk panel", "private brand player"
]

URGENCY_KEYWORDS = [
    "buffering", "isp block", "blocked", "not working",
    "setup help", "dns issue", "connection error", "connection failed",
    "login issue", "url help", "401 error", "portal not working"
]

COMMERCIAL_KEYWORDS = [
    "price", "cost", "how much", "compare", "vs", "subscription",
    "best service", "buy", "pricing"
]

COMPETITOR_KEYWORDS = [
    "tivimate", "smarters", "xciptv", "ott navigator", "implayer",
    "iboplayer", "purple player", "televizo"
]

NEGATIVE_TRIGGERS = [
    "tired of", "sick of", "trash", "bad service", "sucks", "horrible",
    "unreliable", "scam", "waste of money"
]

def calculate_lead_score(message_text, user_data):
    """
    Calculates a lead score based on keywords, user quality, and sentiment.
    Returns an integer score.
    """
    score = 0
    if not message_text:
        return 0
    text = message_text.lower()

    # 1. HIGH INTENT KEYWORDS (+7 points)
    if any(word in text for word in REBRAND_KEYWORDS):
        score += 7

    # 2. PAIN POINT KEYWORDS (+3 points)
    if any(word in text for word in URGENCY_KEYWORDS):
        score += 3

    # 3. COMPETITOR MENTIONS (+2 points)
    if any(word in text for word in COMPETITOR_KEYWORDS):
        score += 2
        
    # 4. COMMERCIAL INTENT (+4 points)
    if any(word in text for word in COMMERCIAL_KEYWORDS):
        score += 4

    # 5. NEGATIVE SENTIMENT (+2 points)
    if any(word in text for word in NEGATIVE_TRIGGERS):
        score += 2

    # 6. USER QUALITY BONUSES (Multipliers)
    if user_data:
        # Give points for having a username (shows an active/real user)
        if hasattr(user_data, 'username') and user_data.username:
            score += 2
        
        # Give points for being Premium (shows they have money/intent)
        if hasattr(user_data, 'premium') and user_data.premium:
            score += 3

    return score
