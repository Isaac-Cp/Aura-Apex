
import os
import json
from dotenv import load_dotenv

load_dotenv()

def load_json_config(path, default):
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return default
    return default

# Load rules from external JSON
RULES_PATH = os.path.join("data", "rules.json")

def get_rules():
    """Dynamically load and return the latest rules from rules.json."""
    return load_json_config(RULES_PATH, {})

API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
PHONE_NUMBER = os.getenv("PHONE_NUMBER")
SESSION_STRING = os.getenv("SESSION_STRING")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
CURATOR_CHANNEL_ID = os.getenv("CURATOR_CHANNEL_ID")
TARGET_CHANNEL_ID = os.getenv("TARGET_CHANNEL_ID")
CHANNEL_INVITE_LINK = os.getenv("CHANNEL_INVITE_LINK")
ADMIN_LEADS_CHANNEL_ID = os.getenv("ADMIN_LEADS_CHANNEL_ID")
DB_FILE = os.getenv("DB_FILE", "gold_leads.db")
WAITING_FOR_CODE_FILE = "WAITING_FOR_CODE"
BLACKLIST_FILE = "blacklist.txt"
KEEP_ALIVE_SECRET = os.getenv("KEEP_ALIVE_SECRET", "change_me")
PORT = int(os.getenv("PORT", 8080))

# Static fallbacks for initial load, but functions should use get_rules()
rules = get_rules()
BANNED_ZONES = rules.get("BANNED_ZONES", [])
BANNED_CURRENCIES = rules.get("BANNED_CURRENCIES", [])
JUNK_KEYWORDS = rules.get("JUNK_KEYWORDS", [])
TIER_3_CODES = rules.get("TIER_3_CODES", [])
TIER_1_INDICATORS = rules.get("TIER_1_INDICATORS", [])
URGENCY_KEYWORDS = rules.get("URGENCY_KEYWORDS", [])
SENTIMENT_BLACKLIST = rules.get("SENTIMENT_BLACKLIST", [])
REBRAND_KEYWORDS = rules.get("REBRAND_KEYWORDS", [])
COMMERCIAL_KEYWORDS = rules.get("COMMERCIAL_KEYWORDS", [])
COMPETITOR_KEYWORDS = rules.get("COMPETITOR_KEYWORDS", [])
NEGATIVE_TRIGGERS = rules.get("NEGATIVE_TRIGGERS", [])
BUYER_PAIN_KEYWORDS = rules.get("BUYER_PAIN_KEYWORDS", [])
SELLER_SHIELD_TERMS = rules.get("SELLER_SHIELD_TERMS", [])
ESSENTIAL_HASHTAGS = rules.get("ESSENTIAL_HASHTAGS", [])
GUIDE_KEYWORDS = rules.get("GUIDE_KEYWORDS", [])
FIX_KEYWORDS = rules.get("FIX_KEYWORDS", [])
NEWS_KEYWORDS = rules.get("NEWS_KEYWORDS", [])
MARKET_KEYWORDS = rules.get("MARKET_KEYWORDS", {})

REQUEST_TIMEOUT = 30
CHECK_INTERVAL_SECONDS = 300

BRAND_COLORS = [(12, 20, 35), (22, 32, 48), (255, 215, 0)]

PLATFORM_SPECS = {
    "4k": (3840, 2160),
    "instagram_square": (1080, 1080),
    "instagram_portrait": (1080, 1350),
    "instagram_landscape": (1080, 566),
    "twitter_large": (1200, 675),
    "twitter_small": (600, 335),
    "facebook_large": (1200, 630),
    "facebook_square": (1080, 1080)
}

PRO_TIPS = [
    "Switch decoder to Software when OS updates cause stutter.",
    "Use a stable DNS; avoid public resolvers that leak and flap.",
    "Keep EPG refresh at 24h to prevent provider rate-limit spikes.",
    "Prefer wired Ethernet for 4K; Wi‑Fi jitter ruins handshake stability."
]

if not API_ID or not API_HASH or not PHONE_NUMBER:
    print("Error: Missing API_ID, API_HASH, or PHONE_NUMBER in .env file.")
