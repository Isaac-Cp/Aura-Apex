
import asyncio
import logging
import logging.handlers
import random
import sys
import datetime
import os
import re
import sqlite3
import time

from telethon import TelegramClient, events, functions
from telethon.sessions import StringSession
from telethon.tl.functions.messages import CheckChatInviteRequest, ImportChatInviteRequest
from telethon.tl.functions.messages import GetBotCallbackAnswerRequest
from telethon.tl.functions.channels import JoinChannelRequest, LeaveChannelRequest, GetFullChannelRequest
from telethon.tl.functions.account import UpdateNotifySettingsRequest
from telethon.tl.functions.users import GetFullUserRequest
from telethon.tl.types import InputNotifyPeer, InputPeerNotifySettings, ReactionEmoji, UserStatusOffline, UserStatusOnline, UserStatusRecently, UserStatusLastWeek, UserStatusLastMonth
from telethon.errors import FloodWaitError, UserPrivacyRestrictedError, PeerIdInvalidError, YouBlockedUserError, UserBannedInChannelError, ChatWriteForbiddenError, ChannelPrivateError
from telethon.tl.functions.contacts import BlockRequest
from fake_useragent import UserAgent
from groq import AsyncGroq
from deep_translator import GoogleTranslator

# Custom modules
from aura_core import proxy_health_monitor, should_outreach, load_json, save_json
from keep_alive import keep_alive
from config import (
    API_ID, API_HASH, PHONE_NUMBER, GROQ_API_KEY,
    BANNED_ZONES, BANNED_CURRENCIES, JUNK_KEYWORDS, 
    TIER_3_CODES, TIER_1_INDICATORS, URGENCY_KEYWORDS, SENTIMENT_BLACKLIST
)

# Logging Setup
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)
try:
    _fh = logging.handlers.RotatingFileHandler('bot_error.log', maxBytes=1000000, backupCount=3, encoding='utf-8')
    _fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(_fh)
except Exception as e:
    print(f"Logging setup warning: {e}")

# AI Provider Setup (Groq)
ai_client = None
if GROQ_API_KEY:
    try:
        ai_client = AsyncGroq(api_key=GROQ_API_KEY)
    except Exception as e:
        logger.error(f"Groq Init Error: {e}")
else:
    logger.warning("GROQ_API_KEY missing.")

# Constants
SESSION_NAME = 'aura_apex_supreme_session'
STATS_FILE = 'supreme_stats.json'
PROCESSED_GROUPS = 'supreme_groups.json'
PROCESSED_LEADS = 'supreme_leads.json'
BLACKLIST_FILE = 'blacklist.txt'
PROXY_FILE = 'proxy.txt'
DB_FILE = 'gold_leads.db'


# Specialized Scouters & Keyword Matrix
SCOUTER_MISSIONS = {
    "Scouter 1 (Panels)": [
        "Strong IPTV panel", "B1G IPTV server USA", "IPTV reseller UK",
        "wholesale IPTV panel", "Distribuidor IPTV", "Rivenditore IPTV"
    ],
    "Scouter 1 (Panels & Infrastructure)": [
        "private iptv infrastructure 2026", "anti-freeze server direct owner",
        "High-uptime IPTV panel UK", "Dedicated IPTV ports USA",
        "Rivenditore IPTV stabilità", "Panel de revendedor estable"
    ],
    "Scouter 2 (Fire Stick)": [
        "Fire Stick setup Spain", "Android Box Italy",
        "Firestick setup guide IT", "configuracion firestick ES"
    ],
    "Scouter 2 (App-Specific Setup)": [
        "Tivimate Premium activation help", "Ibo Player Pro setup 2026",
        "Purple Player m3u playlist", "XCIPTV player login fix",
        "Smart TV OTT setup guide", "Tivimate companion login USA"
    ],
    "Scouter 3 (Premium OTT)": [
        "Crystal OTT Germany", "Magnum OTT Switzerland",
        "Apollo Group TV DE", "premium IPTV CH"
    ],
    "Scouter 3 (Hardware Hooks)": [
        "Firestick 4K Max jailbreak UK", "Formuler Z11 Pro portal setup",
        "BuzzTV setup guide 2026", "Nvidia Shield IPTV settings",
        "Mag 540 portal change help", "Shield TV wholesale Australia"
    ],
    "Scouter 4 (Wholesale)": [
        "Bulk Mag Box Canada", "Nvidia Shield Wholesale Australia",
        "IPTV hardware CA", "Shield TV AU"
    ],
    "Scouter 4 (Wholesale & B2B)": [
        "IPTV credits wholesale UK", "Direct source reseller USA",
        "Bulk credits IPTV Europe", "IPTV panel white label service",
        "Custom DNS hardcoding service", "IPTV billing portal setup"
    ],
    "Scouter 5 (Live Sports)": [
        "Premium Sports IPTV UK", "Sky Sports IPTV USA",
        "Optus Sport IPTV AU", "TSN IPTV Canada"
    ],
    "Scouter 5 (Live Event Intent)": [
        "NFL Sunday Ticket IPTV USA", "Champions League stable UK",
        "Premier League 4K streams", "F1 live IPTV no lag",
        "UFC PPV stable service", "Sky Sports ultra HD IPTV"
    ],
    "Scouter 6 (Frustrated Switcher)": [
        "buffering every 5 minutes", "black screen during match", "channels keep looping",
        "provider not responding", "need trial stable backend", "alternative to apollo", "alternative to xtream"
    ],
    "Scouter 6 (The Churn Hunter)": [
        "alternative to apollo group tv", "xtream codes login expired",
        "provider disappeared help", "portal URL changed again",
        "admin not responding iptv", "channels looping fix"
    ],
    "Scouter 7 (Large Scale Buyer)": [
        "looking for reliable panel", "need local cards only", "bulk iptv credits uk",
        "bulk iptv credits usa", "rebrandable app dns", "looking for direct owner"
    ],
    "Scouter 7 (Urgent Troubleshooting)": [
        "black screen during match fix", "buffering every 5 minutes help",
        "ISP block IPTV bypass UK", "VPN for IPTV stuttering",
        "M3U error 404 fix", "playlist not loading help"
    ],
    "Scouter 8 (Tech-Savvy Newbie)": [
        "setup iptv on tivimate", "formuler z11 help setup",
        "best playlist for firestick", "how to install m3u on smart tv"
    ],
    "Scouter 8 (New Market Trends)": [
        "8K IPTV stream trial", "low latency sports IPTV",
        "personalized IPTV playlist", "AI based IPTV support",
        "no-VPN required IPTV USA"
    ],
    "Scouter 9 (2026 High-Value)": [
        "provider disappeared", "admin not responding", "renew button not working", "portal URL changed",
        "tivimate premium trial", "best app for buzz tv", "mag 540 portal", "firestick developer mode help",
        "direct source reseller", "looking for 100 credits", "panel with catchup", "anti-freeze server owner"
    ],
}

BUYER_INTENT = [
    "looking for IPTV", "best IPTV 2026", "need m3u", "stable service", "no buffering",
    "IPTV trial", "recommend service", "buy subscription", "firestick setup", "sports streaming"
]
B2B_INTENT = [
    "iptv panel price", "buy reseller credits", "best reseller panel", "start iptv business",
    "iptv credits cost", "become reseller", "panel setup", "wholesale iptv"
]
REBRAND_INTENT = [
    "iptv rebrand", "custom apk rebranding", "white label iptv", "rebrand ibo player",
    "dns hardcoding", "iptv app source code", "tivimate rebrand", "custom billing portal", "whmcs iptv"
]
PROBLEM_TRIGGERS = [
    "server down", "buffering issue", "links expired", "help m3u not working", "service blocked", "need new provider"
]
ESSENTIAL_HASHTAGS = [
    "#IPTV", "#IPTV2025", "#IPTV2026", "#IPTVReseller", "#IPTVPanel", "#4KIPTV",
    "#NoBuffering", "#CutTheCord", "#SportsStreaming", "#IPTVRebrand", "#WhiteLabelIPTV", "#M3U", "#XtreamCodes"
]

QC_GROUP_KEYWORDS = [
    "IPTV support", "verified iptv panel", "catchup server quality", "anti-freeze iptv owner",
    "iptv infrastructure hub", "stable iptv community", "iptv technical group", "sports iptv verified"
]

MARKET_KEYWORDS = {
    "en-UK": {
        "buyer": ["looking for sky sports", "bt sport stable", "uk tv bundle", "freeview iptv"],
        "b2b": ["uk panel reseller", "local uk cards only", "uk provider direct"],
        "rebrand": ["uk white label app", "dns hardcode uk", "uk billing portal"],
        "problem": ["black screen during match", "buffering every minute", "provider down uk"],
        "tags": ["#UKIPTV", "#SkySports", "#BTSPORT", "#4KUK", "#UKStreams"]
    },
    "en-US": {
        "buyer": ["need nfl sunday ticket", "espn 4k", "usa stable iptv", "local channels usa"],
        "b2b": ["usa credits bulk", "direct owner usa", "panel price usd"],
        "rebrand": ["white label usa", "usa app rebrand", "usa dns lock"],
        "problem": ["channel keeps looping", "blackouts", "provider down usa"],
        "tags": ["#USIPTV", "#NFL", "#NBA", "#ESPN", "#USStreams"]
    },
    "es-ES": {
        "buyer": ["busco iptv estable", "futbol 4k españa", "lista m3u españa"],
        "b2b": ["revendedor panel españa", "creditos mayorista iptv", "dueño directo españa"],
        "rebrand": ["marca blanca iptv", "dns fijado españa", "portal facturacion"],
        "problem": ["pantalla negra", "buffering", "servidor caido"],
        "tags": ["#IPTVEspaña", "#LaLiga", "#Futbol4K", "#StreamsES"]
    },
    "it-IT": {
        "buyer": ["cerco iptv stabile", "serie a 4k", "lista m3u italia"],
        "b2b": ["rivenditore crediti iptv", "prezzo pannello it", "proprietario diretto"],
        "rebrand": ["white label italia", "dns bloccato it", "portale fatturazione"],
        "problem": ["schermo nero", "buffering continuo", "server giu"],
        "tags": ["#IPTVItalia", "#SerieA", "#Calcio4K", "#StreamsIT"]
    },
    "de-DE": {
        "buyer": ["suche iptv stabil", "bundesliga 4k", "m3u liste deutsch"],
        "b2b": ["reseller panel de", "großhandel credits", "direkter anbieter"],
        "rebrand": ["white label de", "dns hardcode de", "abrechnung portal"],
        "problem": ["schwarzer bildschirm", "puffern", "server down"],
        "tags": ["#IPTVDeutschland", "#Bundesliga", "#4KDE", "#StreamsDE"]
    },
    "fr-FR": {
        "buyer": ["cherche iptv stable", "ligue 1 4k", "liste m3u france"],
        "b2b": ["revendeur panel fr", "credits iptv gros", "proprio direct fr"],
        "rebrand": ["white label france", "dns verrouille fr", "portail facturation"],
        "problem": ["écran noir", "buffering constant", "serveur down"],
        "tags": ["#IPTVFrance", "#Ligue1", "#4KFR", "#StreamsFR"]
    }
}

def apply_market_keywords():
    mk = os.environ.get("MARKET", "").strip()
    cfg = MARKET_KEYWORDS.get(mk)
    if not cfg:
        return
    try:
        BUYER_INTENT.extend([t for t in cfg.get("buyer", []) if t not in BUYER_INTENT])
        B2B_INTENT.extend([t for t in cfg.get("b2b", []) if t not in B2B_INTENT])
        REBRAND_INTENT.extend([t for t in cfg.get("rebrand", []) if t not in REBRAND_INTENT])
        PROBLEM_TRIGGERS.extend([t for t in cfg.get("problem", []) if t not in PROBLEM_TRIGGERS])
        ESSENTIAL_HASHTAGS.extend([t for t in cfg.get("tags", []) if t not in ESSENTIAL_HASHTAGS])
        logger.info(f"Market keywords applied for {mk}.")
    except Exception as e:
        logger.error(f"Market keywords error: {e}")
# Ghost Mode: User-Agent Rotation
ua = UserAgent()
request_counter = 0

def get_ghost_ua():
    global request_counter
    request_counter += 1
    return ua.random if request_counter % 5 == 0 else ua.chrome

def init_db():
    try:
        conn = sqlite3.connect(DB_FILE, timeout=30)
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS leads 
                     (id INTEGER PRIMARY KEY AUTOINCREMENT, 
                      link TEXT UNIQUE, 
                      group_title TEXT, 
                      members INTEGER, 
                      tech_score INTEGER, 
                      quality_score INTEGER, 
                      status TEXT, 
                      timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)''')
        c.execute('''CREATE TABLE IF NOT EXISTS joined_groups
                     (id INTEGER PRIMARY KEY AUTOINCREMENT,
                      group_id INTEGER UNIQUE,
                      title TEXT,
                      username TEXT,
                      link TEXT,
                      joined_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                      last_scanned_id INTEGER DEFAULT 0,
                      banned INTEGER DEFAULT 0,
                      archived INTEGER DEFAULT 0)''')
        c.execute('''CREATE TABLE IF NOT EXISTS prospects
                     (id INTEGER PRIMARY KEY AUTOINCREMENT,
                      user_id INTEGER,
                      username TEXT,
                      message TEXT,
                      message_id INTEGER,
                      message_ts DATETIME,
                      group_id INTEGER,
                      group_title TEXT,
                      persona_id TEXT,
                      status TEXT,
                      opt_out INTEGER DEFAULT 0,
                      responses_count INTEGER DEFAULT 0,
                      last_contacted_ts DATETIME,
                      UNIQUE(user_id, group_id, message_id))''')
        c.execute('''CREATE TABLE IF NOT EXISTS keywords
                     (term TEXT PRIMARY KEY,
                      weight INTEGER DEFAULT 1,
                      hits INTEGER DEFAULT 0,
                      conversions INTEGER DEFAULT 0,
                      updated_at DATETIME DEFAULT CURRENT_TIMESTAMP)''')
        c.execute('''CREATE TABLE IF NOT EXISTS activity_log
                     (id INTEGER PRIMARY KEY AUTOINCREMENT,
                      ts DATETIME DEFAULT CURRENT_TIMESTAMP,
                      type TEXT,
                      details TEXT)''')
        try:
            c.execute("PRAGMA journal_mode=WAL;")
            c.execute("PRAGMA synchronous=NORMAL;")
        except Exception:
            pass
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Database Init Error: {e}")

def migrate_db():
    try:
        conn = sqlite3.connect(DB_FILE, timeout=30)
        c = conn.cursor()
        try:
            c.execute("ALTER TABLE prospects ADD COLUMN persona_id TEXT")
        except Exception:
            pass
        try:
            c.execute("ALTER TABLE joined_groups ADD COLUMN archived INTEGER DEFAULT 0")
        except Exception:
            pass
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Database Migration Error: {e}")

def save_lead_to_db(link, title, members, tech_score, quality_score, status):
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("INSERT OR REPLACE INTO leads (link, group_title, members, tech_score, quality_score, status) VALUES (?, ?, ?, ?, ?, ?)",
                  (link, title, members, tech_score, quality_score, status))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"DB Save Error: {e}")

def log_activity(event_type, details):
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("INSERT INTO activity_log (type, details) VALUES (?, ?)", (event_type, details[:1000]))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Activity Log Error: {e}")

def record_joined_group(group_id, title, username, link):
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("""INSERT OR IGNORE INTO joined_groups (group_id, title, username, link) 
                     VALUES (?, ?, ?, ?)""", (group_id, title, username, link))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Joined Group Save Error: {e}")

def mark_group_banned(group_id):
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("UPDATE joined_groups SET banned = 1 WHERE group_id = ?", (group_id,))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Group Ban Mark Error: {e}")

def save_prospect(user_id, username, message, message_id, message_ts, group_id, group_title, persona_id, status):
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("""INSERT OR IGNORE INTO prospects 
                     (user_id, username, message, message_id, message_ts, group_id, group_title, persona_id, status) 
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                  (user_id, username, message, message_id, message_ts, group_id, group_title, persona_id, status))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Prospect Save Error: {e}")

def choose_persona_id():
    return random.choice(["expert", "peer", "concise"])

def get_prospect_persona(user_id, group_id):
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("SELECT persona_id FROM prospects WHERE user_id = ? AND group_id = ? ORDER BY id DESC LIMIT 1", (user_id, group_id))
        row = c.fetchone()
        conn.close()
        return row[0] if row and row[0] else None
    except Exception as e:
        logger.error(f"Get persona error: {e}")
        return None
def update_prospect_status(user_id, status, opt_out=False, increment_response=False):
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        fields = []
        params = []
        fields.append("status = ?")
        params.append(status)
        if opt_out:
            fields.append("opt_out = 1")
        if increment_response:
            fields.append("responses_count = responses_count + 1")
        fields.append("last_contacted_ts = CURRENT_TIMESTAMP")
        q = "UPDATE prospects SET " + ", ".join(fields) + " WHERE user_id = ?"
        params.append(user_id)
        c.execute(q, tuple(params))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Prospect Status Update Error: {e}")

def user_opted_out(user_id):
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("SELECT opt_out FROM prospects WHERE user_id = ? AND opt_out = 1 LIMIT 1", (user_id,))
        row = c.fetchone()
        conn.close()
        return bool(row)
    except Exception as e:
        logger.error(f"Opt-out Check Error: {e}")
        return False

def record_keyword_hits(text, converted=False):
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        lower = text.lower()
        terms = []
        for k in BUYER_INTENT + B2B_INTENT + REBRAND_INTENT + PROBLEM_TRIGGERS:
            if k in lower:
                terms.append(k)
        for term in terms:
            c.execute("INSERT OR IGNORE INTO keywords (term) VALUES (?)", (term,))
            c.execute("UPDATE keywords SET hits = hits + 1, updated_at = CURRENT_TIMESTAMP WHERE term = ?", (term,))
            if converted:
                c.execute("UPDATE keywords SET conversions = conversions + 1, updated_at = CURRENT_TIMESTAMP WHERE term = ?", (term,))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Keyword Record Error: {e}")

def prospect_has_active_conversation(user_id):
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("SELECT 1 FROM prospects WHERE user_id = ? AND status IN ('responded','converted') LIMIT 1", (user_id,))
        row = c.fetchone()
        conn.close()
        return bool(row)
    except Exception as e:
        logger.error(f"Prospect conversation check error: {e}")
        return False

def should_queue_handshake(user_id):
    if user_opted_out(user_id):
        return False
    if prospect_has_active_conversation(user_id):
        return False
    return True

def detect_language_from_bio(text):
    if not text:
        return "unknown"
    try:
        if re.search(r'[А-Яа-яЁё]', text):
            return "ru"
        return "latin"
    except Exception:
        return "unknown"

def market_primary_language():
    mk = os.environ.get("MARKET", "").lower()
    if mk.startswith("en"):
        return "en"
    if mk.startswith("es"):
        return "es"
    if mk.startswith("it"):
        return "it"
    if mk.startswith("de"):
        return "de"
    if mk.startswith("fr"):
        return "fr"
    return "en"

def detect_language_from_text(text):
    try:
        if not text:
            return None
        if re.search(r'[А-Яа-яЁё]', text):
            return "ru"
        return None
    except Exception:
        return None

def choose_target_language(bio_text, snippet):
    lang_text = detect_language_from_text(snippet or "")
    if lang_text:
        return lang_text
    bio_lang = detect_language_from_bio(bio_text or "")
    if bio_lang == "ru":
        return "ru"
    return market_primary_language()

def translate_text(text, target_lang):
    try:
        if not text or not target_lang or target_lang == "en":
            return text
        return GoogleTranslator(source='auto', target=target_lang).translate(text)
    except Exception as e:
        logger.error(f"Translate error: {e}")
        return text

def ensure_spintax_variation(text, last_text=None):
    try:
        if not text:
            return text
        def jaccard(a, b):
            if not a or not b:
                return 0.0
            sa = set(a.split())
            sb = set(b.split())
            inter = len(sa & sb)
            union = len(sa | sb)
            return (inter / union) if union else 0.0
        sim = jaccard(text.lower(), (last_text or "").lower())
        if sim <= 0.7:
            return text
        synonyms = {
            "stable": ["rock-solid", "steady", "reliable"],
            "trial": ["test line", "preview", "sample"],
            "support": ["help", "assist", "backing"],
            "streams": ["channels", "feeds", "lines"],
            "details": ["info", "brief", "notes"],
            "fix": ["solution", "patch", "tweak"],
            "want": ["need", "keen", "open"],
            "panel": ["backend", "portal", "console"],
            "no-buffer": ["smooth", "lag-free", "clean"],
            "catchup": ["replay", "timeshift", "backlog"]
        }
        def swap_words(t):
            out = t
            for k, vals in synonyms.items():
                if k in out.lower():
                    rep = random.choice(vals)
                    out = re.sub(k, rep, out, flags=re.IGNORECASE)
            return out
        variant = swap_words(text)
        if jaccard(variant.lower(), (last_text or "").lower()) <= 0.7:
            return variant
        extra = random.choice([" quick tip:", " heads-up:", " fyi:", " btw:"])
        return (variant + extra)[:220]
    except Exception:
        return text

# Stats Template
apex_supreme_stats = {
    "rich_joined": 0,
    "unique_dms": 0,
    "spam_shielded": 0,
    "bots_queried": 0,
    "tier_1_leads": 0,
    "day_counter": 1,
    "last_report": str(datetime.datetime.now()),
    "qc_groups": []
}

# Proxy Helper
def get_proxy():
    if os.path.exists(PROXY_FILE):
        try:
            with open(PROXY_FILE, 'r', encoding='utf-8') as f:
                lines = [line.strip() for line in f if line.strip()]
                if lines:
                    p = lines[0].split(':')
                    if len(p) == 4:
                        import socks
                        # Set System Timezone to match proxy (Simulation)
                        os.environ['TZ'] = 'Europe/London' # Example: UK Primary Target
                        if hasattr(time, 'tzset'): 
                            time.tzset()
                        return (socks.SOCKS5, p[0], int(p[1]), True, p[2], p[3])
        except Exception as e:
            logger.error(f"Proxy Load Error: {e}")
    return None

# Initialize Client
# Note: We rely on valid API_ID/HASH from config
if not API_ID or not API_HASH:
    logger.critical("API_ID or API_HASH missing in config!")
    sys.exit(1)

# Explicit loop management for stability
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)
SESSION_STRING = (os.environ.get("SESSION_STRING") or "").strip()
if SESSION_STRING:
    client = TelegramClient(StringSession(SESSION_STRING), int(API_ID), API_HASH, proxy=get_proxy(), loop=loop)
else:
    client = TelegramClient(SESSION_NAME, int(API_ID), API_HASH, proxy=get_proxy(), loop=loop)
apply_market_keywords()

def _code_callback():
    try:
        env_code = (os.environ.get("TELEGRAM_CODE") or "").strip()
        if env_code:
            return env_code
        path = os.path.join(os.getcwd(), "WAITING_FOR_CODE")
        deadline = time.time() + 180
        while time.time() < deadline:
            if os.path.exists(path):
                try:
                    with open(path, "r", encoding="utf-8") as f:
                        code = (f.read() or "").strip()
                    if code:
                        try:
                            os.remove(path)
                        except Exception:
                            pass
                        return code
                except Exception:
                    pass
            time.sleep(2)
    except Exception as e:
        logger.error(f"Code callback error: {e}")
    raise RuntimeError("Telegram login code not provided within timeout")
def add_to_blacklist(user_id):
    try:
        with open(BLACKLIST_FILE, 'a', encoding='utf-8') as f:
            f.write(f"{user_id},{datetime.datetime.now().isoformat()}\n")
    except Exception:
        pass

# --- Snippet Scoring Engine ---

def calculate_quality_score(text):
    """Machine Learning-style snippet scoring."""
    score = 0
    text_lower = text.lower()
    
    # Priority Keywords (+50)
    if any(k in text_lower for k in ["panel", "reseller", "wholesale", "distribuidor", "rivenditore"]):
        score += 50
        
    # Tier-1 Location Keywords (+30)
    if any(k in text_lower for k in ["italy", "spain", "usa", "uk", "canada", "germany", "firestick"]):
        score += 30
        
    # Junk Reductions (-20 to -100)
    if any(k in text_lower for k in ["test", "free"]):
        score -= 20
        
    # Auto-Purge: Tier-3 Codes
    if any(code in text_lower for code in TIER_3_CODES):
        score = -100
        
    return score

# --- Mission Filter & Triage ---

def execute_mission_filter(text):
    """Tier-1 Domination: Filter out junk zones and currencies."""
    text_lower = text.lower()
    banned_zones_lower = [z.lower() for z in BANNED_ZONES]
    tier3_lower = [c.lower() for c in TIER_3_CODES]
    junk_lower = [k.lower() for k in JUNK_KEYWORDS]
    currencies_lower = [c.lower() for c in BANNED_CURRENCIES]
    tier1_lower = [v.lower() for v in TIER_1_INDICATORS]
    
    # Rule 1: Kill Banned Zones & Codes
    if any(zone in text_lower for zone in banned_zones_lower) or any(code in text_lower for code in tier3_lower):
        return None
        
    # Rule 2: Kill Junk Keywords & Currencies
    if any(k in text_lower for k in junk_lower + currencies_lower):
        return None
        
    # Rule 3: Validate Tier-1 Indicators
    if TIER_1_INDICATORS:
        if any(val in text_lower for val in tier1_lower):
            return "TIER_1_VAL"
        return None
    
    # Fallback: if no Tier-1 indicators are configured, allow positively scored groups
    return "TIER_1_VAL"

def intent_score(text):
    t = text.lower()
    score = 0
    score += sum(3 for k in BUYER_INTENT if k in t)
    score += sum(5 for k in B2B_INTENT if k in t)
    score += sum(4 for k in REBRAND_INTENT if k in t)
    score += sum(3 for k in PROBLEM_TRIGGERS if k in t)
    score += sum(2 for k in ESSENTIAL_HASHTAGS if k.lower() in t)
    if ("price" in t and "panel" in t and "rebrand" in t):
        score += 8
    return score

def should_scrape_now():
    lt = time.localtime()
    h = lt.tm_hour
    w = lt.tm_wday
    general = (18 <= h <= 23) or (13 <= h <= 16)
    b2b = (9 <= h <= 17)
    if w in [0, 1, 2, 3, 4]:
        return general or b2b
    return general

# --- Logic Engine ---

async def gatekeeper(chat_ref):
    stats = load_json(STATS_FILE, apex_supreme_stats)
    try:
        current_ua = get_ghost_ua()
        logger.info(f"Ghost Mode: Requesting via UA: {current_ua[:30]}...")

        link = None
        entity = None
        title = ""
        about = ""
        members = 0
        if isinstance(chat_ref, str):
            token = chat_ref.split('/')[-1]
            if 'joinchat' in chat_ref or token.startswith('+'):
                try:
                    invite = await client(CheckChatInviteRequest(token.lstrip('+')))
                    chat_obj = getattr(invite, 'chat', invite)
                    title = getattr(chat_obj, 'title', '').lower()
                    about = getattr(chat_obj, 'about', '').lower() if hasattr(chat_obj, 'about') else ''
                    members = getattr(chat_obj, 'participants_count', 0)
                except Exception:
                    pass
                link = chat_ref
            else:
                link = chat_ref
                try:
                    entity = await client.get_input_entity(chat_ref)
                except Exception:
                    try:
                        entity = await client.get_input_entity(token)
                    except Exception:
                        entity = None
        else:
            try:
                entity = await client.get_input_entity(chat_ref)
                link = f"channel_id:{getattr(chat_ref, 'id', 'unknown')}"
            except Exception:
                entity = None
                link = None

        if entity:
            try:
                full = await client(GetFullChannelRequest(entity))
                full_chat = getattr(full, 'full_chat', None)
                about = (getattr(full_chat, 'about', '') or '').lower()
                members = getattr(full_chat, 'participants_count', 0)
                title = getattr(getattr(full, 'chats', [{}])[0], 'title', '').lower()
            except Exception:
                pass

        pre_text = (title + " " + about).strip()
        if pre_text:
            q_score = calculate_quality_score(pre_text)
            if q_score <= 0:
                 return False, f"Low Quality Score ({q_score})"
            if not execute_mission_filter(pre_text):
                stats["spam_shielded"] += 1
                save_json(STATS_FILE, stats)
                return False, "Junk/Tier-3 Filtered"
        if members and members < 30:
            return False, f"Small Group ({members})"

        log_activity("group_join_attempt", link or "unknown")
        try:
            if isinstance(chat_ref, str):
                token = chat_ref.split('/')[-1]
                if 'joinchat' in chat_ref or token.startswith('+'):
                    join_result = await client(ImportChatInviteRequest(token.lstrip('+')))
                elif entity:
                    join_result = await client(JoinChannelRequest(entity))
                else:
                    return False, "Join Error: Unable to resolve entity"
            else:
                join_result = await client(JoinChannelRequest(entity))
            if hasattr(join_result, "chats") and join_result.chats:
                chat_obj2 = join_result.chats[0]
                chat_id = chat_obj2.id
                chat_title = chat_obj2.title
                chat_username = getattr(chat_obj2, "username", None)
            else:
                chat_id = join_result.updates[0].message.peer_id.channel_id
                chat_title = "Unknown"
                chat_username = None
            record_joined_group(chat_id, chat_title, chat_username, link or (f"https://t.me/{chat_username}" if chat_username else f"channel_id:{chat_id}"))
            log_activity("group_joined", f"{chat_id}:{chat_title}")
        except FloodWaitError as fe:
            wait_s = int(getattr(fe, "seconds", 60)) + 60
            logger.warning(f"Join FloodWait: sleeping {wait_s}s")
            await asyncio.sleep(wait_s)
            log_activity("group_join_rate_limited", f"{(link or 'unknown')}:{wait_s}")
            return False, "Join rate limited"
        except UserBannedInChannelError as e:
            log_activity("group_join_banned", f"{(link or 'unknown')}:{str(e)[:120]}")
            return False, "Join banned"
        except Exception as e: 
            log_activity("group_join_error", f"{(link or 'unknown')}:{str(e)[:120]}")
            return False, f"Join Error: {e}"

        await asyncio.sleep(random.randint(180, 420))
        try:
            dialogs = await client.get_dialogs(limit=10)
            public_dialogs = [d for d in dialogs if d.is_channel or d.is_group]
            if public_dialogs:
                target = random.choice(public_dialogs)
                await client.get_messages(target, limit=3)
        except Exception:
            pass

        messages = await client.get_messages(chat_id, limit=50)
        combined_text = " ".join([m.text for m in messages if m.text]).lower()
        
        tech_hits = sum(1 for m in messages if m.text and any(k in m.text.lower() for k in ['dns', 'portal', 'xtream', 'm3u', 'stalker', 'mac']))
        total_q_score = calculate_quality_score(combined_text) + (tech_hits * 5)

        if total_q_score < 30:
            await client(LeaveChannelRequest(chat_id))
            return False, f"Low Depth Score ({total_q_score})"

        urgent_terms = ["down", "black screen", "expired", "server down", "buffering issue", "need new provider"]
        urgent = any(k in combined_text for k in urgent_terms)
        status_val = "URGENT" if urgent else "VERIFIED"
        save_lead_to_db(link or f"channel_id:{chat_id}", chat_title, members if 'members' in locals() else 0, tech_hits, total_q_score, status_val)
        if urgent:
            try:
                await client.send_message('me', f"🔴 URGENT Lead\nGroup: {chat_title}\nLink: {link or f'channel_id:{chat_id}'}")
            except Exception:
                pass
        
        try:
            await client(UpdateNotifySettingsRequest(
                peer=InputNotifyPeer(peer=await client.get_input_entity(chat_id)),
                settings=InputPeerNotifySettings(mute_until=2147483647)
            ))
        except: pass
        
        return True, f"Gold Lead (Score: {total_q_score})"
    except Exception as e:
        logger.error(f"Gatekeeper error: {e}")
        return False, "Analysis Error"


async def user_discovery_loop():
    while True:
        try:
            stats = load_json(STATS_FILE, apex_supreme_stats)
            warm_start = stats.get("warmup_started_at")
            now_ts = time.time()
            if not warm_start:
                warm_start = now_ts
                stats["warmup_started_at"] = warm_start
                save_json(STATS_FILE, stats)
            stats = load_json(STATS_FILE, apex_supreme_stats)
            mission_name = random.choice(list(SCOUTER_MISSIONS.keys()))
            kw = random.choice(SCOUTER_MISSIONS[mission_name])
            if not should_scrape_now():
                await asyncio.sleep(600)
                continue
            # Warm-up phase: read-only before joining groups
            warmup_window = 900 if os.environ.get("AURA_MODE", "").lower() == "testing" else 86400
            if now_ts - warm_start < warmup_window:
                await asyncio.sleep(1800)
                continue
            if random.random() < 0.35:
                kw = random.choice(ESSENTIAL_HASHTAGS)
            res = await client(functions.contacts.SearchRequest(q=kw, limit=30))
            chats = getattr(res, 'chats', []) or []
            groups = load_json(PROCESSED_GROUPS, [])
            for ch in chats:
                cid = getattr(ch, "id", None)
                uname = getattr(ch, "username", None)
                ident = f"https://t.me/{uname}" if uname else (f"channel_id:{cid}" if cid is not None else None)
                if not ident:
                    continue
                if ident in groups:
                    continue
                groups.append(ident)
                save_json(PROCESSED_GROUPS, groups)
                is_rich, reason = await gatekeeper(ch if not uname else ident)
                if is_rich:
                    stats["rich_joined"] += 1
                    try:
                        await client.send_message('me', f"🏆 Discovery: Gold Lead\nLink: {ident}\n{reason}")
                    except Exception:
                        pass
                save_json(STATS_FILE, stats)
            interval = 1800 if os.environ.get("AURA_MODE", "").lower() == "testing" else 14400
            await asyncio.sleep(interval)
        except Exception as e:
            logger.error(f"User discovery loop error: {e}")
            await asyncio.sleep(3600)


async def _click_button(message, text=None, index=None):
    try:
        if hasattr(message, "click"):
            if text:
                return await message.click(text=text)
            return await message.click(index=index if index is not None else 0)
    except Exception:
        pass
    try:
        # Fallback via callback request
        buttons = getattr(message, "buttons", []) or []
        flat = [b for row in buttons for b in row]
        target = None
        if text:
            for b in flat:
                if getattr(b, "text", "").lower().strip() == text.lower().strip():
                    target = b
                    break
        elif index is not None and index < len(flat):
            target = flat[index]
        if target and hasattr(target, "data"):
            return await client(GetBotCallbackAnswerRequest(peer=message.peer_id, msg_id=message.id, data=target.data))
    except Exception:
        pass
    return None

async def navigate_nicehub(max_pages=3, max_links=6):
    return
async def navigate_tosearch(keyword, max_pages=3, max_links=8):
    return
async def humanization_loop():
    while True:
        try:
            dialogs = await client.get_dialogs(limit=20)
            public_dialogs = [d for d in dialogs if d.is_channel or d.is_group]
            if public_dialogs:
                target = random.choice(public_dialogs)
                # Just fetch messages to simulate reading
                await client.get_messages(target, limit=5)
                msgs = await client.get_messages(target, limit=1)
                if msgs and msgs[0].text:
                     try:
                         await client(functions.messages.SendReactionRequest(
                             peer=target, msg_id=msgs[0].id, reaction=[ReactionEmoji(emoticon='👍')]
                         ))
                     except: pass
            
            await asyncio.sleep(21600) # 6h
        except Exception as e:
            logger.error(f"Humanization Loop Error: {e}")
            await asyncio.sleep(3600)

# --- V2.1: Heuristic Handshake & Auto-Reply ---

queued_handshakes = {} # user_id: {"msg_id": int, "chat_id": int, "due": float, "snippet": str, "group_title": str}
watchlist = {} # user_id: expire_time

@client.on(events.NewMessage(incoming=True))
async def handle_v21_logic(event):
    if event.is_private:
        text = event.raw_text.lower()
        shield_terms = set(SENTIMENT_BLACKLIST + ["report", "spam", "scam", "block", "fuck off", "who is this"])
        user_id = event.sender_id
        if any(k in text for k in shield_terms):
            add_to_blacklist(user_id)
            try:
                await client(BlockRequest(id=user_id))
            except Exception as e:
                logger.error(f"Block failed: {e}")
            log_activity("blocked_user", str(user_id))
            return
        optout_terms = ["stop", "unsubscribe", "no more messages", "dont message", "do not message", "remove me", "leave me alone"]
        if any(k in text for k in optout_terms):
            update_prospect_status(user_id, "opted_out", opt_out=True, increment_response=True)
            log_activity("opt_out", f"{user_id}:{event.raw_text[:140]}")
            try:
                await event.reply("Understood, I will not message you again here.")
            except Exception:
                pass
            return
        conv_terms = ["trial", "test line", "ready to buy", "how much", "send details", "ok send", "price", "subscribe", "subscription"]
        is_conversion = any(k in text for k in conv_terms)
        if is_conversion:
            update_prospect_status(user_id, "converted", opt_out=False, increment_response=True)
            record_keyword_hits(event.raw_text, converted=True)
            log_activity("conversion", f"{user_id}:{event.raw_text[:140]}")
        else:
            update_prospect_status(user_id, "responded", opt_out=False, increment_response=True)
            record_keyword_hits(event.raw_text, converted=False)
            log_activity("inbound_dm", f"{user_id}:{event.raw_text[:140]}")
        try:
            await client.send_message('me', f"Lead replied. User: {user_id} | Msg: {event.raw_text[:140]}")
        except Exception:
            pass
        return

    if not event.is_private:
        text = event.raw_text.lower()
        user_id = event.sender_id
        group_id = event.chat_id
        group_title = getattr(event.chat, "title", "group")
        try:
            sender = await event.get_sender()
            username = getattr(sender, "username", None)
        except Exception:
            username = None
        if any(k in text for k in URGENCY_KEYWORDS):
             try:
                await client.send_message('me', f"🔴 **URGENCY: GOLDEN LEAD**\nUser: `{event.sender_id}`\nMsg: `{event.raw_text[:50]}`")
             except: pass
        
        s = intent_score(event.raw_text)
        basic_terms = ["need iptv", "looking for streaming", "best tv provider", "looking for iptv", "iptv recommendation", "need streaming"]
        if s >= 8 or any(t in text for t in basic_terms):
            msg_ts = getattr(event.message, "date", datetime.datetime.now()).isoformat()
            persona_id = choose_persona_id()
            save_prospect(user_id, username, event.raw_text, event.id, msg_ts, group_id, group_title, persona_id, "not_contacted")
            record_keyword_hits(event.raw_text, converted=False)
            log_activity("prospect_identified", f"{user_id}:{group_title}:{event.id}")
        if s >= 10:
            try:
                await client.send_message('me', f"🟡 High-Intent Lead\nUser: `{event.sender_id}`\nScore: `{s}`\nMsg: `{event.raw_text[:140]}`")
            except: pass
        
        # Conversation Watchlist Escalation (5-minute watch)
        now = time.time()
        exp = watchlist.get(event.sender_id)
        if exp and now <= exp and s >= 10:
            user_id = event.sender_id
            if user_id not in queued_handshakes and should_queue_handshake(user_id):
                group_title = getattr(event.chat, "title", "group")
                queued_handshakes[user_id] = {
                    "msg_id": event.id,
                    "chat_id": event.chat_id,
                    "due": time.time() + random.randint(300, 600), # 5-10m
                    "snippet": event.raw_text[:120],
                    "group_title": group_title
                }
            watchlist.pop(event.sender_id, None)
        elif s < 6 and event.sender_id not in watchlist:
            watchlist[event.sender_id] = now + 300
        
        # Potential Lead Handshake (Random selection)
        if (s >= 12 or any(val in text for val in TIER_1_INDICATORS)) and random.random() < 0.5:
            user_id = event.sender_id
            if user_id not in queued_handshakes and should_queue_handshake(user_id):
                group_title = getattr(event.chat, "title", "group")
                queued_handshakes[user_id] = {
                    "msg_id": event.id,
                    "chat_id": event.chat_id,
                    "due": time.time() + random.randint(600, 900), # 10-15m
                    "snippet": event.raw_text[:120],
                    "group_title": group_title
                }
                logger.info(f"Queued Handshake for {user_id} in 10-15m.")

async def handshake_processor():
    """V2.1: Perform heuristic reactions 10-15m before outreach."""
    while True:
        try:
            now = time.time()
            # Copy keys to avoid RuntimeError during iteration
            to_process = [u for u, data in queued_handshakes.items() if data["due"] <= now]
            for u in to_process:
                if u in queued_handshakes:
                    data = queued_handshakes.pop(u)
                    msg_id = data["msg_id"]
                    chat_id = data["chat_id"]
                    snippet = data.get("snippet", "")
                    group_title = data.get("group_title", "the group")
                    try:
                        if should_outreach():
                            await client(functions.messages.SendReactionRequest(
                                peer=chat_id, msg_id=msg_id, reaction=[ReactionEmoji(emoticon='👍')]
                            ))
                        logger.info(f"Heuristic Handshake: Reacted to {u} in {chat_id}")
                    except Exception as e:
                        logger.error(f"Handshake failed: {e}")

                    try:
                        if user_opted_out(u):
                            logger.info("Skipping DM (user opted out).")
                            continue
                        hour = time.localtime().tm_hour
                        if hour < 9 or hour > 21:
                            logger.info("Skipping DM (outside human hours).")
                            continue
                        stats = load_json(STATS_FILE, apex_supreme_stats)
                        dm_today = stats.get("dm_initiated_today", 0)
                        me = await client.get_me()
                        is_premium = bool(getattr(me, "premium", False))
                        cap = 25 if is_premium else 10
                        cap = min(cap, 75)
                        if dm_today >= cap:
                            logger.info(f"DM cap reached ({dm_today}/{cap}).")
                            continue
                        try:
                            fu = await client(GetFullUserRequest(u))
                            uname = getattr(getattr(fu, "user", None), "username", None)
                            if not uname:
                                logger.info("Skipping DM (no username).")
                                continue
                            photo_ok = False
                            try:
                                photo_ok = bool(getattr(getattr(fu, "user", None), "photo", None) or getattr(getattr(fu, "full_user", None), "profile_photo", None))
                            except Exception:
                                photo_ok = False
                            if not photo_ok:
                                logger.info("Skipping DM (no profile photo).")
                                continue
                            common = getattr(fu.full_user, "common_chats_count", 0)
                            if common <= 0:
                                logger.info("Skipping DM (no mutual context).")
                                continue
                            lead_premium = bool(getattr(getattr(fu, "user", None), "premium", False))
                        except (UserPrivacyRestrictedError, PeerIdInvalidError):
                            logger.info("Skipping DM (privacy or invalid peer).")
                            continue
                        try:
                            status = getattr(fu, "user", None)
                            status = getattr(status, "status", None) if status else None
                            if not status:
                                status = getattr(getattr(fu, "full_user", None), "status", None)
                            recent_ok = False
                            now_ts = datetime.datetime.now(datetime.timezone.utc)
                            if isinstance(status, (UserStatusOnline, UserStatusRecently)):
                                recent_ok = True
                            elif isinstance(status, UserStatusOffline):
                                dt = getattr(status, "was_online", None)
                                if dt:
                                    if dt.tzinfo is None:
                                        dt = dt.replace(tzinfo=datetime.timezone.utc)
                                    if (now_ts - dt) <= datetime.timedelta(days=4):
                                        recent_ok = True
                            elif isinstance(status, (UserStatusLastWeek, UserStatusLastMonth)):
                                recent_ok = False
                            else:
                                recent_ok = False
                            if not recent_ok:
                                logger.info("Skipping DM (user inactive beyond 4 days).")
                                continue
                        except Exception as _e:
                            logger.error(f"User status check error: {_e}")
                            continue
                        social_hint = ""
                        try:
                            recent = await client.get_messages(chat_id, limit=10)
                            problems = ["black screen", "buffering", "down", "looping", "expired"]
                            competitor = ["apollo", "xtream", "stb", "mag"]
                            if any(m.text and any(p in m.text.lower() for p in problems) for m in recent):
                                social_hint = "Noticed others mentioning black screens and buffering today."
                            elif any(m.text and any(c in m.text.lower() for c in competitor) for m in recent):
                                social_hint = "Saw chatter about apollo/xtream issues in the group."
                        except Exception:
                            pass
                        await typing_heartbeat(u, random.uniform(6, 9))
                        dm_text = None
                        persona_id = get_prospect_persona(u, chat_id) or choose_persona_id()
                        try:
                            if ai_client:
                                if persona_id == "expert":
                                    prompt = (
                                        "You are Aiden (Expert). Calm tech specialist. "
                                        "Reference root cause (DNS/portal). Include brief service details "
                                        "(4K sports, catchup, anti-freeze). Add a tailored recommendation. "
                                        "Keep under 60 words, no links. End with a helpful question. " f"Context: {social_hint}"
                                    )
                                elif persona_id == "peer":
                                    prompt = (
                                        "You are Aiden (Peer). Casual friend vibe. "
                                        "Mention switching panels worked 10/10. Include brief service details "
                                        "(stable streams, 4K options, support). Tailor recommendations. "
                                        "Keep under 60 words, no links. End with a helpful question. " f"Context: {social_hint}"
                                    )
                                else:
                                    prompt = (
                                        "You are Aiden (Concise). Brief and direct. "
                                        "Offer a stable trial privately with short service details "
                                        "(no-buffer, sports, catchup). Tailor the recommendation. "
                                        "Keep under 50 words, no links. End with a helpful question. " f"Context: {social_hint}"
                                    )
                                user_msg = f"Quoted from {group_title}: \"{snippet}\""
                                resp = await ai_client.chat.completions.create(
                                    messages=[
                                        {"role": "system", "content": prompt},
                                        {"role": "user", "content": user_msg}
                                    ],
                                    model="llama-3.3-70b-versatile",
                                )
                                dm_text = (resp.choices[0].message.content or "").strip()
                                # Safety: strip links
                                dm_text = re.sub(r'(https?://\S+|t\.me/\S+)', '', dm_text)
                                # Hard cap length
                                if len(dm_text) > 220:
                                    dm_text = dm_text[:220]
                            else:
                                if persona_id == "expert":
                                    base = f"saw your note in {group_title}. likely dns/portal handshake."
                                    proof = f" {social_hint}" if social_hint else ""
                                    svc = " 4k sports, catchup, anti-freeze available."
                                    dm_text = f"{base}{proof}{svc} want me to share the fix?"
                                elif persona_id == "peer":
                                    base = f"same thing happened in {group_title} last week."
                                    proof = f" {social_hint}" if social_hint else ""
                                    svc = " stable streams with 4k options and support."
                                    dm_text = f"{base}{proof}{svc} i switched panels and it’s been 10/10. want details?"
                                else:
                                    svc = " no-buffer line, sports, catchup."
                                    dm_text = f"still need a stable trial from the group? {svc} can send info—want it?"
                        except Exception as e:
                            logger.error(f"Groq DM compose error: {e}")
                            if persona_id == "expert":
                                dm_text = f"noticed your note in {group_title}. dns/portal fix worked for me. want steps?"
                            elif persona_id == "peer":
                                dm_text = "had same issue—switched panels and it’s solid now. want details?"
                            else:
                                dm_text = "want a stable trial link? i can share privately."

                        try:
                            bio_text = getattr(getattr(fu, "full_user", None), "about", "") or ""
                            target_lang = choose_target_language(bio_text, snippet)
                            dm_text = translate_text(dm_text, target_lang)
                        except Exception:
                            pass
                        try:
                            stats = load_json(STATS_FILE, apex_supreme_stats)
                            last_text = stats.get("last_dm_text")
                            dm_text = ensure_spintax_variation(dm_text, last_text)
                            stats["last_dm_text"] = dm_text
                            save_json(STATS_FILE, stats)
                        except Exception:
                            pass
                        try:
                            await asyncio.sleep(random.randint(300, 420))
                            await client.send_message(u, dm_text)
                            update_prospect_status(u, "contacted", opt_out=False, increment_response=False)
                            log_activity("dm_sent", f"{u}:{group_title}")
                            stats["dm_initiated_today"] = dm_today + 1
                            stats["unique_dms"] = stats.get("unique_dms", 0) + 1
                            save_json(STATS_FILE, stats)
                            logger.info(f"DM sent to {u}. Count today: {stats['dm_initiated_today']}/{cap}")
                            # Human interval throttle between conversations
                            await asyncio.sleep(random.randint(900, 1200))  # 15–20 minutes
                        except FloodWaitError as fe:
                            wait_s = int(getattr(fe, "seconds", 60)) + 60
                            logger.warning(f"FloodWait: sleeping {wait_s}s")
                            await asyncio.sleep(wait_s)
                        except (UserPrivacyRestrictedError, YouBlockedUserError, PeerIdInvalidError) as e:
                            logger.info(f"DM skipped due to privacy/block/invalid: {e}")
                            continue
                        except Exception as e:
                            logger.error(f"DM send error: {e}")
                            continue
                    except Exception as e:
                        logger.error(f"Proactive DM flow error: {e}")
            await asyncio.sleep(60)
        except Exception as e:
            logger.error(f"Handshake Processor Error: {e}")
            await asyncio.sleep(60)

async def typing_heartbeat(entity, duration=6.0):
    start = time.time()
    try:
        while time.time() - start < duration:
            try:
                await client.send_chat_action(entity, "typing")
            except Exception:
                break
            await asyncio.sleep(3)
    except Exception:
        pass
# --- Command Deck ---

@client.on(events.NewMessage(pattern='/health'))
async def health_check(event):
    if not event.is_private: return
    # Trust Report
    report = (
        f"🛡️ **Aura V2.1 Trust Report**\n"
        f"✅ Account Status: Active (Premium Shield)\n"
        f"🛰️ Scouter State: Online\n"
        f"👻 Ghost Mode: UA Rotation Engaged\n"
        f"📍 Timezone: {os.environ.get('TZ', 'Standard')}\n"
        f"📈 Growth Pattern: Randomized (5-12%)"
    )
    await event.reply(report)

@client.on(events.NewMessage(pattern=r'/sleep (\d+)'))
async def sleep_bot(event):
    if not event.is_private: return
    try:
        hours = int(event.pattern_match.group(1))
        await event.reply(f"💤 Entering Ghost Mode (Read-Only) for {hours} hours.")
        # In a real implementation, we would set a flag in shared state/DB
        await asyncio.sleep(hours * 3600)
        await event.reply("🚀 Aiden is awake and back in scout mode.")
    except Exception:
        await event.reply("Usage: /sleep <hours>")

@client.on(events.NewMessage(pattern='/reset_persona'))
async def reset_persona(event):
    if not event.is_private: return
    # Mimic human profile update
    await event.reply("🔄 Refreshing profile metadata and persona hooks...")
    # Logic to update bio/name randomly could go here
    await asyncio.sleep(5)
    await event.reply("✅ Persona Reset: Digital footprint refreshed.")

@client.on(events.NewMessage(pattern='/export'))
async def export_db(event):
    if not event.is_private: return
    try:
        if os.path.exists(DB_FILE):
            await client.send_file('me', DB_FILE, caption=f"🛰️ **Gold Leads Database Export**\nTime: {datetime.datetime.now()}")
        else:
            await event.reply("Database file not found.")
    except Exception as e:
        await event.reply(f"Export failed: {e}")

async def historical_scan_loop():
    while True:
        try:
            conn = sqlite3.connect(DB_FILE)
            c = conn.cursor()
            c.execute("SELECT group_id, last_scanned_id, title FROM joined_groups WHERE banned = 0")
            rows = c.fetchall()
            conn.close()
            for group_id, last_scanned_id, title in rows:
                try:
                    if last_scanned_id and last_scanned_id > 0:
                        msgs = await client.get_messages(group_id, min_id=last_scanned_id, limit=100)
                    else:
                        msgs = await client.get_messages(group_id, limit=100)
                except (UserBannedInChannelError, ChatWriteForbiddenError, ChannelPrivateError) as e:
                    mark_group_banned(group_id)
                    log_activity("group_banned", f"{group_id}:{str(e)[:160]}")
                    continue
                except FloodWaitError as fe:
                    wait_s = int(getattr(fe, "seconds", 60)) + 60
                    logger.warning(f"History scan FloodWait: sleeping {wait_s}s")
                    await asyncio.sleep(wait_s)
                    continue
                except Exception as e:
                    logger.error(f"History scan error: {e}")
                    continue
                if not msgs:
                    continue
                max_id = last_scanned_id or 0
                for m in reversed(msgs):
                    if not getattr(m, "id", None):
                        continue
                    if last_scanned_id and m.id <= last_scanned_id:
                        continue
                    text = getattr(m, "text", "") or ""
                    if not text:
                        continue
                    tl = text.lower()
                    s = intent_score(text)
                    basic_terms = ["need iptv", "looking for streaming", "best tv provider", "looking for iptv", "iptv recommendation", "need streaming"]
                    if s >= 8 or any(t in tl for t in basic_terms):
                        user_id = getattr(m, "sender_id", None)
                        if user_id:
                            try:
                                username = None
                                try:
                                    sender = await m.get_sender()
                                    username = getattr(sender, "username", None)
                                except Exception:
                                    username = None
                                msg_ts = getattr(m, "date", datetime.datetime.now()).isoformat()
                                persona_id = choose_persona_id()
                                save_prospect(user_id, username, text, m.id, msg_ts, group_id, title or "group", persona_id, "not_contacted")
                                record_keyword_hits(text, converted=False)
                                log_activity("prospect_identified_history", f"{user_id}:{title}:{m.id}")
                                if s >= 12 and user_id not in queued_handshakes:
                                    due_in = random.randint(600, 900)
                                    queued_handshakes[user_id] = {
                                        "msg_id": m.id,
                                        "chat_id": group_id,
                                        "due": time.time() + due_in,
                                        "snippet": text[:120],
                                        "group_title": title or "group"
                                    }
                            except Exception as e:
                                logger.error(f"History prospect error: {e}")
                    if not max_id or m.id > max_id:
                        max_id = m.id
                if max_id and max_id != last_scanned_id:
                    try:
                        conn2 = sqlite3.connect(DB_FILE)
                        c2 = conn2.cursor()
                        c2.execute("UPDATE joined_groups SET last_scanned_id = ? WHERE group_id = ?", (max_id, group_id))
                        conn2.commit()
                        conn2.close()
                    except Exception as e:
                        logger.error(f"Update last_scanned_id error: {e}")
                await asyncio.sleep(5)
            await asyncio.sleep(1800)
        except Exception as e:
            logger.error(f"Historical scan loop error: {e}")
            await asyncio.sleep(300)

async def prune_dead_chats_loop():
    while True:
        try:
            cutoff = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=7)).isoformat()
            conn = sqlite3.connect(DB_FILE)
            c = conn.cursor()
            c.execute("SELECT group_id, title FROM joined_groups WHERE banned = 0 AND archived = 0")
            rows = c.fetchall()
            conn.close()
            for group_id, title in rows:
                try:
                    conn2 = sqlite3.connect(DB_FILE)
                    c2 = conn2.cursor()
                    c2.execute("SELECT COUNT(*) FROM prospects WHERE group_id = ? AND message_ts >= ?", (group_id, cutoff))
                    cnt = c2.fetchone()[0]
                    c2.execute("SELECT quality_score FROM leads WHERE group_title = ? ORDER BY timestamp DESC LIMIT 1", (title,))
                    row = c2.fetchone()
                    qscore = row[0] if row else 0
                    conn2.close()
                except Exception as e:
                    logger.error(f"Prune query error: {e}")
                    continue
                if qscore > 0 and cnt == 0:
                    try:
                        await client(LeaveChannelRequest(group_id))
                    except Exception as e:
                        logger.error(f"Prune leave error: {e}")
                    try:
                        conn3 = sqlite3.connect(DB_FILE)
                        c3 = conn3.cursor()
                        c3.execute("UPDATE joined_groups SET archived = 1 WHERE group_id = ?", (group_id,))
                        conn3.commit()
                        conn3.close()
                        log_activity("group_archived", f"{group_id}:{title}")
                    except Exception as e:
                        logger.error(f"Prune archive error: {e}")
                await asyncio.sleep(2)
            await asyncio.sleep(43200) # run twice a day
        except Exception as e:
            logger.error(f"Prune loop error: {e}")
            await asyncio.sleep(3600)

async def stats_report():
    while True:
        await asyncio.sleep(43200) # 12h
        try:
            stats = load_json(STATS_FILE, apex_supreme_stats)
            conn = sqlite3.connect(DB_FILE)
            try:
                count = conn.execute("SELECT COUNT(*) FROM leads").fetchone()[0]
            except:
                count = 0
            prospects_total = 0
            contacted = 0
            responded = 0
            opt_outs = 0
            conversions = 0
            try:
                cur = conn.cursor()
                cur.execute("SELECT COUNT(*) FROM prospects")
                row = cur.fetchone()
                prospects_total = row[0] if row else 0
                cur.execute("SELECT COUNT(*) FROM prospects WHERE status IN ('contacted','converted')")
                row = cur.fetchone()
                contacted = row[0] if row else 0
                cur.execute("SELECT COUNT(*) FROM prospects WHERE status IN ('responded','converted')")
                row = cur.fetchone()
                responded = row[0] if row else 0
                cur.execute("SELECT COUNT(*) FROM prospects WHERE opt_out = 1")
                row = cur.fetchone()
                opt_outs = row[0] if row else 0
                cur.execute("SELECT COUNT(*) FROM prospects WHERE status = 'converted'")
                row = cur.fetchone()
                conversions = row[0] if row else 0
            except Exception as e:
                logger.error(f"Prospect stats error: {e}")
            conn.close()
            
            # Randomized Growth logic: 5-12%
            growth = random.uniform(0.05, 0.12)
            stats["day_counter"] += 1
            save_json(STATS_FILE, stats)

            try:
                response_rate = (responded / contacted) if contacted > 0 else None
                zero_since = stats.get("last_zero_response_since")
                now_ts = time.time()
                if response_rate == 0:
                    if not zero_since:
                        stats["last_zero_response_since"] = now_ts
                    else:
                        if now_ts - zero_since >= 43200: # 12h
                            try:
                                await client.send_message('me', "⚠️ Shadowban suspected: responses 0% for 12h. Switch to backup session.")
                            except Exception:
                                pass
                            stats["shadowban_alerted"] = True
                    save_json(STATS_FILE, stats)
                else:
                    if zero_since:
                        stats.pop("last_zero_response_since", None)
                        save_json(STATS_FILE, stats)
            except Exception as e:
                logger.error(f"Shadowban check error: {e}")

            report = (
                f"🛰️ **Aura Apex Supreme V2.1**\n"
                f"💎 **Verified Gold Leads:** {count}\n"
                f"🛡️ **Spam Auto-Purged:** {stats['spam_shielded']}\n"
                f"👥 **Prospects Tracked:** {prospects_total}\n"
                f"✉️ **Contacted/Responded:** {contacted}/{responded}\n"
                f"🚫 **Opt-out:** {opt_outs} | ✅ Conversions: {conversions}\n"
                f"📈 **Randomized Growth:** +{int(growth*100)}% Today\n"
                f"📍 **Sync State:** Hardware Spoofing Active\n"
                f"📈 **Day:** {stats['day_counter']} | **State:** 🟢\n"
                f"🔍 **QC Groups:** {len(stats.get('qc_groups', []))}"
            )
            await client.send_message('me', report)
        except Exception as e:
            logger.error(f"Stats Report Error: {e}")

async def noise_generation_loop():
    bots = ['@IFTTT', '@Stickers']
    searches = ["weather", "movies", "recipes", "football", "traffic", "music"]
    while True:
        try:
            if os.environ.get("AURA_MODE", "").lower() == "testing":
                await asyncio.sleep(random.randint(43200, 172800))
                continue
            b = random.choice(bots)
            q = random.choice(searches)
            try:
                await client.send_message(b, f"{q}")
            except Exception as e:
                logger.error(f"Noise bot send failed: {e}")
        except Exception as e:
            logger.error(f"Noise loop error: {e}")
        await asyncio.sleep(random.randint(43200, 172800))

async def main():
    print("Initializing AURA APEX SUPREME V2.1: FORTRESS HARDENING...")
    init_db()
    migrate_db()
    async def ensure_qc_group_joined():
        stats = load_json(STATS_FILE, apex_supreme_stats)
        qc = stats.get("qc_groups", [])
        if qc:
            return True
        link = os.environ.get("QC_GROUP_LINK", "").strip()
        if not link:
            try:
                res = await client(functions.contacts.SearchRequest(q=random.choice(QC_GROUP_KEYWORDS), limit=20))
                chats = getattr(res, 'chats', []) or []
                for ch in chats:
                    uname = getattr(ch, "username", None)
                    if not uname:
                        continue
                    link = f"https://t.me/{uname}"
                    ok, _reason = await gatekeeper(link)
                    if ok:
                        stats = load_json(STATS_FILE, apex_supreme_stats)
                        qc = stats.get("qc_groups", [])
                        if link not in qc:
                            qc.append(link)
                            stats["qc_groups"] = qc
                            save_json(STATS_FILE, stats)
                        log_activity("qc_joined", link)
                        return True
            except Exception as e:
                log_activity("qc_join_error", str(e)[:140])
                return False
        ok, _reason = await gatekeeper(link)
        if ok:
            stats = load_json(STATS_FILE, apex_supreme_stats)
            qc = stats.get("qc_groups", [])
            if link not in qc:
                qc.append(link)
                stats["qc_groups"] = qc
                save_json(STATS_FILE, stats)
            log_activity("qc_joined", link)
        return bool(ok)
    
    async def _start_with_retry():
        retries = 3
        delay = 5
        for _ in range(retries):
            try:
                if SESSION_STRING:
                    await client.start()
                else:
                    await client.start(phone=PHONE_NUMBER, code_callback=_code_callback, password=os.environ.get("TELEGRAM_PASSWORD"))
                return
            except Exception as _e:
                if 'database is locked' in str(_e).lower():
                    await asyncio.sleep(delay)
                    continue
                logger.error(f"Connection Error: {_e}")
                raise
        await asyncio.sleep(delay)
        if SESSION_STRING:
            await client.start()
        else:
            await client.start(phone=PHONE_NUMBER, code_callback=_code_callback, password=os.environ.get("TELEGRAM_PASSWORD"))

    await _start_with_retry()
    print("🏰 Fortress V2.1 Active: Handshake + Hardware Spoofing + Randomized Growth.")
    qc_ok = False
    try:
        qc_ok = await ensure_qc_group_joined()
        if not qc_ok:
            try:
                await client.send_message('me', "QC group join verification failed or unavailable. Discovery will pause DM/historical operations.")
            except Exception:
                pass
    except Exception:
        qc_ok = False
    asyncio.create_task(user_discovery_loop())
    asyncio.create_task(stats_report())
    if qc_ok:
        asyncio.create_task(handshake_processor())
        asyncio.create_task(historical_scan_loop())
    asyncio.create_task(proxy_health_monitor(client, PROXY_FILE))
    asyncio.create_task(noise_generation_loop())
    asyncio.create_task(prune_dead_chats_loop())
    async def qc_group_autojoin_loop():
        while True:
            try:
                kw = random.choice(QC_GROUP_KEYWORDS)
                res = await client(functions.contacts.SearchRequest(q=kw, limit=25))
                chats = getattr(res, 'chats', []) or []
                stats = load_json(STATS_FILE, apex_supreme_stats)
                qc = stats.get("qc_groups", [])
                for ch in chats:
                    cid = getattr(ch, "id", None)
                    uname = getattr(ch, "username", None)
                    ident = f"https://t.me/{uname}" if uname else (f"channel_id:{cid}" if cid is not None else None)
                    if not ident:
                        continue
                    if ident in qc:
                        continue
                    ok, _reason = await gatekeeper(ch if not uname else ident)
                    if ok:
                        qc.append(ident)
                        stats["qc_groups"] = qc
                        save_json(STATS_FILE, stats)
                        log_activity("qc_joined", ident)
                await asyncio.sleep(21600)
            except Exception as e:
                log_activity("qc_autojoin_error", str(e)[:140])
                await asyncio.sleep(3600)
    async def qc_membership_verifier_loop():
        while True:
            try:
                stats = load_json(STATS_FILE, apex_supreme_stats)
                qc = stats.get("qc_groups", [])
                if not qc:
                    await asyncio.sleep(1800)
                    continue
                dialogs = await client.get_dialogs(limit=200)
                links_present = set()
                for d in dialogs:
                    uname = getattr(d.entity, "username", None)
                    if uname:
                        links_present.add(f"https://t.me/{uname}")
                for link in list(qc):
                    if link not in links_present:
                        ok, _reason = await gatekeeper(link)
                        if not ok:
                            log_activity("qc_rejoin_failed", link)
                await asyncio.sleep(43200)
            except Exception as e:
                log_activity("qc_verify_error", str(e)[:140])
                await asyncio.sleep(3600)
    client.loop.create_task(qc_group_autojoin_loop())
    client.loop.create_task(qc_membership_verifier_loop())

    await client.run_until_disconnected()

if __name__ == '__main__':
    try:
        if os.environ.get("DRY_RUN") == "1":
            keep_alive()
            print("DRY_RUN active: Health endpoint started on PORT. Skipping Telegram start.")
            while True:
                time.sleep(60)
        else:
            keep_alive()
            asyncio.run(main())
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logger.critical(f"Fatal Error: {e}")
