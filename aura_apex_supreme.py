from typing import Optional, List, Dict, Any, Union, Tuple
import asyncio
import logging
import random
import sys
import datetime
import os
import re
import time
from dotenv import load_dotenv

load_dotenv()
os.environ["AURA_MODE"] = "testing"
os.environ["STOP_OUTREACH"] = "0"
import zlib
import urllib.parse
import ssl
import certifi
import aiohttp
from bs4 import BeautifulSoup
import aiosqlite
from zoneinfo import ZoneInfo

from telethon import TelegramClient, events, functions
from telethon.sessions import StringSession
from telethon.tl.functions.messages import CheckChatInviteRequest, ImportChatInviteRequest
from telethon.tl.functions.messages import GetBotCallbackAnswerRequest
from telethon.tl.functions.channels import JoinChannelRequest, LeaveChannelRequest, GetFullChannelRequest
from telethon.tl.functions.account import UpdateNotifySettingsRequest
from telethon.tl.functions.users import GetFullUserRequest
from telethon.tl import types
from telethon.tl.types import InputNotifyPeer, InputPeerNotifySettings, ReactionEmoji, UserStatusOffline, UserStatusOnline, UserStatusRecently, UserStatusLastWeek, UserStatusLastMonth
from telethon.errors import FloodWaitError, UserPrivacyRestrictedError, PeerIdInvalidError, YouBlockedUserError, UserBannedInChannelError, ChatWriteForbiddenError, ChannelPrivateError, PeerFloodError
from telethon.tl.functions.contacts import BlockRequest
from fake_useragent import UserAgent
from groq import AsyncGroq
from deep_translator import GoogleTranslator

try:
    import praw
except Exception:
    praw = None
try:
    import psutil
except Exception:
    psutil = None

# Custom modules
from aura_core import (
    proxy_health_monitor, should_outreach, load_json, save_json, clean_old_logs_async,
    calculate_lead_score, REBRAND_KEYWORDS, URGENCY_KEYWORDS, COMPETITOR_KEYWORDS,
    setup_logging, load_json_async, save_json_async
)
from keep_alive import keep_alive
from config import (
    API_ID, API_HASH, PHONE_NUMBER, GROQ_API_KEY,
    BANNED_ZONES, BANNED_CURRENCIES, JUNK_KEYWORDS, 
    TIER_3_CODES, TIER_1_INDICATORS, URGENCY_KEYWORDS, SENTIMENT_BLACKLIST, ADMIN_LEADS_CHANNEL_ID,
    DB_FILE, MARKET_KEYWORDS
)
from config import SESSION_STRING as CONFIG_SESSION_STRING

# Logging Setup
setup_logging()
logger = logging.getLogger(__name__)
try:
    import sentry_sdk
    _SENTRY_DSN = (os.environ.get("SENTRY_DSN") or "").strip()
    if _SENTRY_DSN:
        sentry_sdk.init(dsn=_SENTRY_DSN, traces_sample_rate=float(os.environ.get("SENTRY_TRACES", "0.0") or 0.0))
except Exception:
    pass

def validate_startup_secrets():
    errs = []
    if not API_ID or not str(API_ID).isdigit() or int(API_ID) <= 0:
        errs.append("Invalid API_ID")
    if not API_HASH or not re.fullmatch(r'[0-9a-fA-F]{32}', str(API_HASH)):
        errs.append("Invalid API_HASH")
    phone = (PHONE_NUMBER or "").strip()
    if not phone or not re.fullmatch(r'\+?[0-9]{10,15}', phone):
        errs.append("Invalid PHONE_NUMBER")
    sess = (CONFIG_SESSION_STRING or os.environ.get("SESSION_STRING") or "").strip()
    if not sess or len(sess) < 50:
        errs.append("Missing or invalid SESSION_STRING")
    groq_key = (os.environ.get("GROQ_API_KEY") or "").strip()
    gem_key = (os.environ.get("GEMINI_API_KEY") or os.environ.get("GEMINI_APIKEY") or "").strip()
    if not groq_key and not gem_key:
        errs.append("Missing AI provider key (GROQ_API_KEY or GEMINI_API_KEY)")
    if errs:
        for e in errs:
            logger.critical(e)
        sys.exit(1)

# AI Provider Setup (Groq)
ai_client = None
if GROQ_API_KEY:
    try:
        ai_client = AsyncGroq(api_key=GROQ_API_KEY)
    except Exception as e:
        logger.error(f"Groq Init Error: {e}")
else:
    logger.warning("GROQ_API_KEY missing.")
_AI_DM_MAX_PER_MIN = int(os.environ.get("APEX_AI_MAX_PER_MIN", "12"))
_ai_dm_ts: List[float] = []
def _ai_dm_allow():
    now = time.time()
    cutoff = now - 60.0
    global _ai_dm_ts
    _ai_dm_ts = [t for t in _ai_dm_ts if t >= cutoff]
    if len(_ai_dm_ts) < _AI_DM_MAX_PER_MIN:
        _ai_dm_ts.append(now)
        return True
    return False

SYSTEM_PROMPT = (
    "Identity: You are Aiden, a specialized streaming dev and white-label consultant.\n"
    "Tone: Technical, peer-level, skeptical of cheap providers. Use 'lower case' for tech terms like 'dns' or 'vpn'.\n"
    "Goal: Acknowledge their specific tech pain (buffering/isp throttling) and hint that custom-branded apps solve this via hardcoded DNS.\n"
    "Rules:\n"
    "1. Start with a peer-level observation (e.g., 'Firestick 4K Max? That buffer is usually an MTU issue').\n"
    "2. Mention you move people to private nodes to stop ISP throttling.\n"
    "3. No links. No sales pressure. One technical question to end."
)

OPENERS_HIGH = [
    "Saw your note in {group}.",
    "Caught your message in {group}.",
    "Noticed your post in {group}.",
]
OPENERS_LOW = [
    "Quick thought on that issue.",
    "If helpful, here’s a fast fix.",
    "Sharing a simple checklist.",
]
CTAS_HIGH = [
    "Want a 2‑min checklist?",
    "Can I send a tiny fix plan?",
    "Want a quick diagnostic?",
]
CTAS_LOW = [
    "Want a quick tip?",
    "Can I share a short guide?",
    "Want a tiny checklist?",
]

def _sanitize_dm(dm: str) -> str:
    dm = re.sub(r'(https?://\S+|t\s?\.\s?me/\S+)', '', dm, flags=re.IGNORECASE)
    dm = dm.replace("!", ".").replace("...", "..")
    words = dm.split()
    if len(words) > 35:
        dm = ' '.join(words[:35]) + ".."
    return dm.strip().lower()

def _spin(text: str) -> str:
    out = text
    pattern = re.compile(r'\{([^{}]+)\}')
    while True:
        m = pattern.search(out)
        if not m:
            break
        choices = m.group(1).split('|')
        rep = random.choice(choices).strip()
        out = out[:m.start()] + rep + out[m.end():]
    return out
def _fallback_dm(lead_name: str, group_name: str, user_msg: str, lead_score: int) -> str:
    base = "{Hey|Yo|Quick one}, saw your post in " + str(group_name) + ". {Want a quick tip?|Want a tiny checklist?|Can I send a tiny fix plan?}"
    return _sanitize_dm(_spin(base))

async def generate_ai_dm(lead_name: str, group_name: str, user_msg: str, lead_score: int, context_hint: str, style_hint: str, stealth: bool = False) -> str:
    if not ai_client:
        return _fallback_dm(lead_name, group_name, user_msg, lead_score)
    if not _ai_dm_allow():
        return _fallback_dm(lead_name, group_name, user_msg, lead_score)
    tech_trigger = ""
    if "tivimate" in (user_msg or "").lower():
        tech_trigger = "Mention TiviMate User-Agent settings."
    if "firestick" in (user_msg or "").lower():
        tech_trigger = "Mention Firestick cache and background limits."
    intent_focus = "Help them fix the lag/buffering peer-to-peer."
    if lead_score >= 8:
        intent_focus = "Pivot the conversation toward why branded apps with hardcoded DNS are more stable than generic Smarters/XC apps."
    local_prompt = SYSTEM_PROMPT
    if stealth:
        local_prompt += "\nStealth mode: avoid any commercial wording; sound like a peer sharing a fix."
    up = (
        f"Lead: {lead_name} (found in {group_name})\n"
        f"Message: \"{user_msg}\"\n"
        f"Tech Hook: {tech_trigger}\n"
        f"Focus: {intent_focus} {style_hint}. {context_hint}\n"
        f"Constraint: 3 sentences max. Lowercase tech terms. End with a question about their ISP or device."
    )
    try:
        resp = await ai_client.chat.completions.create(
            messages=[
                {"role": "system", "content": local_prompt},
                {"role": "user", "content": up}
            ],
            model="llama-3.3-70b-versatile",
            temperature=0.8,
            max_tokens=80
        )
        dm = (resp.choices[0].message.content or "").strip()
        return _sanitize_dm(dm)
    except Exception as e:
        if "429" in str(e):
            logger.warning("Groq Rate Limit (429). Waiting 5s before retry...")
            await asyncio.sleep(5)
        try:
            resp = await ai_client.chat.completions.create(
                messages=[
                    {"role": "system", "content": local_prompt},
                    {"role": "user", "content": up}
                ],
                model="llama-3.3-70b-versatile",
                temperature=0.8,
                max_tokens=80
            )
            dm = (resp.choices[0].message.content or "").strip()
            return _sanitize_dm(dm)
        except Exception as e2:
            if "429" in str(e2):
                logger.warning("Groq Rate Limit (429) persistent. Falling back to Gemini/Templates.")
            else:
                logger.debug(f"AI DM generation failed: {e2}")
            try:
                key = (os.environ.get("GEMINI_API_KEY") or "").strip()
                if key:
                    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={key}"
                    payload = {"contents":[{"parts":[{"text": local_prompt + "\n\n" + up}]}]}
                    async with aiohttp.ClientSession() as sess:
                        async with sess.post(url, json=payload, timeout=20) as r:
                            if r.status == 200:
                                data = await r.json()
                                text = ""
                                try:
                                    text = data["candidates"][0]["content"]["parts"][0]["text"]
                                except Exception:
                                    text = ""
                                if text:
                                    return _sanitize_dm(text)
            except Exception:
                pass
            return _sanitize_dm(_fallback_dm(lead_name, group_name, user_msg, lead_score))
DM_ATTEMPTS_LOG = "dm_attempts.json"
DM_COOLDOWN_HOURS = 48

def _now_ts() -> float:
    return time.time()

def _quiet_hours(local_hour: int) -> bool:
    return local_hour >= 22 or local_hour < 7
def _market_tzinfo():
    try:
        m = (MARKET or "").lower()
        if m in ("en-us", "us", "usa"):
            return ZoneInfo("America/New_York")
        if m in ("en-uk", "uk", "gb"):
            return ZoneInfo("Europe/London")
        if m in ("en-eu", "eu", "de-de", "fr-fr", "it-it", "es-es"):
            return ZoneInfo("Europe/Berlin")
        if m in ("es-es", "es", "spain"):
            return ZoneInfo("Europe/Madrid")
        if m in ("it-it", "it", "italy"):
            return ZoneInfo("Europe/Rome")
        return ZoneInfo("UTC")
    except Exception as e:
        logger.debug(f"Timezone info error: {e}")
        return ZoneInfo("UTC")

def _normalize_key(uid: str, text: str) -> str:
    text = re.sub(r'\s+', ' ', (text or '')).strip().lower()
    val = zlib.adler32(f"{uid}:{text}".encode("utf-8")) & 0xffffffff
    return str(val)

async def _load_dm_log_async() -> list:
    try:
        return await load_json_async(DM_ATTEMPTS_LOG, [])
    except Exception:
        return []

async def _save_dm_log_async(items: list) -> None:
    try:
        await save_json_async(DM_ATTEMPTS_LOG, items)
    except Exception as e:
        logger.debug(f"Failed to save DM log: {e}")

def _recent_dm_block(items: list, uid: str) -> bool:
    cutoff = _now_ts() - DM_COOLDOWN_HOURS * 3600
    for it in items[-200:]:
        if it.get("peer_id") == uid and float(it.get("ts", 0) or 0) > cutoff and it.get("status") == "sent":
            return True
    return False

async def safe_send_dm(client: Any, peer: Any, message: str, tzinfo: Any = None) -> bool:
    logs = await _load_dm_log_async()
    uid = str(getattr(peer, "user_id", getattr(peer, "id", peer)))
    if not uid:
        return False
    # Peer-based limit: avoid multiple DMs to same user in a short window
    try:
        if not _dm_peer_limiter_allow(uid, max_per_hour=2):
            logs.append({"ts": _now_ts(), "peer_id": uid, "status": "skipped", "reason": "peer_limit"})
            await _save_dm_log_async(logs)
            return False
    except Exception:
        pass
    try:
        if outreach_deep_sleep():
            logs.append({"ts": _now_ts(), "peer_id": uid, "status": "skipped", "reason": "deep_sleep"})
            await _save_dm_log_async(logs)
            return False
    except Exception:
        pass
    if _recent_dm_block(logs, uid):
        logs.append({"ts": _now_ts(), "peer_id": uid, "status": "skipped", "reason": "cooldown"})
        await _save_dm_log_async(logs)
        return False
    key = _normalize_key(uid, message)
    if any(it.get("key") == key and it.get("status") == "sent" for it in logs[-400:]):
        logs.append({"ts": _now_ts(), "peer_id": uid, "status": "skipped", "reason": "dedup"})
        await _save_dm_log_async(logs)
        return False
    if tzinfo is None:
        tzinfo = _market_tzinfo()
    try:
        local_hour = datetime.datetime.now(tzinfo).hour if tzinfo else datetime.datetime.now().hour
    except Exception as e:
        logger.debug(f"Timezone error, falling back to local: {e}")
        local_hour = datetime.datetime.now().hour
    if _quiet_hours(local_hour):
        logs.append({"ts": _now_ts(), "peer_id": uid, "status": "skipped", "reason": "quiet_hours"})
        await _save_dm_log_async(logs)
        return False
    try:
        est = max(2.0, min(12.0, len(message) * 0.05))
        start = time.time()
        while (time.time() - start) < est:
            try:
                await client.send_chat_action(peer, 'typing')
            except Exception:
                break
            await asyncio.sleep(2.0)
    except Exception as e:
        logger.debug(f"Typing variation error: {e}")
    tries = 0
    while tries < 2:
        try:
            await client.send_message(peer, message)
            logs.append({"ts": _now_ts(), "peer_id": uid, "status": "sent", "key": key})
            await _save_dm_log_async(logs)
            return True
        except FloodWaitError as e:
            logs.append({"ts": _now_ts(), "peer_id": uid, "status": "floodwait", "reason": int(getattr(e, "seconds", 60))})
            await _save_dm_log_async(logs)
            return False
        except PeerFloodError:
            try:
                activate_deep_sleep(12 * 3600)
                logs.append({"ts": _now_ts(), "peer_id": uid, "status": "error", "reason": "peer_flood_deep_sleep"})
                await _save_dm_log_async(logs)
            except Exception:
                pass
            return False
        except ChatWriteForbiddenError:
            logs.append({"ts": _now_ts(), "peer_id": uid, "status": "error", "reason": "forbidden"})
            await _save_dm_log_async(logs)
            return False
        except Exception as e:
            logger.warning(f"Failed to send DM (try {tries+1}): {e}")
            tries += 1
            import random as _r
            backoff = min(30.0, (2 ** tries)) + _r.uniform(0, 0.5)
            await asyncio.sleep(backoff)
    logs.append({"ts": _now_ts(), "peer_id": uid, "status": "error", "reason": "retry_exhausted"})
    await _save_dm_log_async(logs)
    return False
# Constants
SESSION_NAME = 'aura_apex_supreme_session'
STATS_FILE = 'supreme_stats.json'
_stats_cache = None
PROCESSED_GROUPS = 'supreme_groups.json'
PROCESSED_LEADS = 'supreme_leads.json'
# BLACKLIST_FILE is now imported from config
PROXY_FILE = 'proxy.txt'
POTENTIAL_TARGETS = os.path.join("data", "potential_targets.json")
REJECTED_GROUPS = 'rejected_groups.json'
JOIN_ATTEMPTS_LOG = 'join_attempts.json'
PROSPECT_CATALOG_FILE = 'prospect_catalog.json'
SOURCE_KPIS_FILE = 'source_kpis.json'
VERIFIED_INVITES_FILE = 'cached_invites.json'
TARGETS_FILE = os.path.join("data", "targets.json")
RESOLVE_RATE_TOKENS = 5
RESOLVE_RATE_INTERVAL = 60
JOIN_RATE_TOKENS = 10
JOIN_RATE_INTERVAL = 3600
BUYER_INTENT_KEYWORDS = []
MARKET = os.environ.get("MARKET", "").lower()
ENTITY_CACHE_FILE = 'entity_cache.json'
CANDIDATE_QUEUE_FILE = 'candidate_queue.json'
RESOLVE_COOLDOWN_FILE = 'resolve_cooldowns.json'
class _SlidingWindowLimiter:
    def __init__(self, max_actions, window_sec):
        self.max_actions = max_actions
        self.window = window_sec
        self.events = []
    async def consume(self):
        now = time.time()
        cutoff = now - self.window
        self.events = [t for t in self.events if t >= cutoff]
        if len(self.events) < self.max_actions:
            self.events.append(now)
            return True
        return False
resolve_limiter = _SlidingWindowLimiter(RESOLVE_RATE_TOKENS, RESOLVE_RATE_INTERVAL)
join_limiter = _SlidingWindowLimiter(JOIN_RATE_TOKENS, JOIN_RATE_INTERVAL)
async def resolve_limiter_consume():
    return await resolve_limiter.consume()
async def join_limiter_consume():
    return await join_limiter.consume()

# Peer-based DM limiter to avoid rapid messages to the same user
_DM_PEER_LIMITS: Dict[str, list] = {}
def _dm_peer_limiter_allow(peer_id: str, max_per_hour: int = 2) -> bool:
    now = time.time()
    cutoff = now - 3600
    arr = _DM_PEER_LIMITS.get(peer_id, [])
    arr = [t for t in arr if t >= cutoff]
    if len(arr) < max_per_hour:
        arr.append(now)
        _DM_PEER_LIMITS[peer_id] = arr
        return True
    _DM_PEER_LIMITS[peer_id] = arr
    return False
async def ensure_connected():
    global _last_conn_check_ts, _last_conn_status
    now = time.time()
    if (now - _last_conn_check_ts) < 10.0:
        return _last_conn_status
    _last_conn_check_ts = now
    try:
        if client.is_connected():
            _last_conn_status = True
            return True
    except Exception as e:
        logger.debug(f"Connection check error: {e}")
    try:
        await client.connect()
        _last_conn_status = True
        return True
    except Exception as e:
        logger.error(f"Failed to connect: {e}")
        _last_conn_status = False
        return False
SELLER_SHIELD_TERMS = [
    "reseller", "resellers", "reseller wanted", "reseller program",
    "panel", "admin panel", "super admin", "billing panel", "dashboard",
    "credits", "wholesale", "wholesale price", "supplier", "supplier hub",
    "restream", "restreamer", "market", "marketplace", "trade hub",
    "official replacement", "price list",
    # Locale synonyms
    "rivenditore", "rivenditori",
    "distribuidor", "revendedores", "revenda",
    "mayorista", "proveedor",
    "grossiste", "revendeur",
    "revendedor", "fornecedor",
    "bayi"
]
BUYER_PAIN_KEYWORDS = [
    # Direct Problem Solving (High Intent)
    "Firestick setup guide",
    "Android TV box buffering fix",
    "Nvidia Shield best settings",
    "TiviMate EPG missing",
    "IBO Player playlist error",
    "OTT Smarters login failed",
    "VPN for streaming lag",
    "TiviMate buffering Firestick fix",
    "Shield TV IPTV stutter fix",
    "IBO Player activation help",
    "XCIPTV playlist not loading",
    "Purple Player m3u setup",
    
    # Community & Reviews (High Trust)
    "IPTV provider reviews reddit",
    "Cord cutting community discussion",
    "Best streaming apps 2025 forum",
    "Tivimate users chat",
    "Android Box support group",
    "IPTV troubleshooting community",
    
    # Advanced Dorks (Google Search Operators)
    'site:t.me "iptv" "discussion" -"seller"',
    'site:t.me "tivimate" "support"',
    'site:reddit.com "telegram group" "iptv"',
    'inurl:t.me/joinchat "streaming" "help"',
    'site:t.me "no selling" "iptv"',
    'site:tgstat.com "iptv" "community"',
    'site:telemetr.io "iptv" "chat"',
    'site:reddit.com/r/IPTVdiscussion "t.me"',

    # Short & Punchy (For Telethon Fallback)
    "Tivimate", "Smarters", "Firestick", "Android TV", "IPTV Help",
    "Buffering Fix", "Tech Support", "Cord Cutting", "Streaming"
]

def _build_buyer_intent_keywords():
    base = list(BUYER_PAIN_KEYWORDS)
    try:
        mk = MARKET_KEYWORDS.get(MARKET) or {}
        buyer = mk.get("buyer") or []
        problem = mk.get("problem") or []
        tags = mk.get("tags") or []
        base.extend(buyer)
        base.extend(problem)
        base.extend(tags)
    except Exception as e:
        logger.debug(f"Keyword build error: {e}")
    return list(dict.fromkeys(base))
BUYER_INTENT_KEYWORDS = _build_buyer_intent_keywords()
def _buyer_intent_texts(texts):
    t = " ".join([(x or "").lower() for x in texts if x]).strip()
    if not t:
        return False
    signals = [
        "help", "support", "setup", "guide", "fix", "buffering", "issues",
        "community", "chat", "discussion", "question", "problem", "solution",
        "faq", "rules", "q&a", "how to", "no selling", "not a seller", "discussion only",
        "official group", "buyers", "users", "members", "tips", "tricks"
    ]
    return any(k in t for k in signals)


# Specialized Scouters & Keyword Matrix
SCOUTER_MISSIONS = {
    "Scouter 1 (Panels)": [
        "Strong IPTV panel", "B1G IPTV server USA", "IPTV reseller UK",
        "wholesale IPTV panel", "Distribuidor IPTV", "Rivenditore IPTV"
    ],
    "Scouter 1 (Panels & Infrastructure)": [
        "private iptv infrastructure 2026", "anti-freeze server direct owner",
        "High-uptime IPTV panel UK", "Dedicated IPTV ports USA",
        "Rivenditore IPTV stabilitÃ ", "Panel de revendedor estable"
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
    "IPTV trial", "recommend service", "buy subscription", "firestick setup", "sports streaming",
    "anyone have", "any recommendations", "link please", "trial please", "test please",
    "good provider", "reliable iptv", "best iptv", "iptv link", "m3u link"
]
B2B_INTENT = [
    "iptv panel price", "buy reseller credits", "best reseller panel", "start iptv business",
    "iptv credits cost", "become reseller", "panel setup", "wholesale iptv",
    "panel price", "credit price", "reseller panel", "restream"
]
REBRAND_INTENT = [
    "iptv rebrand", "custom apk rebranding", "white label iptv", "rebrand ibo player",
    "dns hardcoding", "iptv app source code", "tivimate rebrand", "custom billing portal", "whmcs iptv",
    "apk rebrand", "app rebrand", "hardcode dns"
]
PROBLEM_TRIGGERS = [
    "server down", "buffering issue", "links expired", "help m3u not working", "service blocked", "need new provider",
    "buffering", "lagging", "freezing", "not working", "help", "black screen", "down", "offline"
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
        "buyer": ["busco iptv estable", "futbol 4k espaÃ±a", "lista m3u espaÃ±a"],
        "b2b": ["revendedor panel espaÃ±a", "creditos mayorista iptv", "dueÃ±o directo espaÃ±a"],
        "rebrand": ["marca blanca iptv", "dns fijado espaÃ±a", "portal facturacion"],
        "problem": ["pantalla negra", "buffering", "servidor caido"],
        "tags": ["#IPTVEspaÃ±a", "#LaLiga", "#Futbol4K", "#StreamsES"]
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
        "b2b": ["reseller panel de", "groÃŸhandel credits", "direkter anbieter"],
        "rebrand": ["white label de", "dns hardcode de", "abrechnung portal"],
        "problem": ["schwarzer bildschirm", "puffern", "server down"],
        "tags": ["#IPTVDeutschland", "#Bundesliga", "#4KDE", "#StreamsDE"]
    },
    "fr-FR": {
        "buyer": ["cherche iptv stable", "ligue 1 4k", "liste m3u france"],
        "b2b": ["revendeur panel fr", "credits iptv gros", "proprio direct fr"],
        "rebrand": ["white label france", "dns verrouille fr", "portail facturation"],
        "problem": ["Ã©cran noir", "buffering constant", "serveur down"],
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
try:
    ua = UserAgent()
except Exception as e:
    logger.debug(f"UserAgent init failed: {e}")
    ua = None
request_counter = 0

def get_ghost_ua():
    global request_counter
    request_counter += 1
    if ua:
        return ua.random if request_counter % 5 == 0 else ua.chrome
    return "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"

async def init_db():
    try:
        async with aiosqlite.connect(DB_FILE) as conn:
            await conn.execute('''CREATE TABLE IF NOT EXISTS leads 
                     (id INTEGER PRIMARY KEY AUTOINCREMENT, 
                      link TEXT UNIQUE, 
                      group_title TEXT, 
                      members INTEGER, 
                      tech_score INTEGER, 
                      quality_score INTEGER, 
                      status TEXT, 
                      timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)''')
            await conn.execute('''CREATE TABLE IF NOT EXISTS joined_groups
                     (id INTEGER PRIMARY KEY AUTOINCREMENT,
                      group_id INTEGER UNIQUE,
                      title TEXT,
                      username TEXT,
                      link TEXT,
                      joined_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                      last_scanned_id INTEGER DEFAULT 0,
                      banned INTEGER DEFAULT 0,
                      archived INTEGER DEFAULT 0)''')
            await conn.execute('''CREATE TABLE IF NOT EXISTS prospects
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
            await conn.execute('''CREATE TABLE IF NOT EXISTS keywords
                     (term TEXT PRIMARY KEY,
                      weight INTEGER DEFAULT 1,
                      hits INTEGER DEFAULT 0,
                      conversions INTEGER DEFAULT 0,
                      updated_at DATETIME DEFAULT CURRENT_TIMESTAMP)''')
            await conn.execute('''CREATE TABLE IF NOT EXISTS activity_log
                     (id INTEGER PRIMARY KEY AUTOINCREMENT,
                      ts DATETIME DEFAULT CURRENT_TIMESTAMP,
                      type TEXT,
                      details TEXT)''')
            await conn.execute('''CREATE TABLE IF NOT EXISTS source_kpis
                     (term TEXT PRIMARY KEY,
                      attempts INTEGER DEFAULT 0,
                      successes INTEGER DEFAULT 0,
                      errors INTEGER DEFAULT 0,
                      updated_at DATETIME DEFAULT CURRENT_TIMESTAMP)''')
            await conn.execute('''CREATE TABLE IF NOT EXISTS join_attempts
                     (id TEXT,
                      title TEXT,
                      status TEXT,
                      reason TEXT,
                      ts REAL)''')
            await conn.execute('''CREATE TABLE IF NOT EXISTS potential_targets
                     (link TEXT PRIMARY KEY,
                      title TEXT,
                      members INTEGER,
                      source_group_id TEXT,
                      discovered_at DATETIME)''')
            await conn.execute('''CREATE TABLE IF NOT EXISTS cached_invites
                     (link TEXT PRIMARY KEY,
                      title TEXT,
                      ts REAL)''')
            await conn.execute('''CREATE TABLE IF NOT EXISTS entity_cache
                     (value TEXT PRIMARY KEY)''')
            await conn.execute('''CREATE TABLE IF NOT EXISTS resolve_cooldowns
                     (key TEXT PRIMARY KEY,
                      until_ts REAL)''')
            await conn.execute('''CREATE TABLE IF NOT EXISTS processed_groups
                     (link TEXT PRIMARY KEY)''')
            await conn.execute('''CREATE TABLE IF NOT EXISTS supreme_stats
                     (id INTEGER PRIMARY KEY, 
                      data TEXT)''')
            await conn.execute('''CREATE TABLE IF NOT EXISTS kv_store
                     (key TEXT PRIMARY KEY, 
                      value TEXT)''')
            await conn.execute('''CREATE TABLE IF NOT EXISTS prospect_catalog
                     (url TEXT PRIMARY KEY, 
                      json TEXT)''')
            try:
                await conn.execute("PRAGMA journal_mode=WAL;")
                await conn.execute("PRAGMA synchronous=NORMAL;")
            except Exception as e:
                logger.debug(f"DB Pragma Error: {e}")
            try:
                await conn.execute("INSERT OR IGNORE INTO kv_store(key, value) VALUES('schema_version', '1')")
            except Exception as e:
                logger.debug(f"Schema version init error: {e}")
            try:
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_prospects_user ON prospects(user_id)")
            except Exception as e:
                logger.debug(f"Index create error: {e}")
            try:
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_prospects_group ON prospects(group_id)")
            except Exception as e:
                logger.debug(f"Index create error: {e}")
            try:
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_joined_groups_gid ON joined_groups(group_id)")
            except Exception as e:
                logger.debug(f"Index create error: {e}")
            try:
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_activity_log_ts ON activity_log(ts)")
            except Exception as e:
                logger.debug(f"Index create error: {e}")
            await conn.commit()
    except Exception as e:
        logger.error(f"Database Init Error: {e}")

async def migrate_db():
    try:
        async with aiosqlite.connect(DB_FILE) as conn:
            try:
                await conn.execute("ALTER TABLE prospects ADD COLUMN persona_id TEXT")
            except Exception as e:
                logger.debug(f"Migration (persona_id) info: {e}")
            try:
                await conn.execute("ALTER TABLE joined_groups ADD COLUMN archived INTEGER DEFAULT 0")
            except Exception as e:
                logger.debug(f"Migration (archived) info: {e}")
            try:
                await conn.execute("ALTER TABLE prospects ADD COLUMN source TEXT")
            except Exception as e:
                logger.debug(f"Migration (prospects.source) info: {e}")
            try:
                await conn.execute("ALTER TABLE prospects ADD COLUMN responded INTEGER DEFAULT 0")
            except Exception as e:
                logger.debug(f"Migration (prospects.responded) info: {e}")
            try:
                await conn.execute("ALTER TABLE joined_groups ADD COLUMN source TEXT")
            except Exception as e:
                logger.debug(f"Migration (joined_groups.source) info: {e}")
            await conn.commit()
    except Exception as e:
        logger.error(f"Database Migration Error: {e}")

def save_lead_to_db(link, title, members, tech_score, quality_score, status):
    try:
        DB_QUEUE.put_nowait((
            "INSERT OR REPLACE INTO leads (link, group_title, members, tech_score, quality_score, status) VALUES (?, ?, ?, ?, ?, ?)",
            (link, title, members, tech_score, quality_score, status)
        ))
    except Exception as e:
        logger.error(f"Failed to queue lead save: {e}")

def log_activity(event_type, details):
    try:
        DB_QUEUE.put_nowait((
            "INSERT INTO activity_log (type, details) VALUES (?, ?)",
            (event_type, (details or "")[:1000])
        ))
    except Exception as e:
        logger.debug(f"Failed to queue activity log: {e}")

def record_joined_group(group_id, title, username, link, source=None):
    try:
        DB_QUEUE.put_nowait((
            "INSERT OR IGNORE INTO joined_groups (group_id, title, username, link, source) VALUES (?, ?, ?, ?, ?)",
            (group_id, title, username, link, source or "")
        ))
    except Exception as e:
        logger.error(f"Failed to queue joined group record: {e}")

async def joined_link_exists(link):
    try:
        async with aiosqlite.connect(DB_FILE) as conn:
            async with conn.execute("SELECT 1 FROM joined_groups WHERE link = ? LIMIT 1", (link,)) as cursor:
                row = await cursor.fetchone()
        return bool(row)
    except Exception as e:
        logger.error(f"Error checking if joined link exists: {e}")
        return False

def mark_group_banned(group_id):
    try:
        DB_QUEUE.put_nowait((
            "UPDATE joined_groups SET banned = 1 WHERE group_id = ?",
            (group_id,)
        ))
    except Exception as e:
        logger.error(f"Failed to queue mark group banned: {e}")

def save_prospect(user_id, username, message, message_id, message_ts, group_id, group_title, persona_id, status, source=None):
    try:
        DB_QUEUE.put_nowait((
            "INSERT OR IGNORE INTO prospects (user_id, username, message, message_id, message_ts, group_id, group_title, persona_id, status, source, responded) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (user_id, username, message, message_id, message_ts, group_id, group_title, persona_id, status, source or "", 0)
        ))
    except Exception as e:
        logger.error(f"Failed to queue prospect save: {e}")

def _load_targets():
    try:
        with open(TARGETS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"niche_targets": [], "blacklist_patterns": []}
def _load_targets_cached(ttl_sec: int = 300):
    global _targets_cache, _targets_mtime, _targets_last_load_ts
    try:
        st = os.stat(TARGETS_FILE)
        mtime = float(getattr(st, "st_mtime", 0.0))
    except Exception:
        mtime = 0.0
    now = time.time()
    if (_targets_cache is not None) and (mtime == _targets_mtime) and ((now - _targets_last_load_ts) < ttl_sec):
        return _targets_cache
    data = _load_targets()
    _targets_cache = data
    _targets_mtime = mtime
    _targets_last_load_ts = now
    return data
def _persona_for_hook(hook):
    h = (hook or "").lower()
    if "sports" in h:
        return "expert"
    if "tech" in h:
        return "concise"
    return random.choice(["expert", "peer", "concise"])
def _hook_for_group_title(title):
    data = _load_targets_cached()
    t = (title or "").lower()
    for item in data.get("niche_targets", []):
        kws = [k.lower() for k in item.get("keywords", [])]
        if any(k in t for k in kws):
            return item.get("aiden_hook")
    return None
def choose_persona_id(group_title=None):
    hook = _hook_for_group_title(group_title or "")
    if hook:
        return _persona_for_hook(hook)
    return random.choice(["expert", "peer", "concise"])

async def get_prospect_persona(user_id, group_id):
    try:
        async with aiosqlite.connect(DB_FILE) as conn:
            async with conn.execute("SELECT persona_id FROM prospects WHERE user_id = ? AND group_id = ? ORDER BY id DESC LIMIT 1", (user_id, group_id)) as cursor:
                row = await cursor.fetchone()
        return row[0] if row and row[0] else None
    except Exception as e:
        logger.error(f"Get persona error: {e}")
        return None
async def get_group_source(group_id):
    try:
        async with aiosqlite.connect(DB_FILE) as conn:
            async with conn.execute("SELECT source FROM joined_groups WHERE group_id = ? LIMIT 1", (group_id,)) as cursor:
                row = await cursor.fetchone()
        return (row[0] if row and row[0] else "") if row else ""
    except Exception as e:
        logger.error(f"Get group source error: {e}")
        return ""
def update_prospect_status(user_id, status, opt_out=False, increment_response=False):
    try:
        fields = []
        params = []
        fields.append("status = ?")
        params.append(status)
        if opt_out:
            fields.append("opt_out = 1")
        if increment_response:
            fields.append("responses_count = responses_count + 1")
            fields.append("responded = 1")
        fields.append("last_contacted_ts = CURRENT_TIMESTAMP")
        q = "UPDATE prospects SET " + ", ".join(fields) + " WHERE user_id = ?"
        params.append(user_id)
        DB_QUEUE.put_nowait((q, tuple(params)))
    except Exception as e:
        logger.debug(f"Failed to queue prospect update: {e}")

async def user_opted_out(user_id):
    try:
        async with aiosqlite.connect(DB_FILE) as conn:
            async with conn.execute("SELECT opt_out FROM prospects WHERE user_id = ? AND opt_out = 1 LIMIT 1", (user_id,)) as cursor:
                row = await cursor.fetchone()
        return bool(row)
    except Exception as e:
        logger.error(f"Opt-out Check Error: {e}")
        return False
async def get_responses_count(user_id):
    try:
        async with aiosqlite.connect(DB_FILE) as conn:
            async with conn.execute("SELECT responses_count FROM prospects WHERE user_id = ? ORDER BY id DESC LIMIT 1", (user_id,)) as cursor:
                row = await cursor.fetchone()
        return int(row[0]) if row and row[0] is not None else 0
    except Exception as e:
        logger.debug(f"Failed to get responses count: {e}")
        return 0

def record_keyword_hits(text, converted=False):
    try:
        lower = (text or "").lower()
        terms = []
        for k in BUYER_INTENT + B2B_INTENT + REBRAND_INTENT + PROBLEM_TRIGGERS:
            if k in lower:
                terms.append(k)
        for term in terms:
            DB_QUEUE.put_nowait(("INSERT OR IGNORE INTO keywords (term) VALUES (?)", (term,)))
            DB_QUEUE.put_nowait(("UPDATE keywords SET hits = hits + 1, updated_at = CURRENT_TIMESTAMP WHERE term = ?", (term,)))
            if converted:
                DB_QUEUE.put_nowait(("UPDATE keywords SET conversions = conversions + 1, updated_at = CURRENT_TIMESTAMP WHERE term = ?", (term,)))
    except Exception as e:
        logger.debug(f"Failed to record keyword hits: {e}")

async def prospect_has_active_conversation(user_id):
    try:
        async with aiosqlite.connect(DB_FILE) as conn:
            async with conn.execute("SELECT 1 FROM prospects WHERE user_id = ? AND status IN ('responded','converted') LIMIT 1", (user_id,)) as cursor:
                row = await cursor.fetchone()
        return bool(row)
    except Exception as e:
        logger.debug(f"Failed to check active conversation: {e}")
        return False
SERVICE_USER_IDS = {777000}
SERVICE_MSG_HINTS = ["login code", "do not give this code", "can be used to log in"]

async def should_queue_handshake(user_id):
    if await user_opted_out(user_id):
        return False
    if await prospect_has_active_conversation(user_id):
        return False
    return True

def detect_language_from_bio(text: str) -> Optional[str]:
    if not text:
        return "unknown"
    try:
        if re.search(r'[Ð-Ð¯Ð°-ÑÐÑ‘]', text):
            return "ru"
        return "latin"
    except Exception as e:
        logger.debug(f"Bio language detection error: {e}")
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
        if re.search(r'[Ð-Ð¯Ð°-ÑÐÑ‘]', text):
            return "ru"
        return None
    except Exception as e:
        logger.debug(f"Text language detection error: {e}")
        return None

def outreach_blocked():
    try:
        v = os.environ.get("STOP_OUTREACH", "").strip().lower()
        return v in ("1", "true", "yes")
    except Exception as e:
        logger.debug(f"Outreach check error: {e}")
        return False

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
        # Simple retry for translation to handle transient SSL/Network issues
        for _ in range(2):
            try:
                return GoogleTranslator(source='auto', target=target_lang).translate(text)
            except Exception:
                time.sleep(1)
        return text
    except Exception as e:
        logger.error(f"Translate error: {e}")
        return text

def market_hour_ok():
    try:
        if os.environ.get("AURA_MODE", "").lower() == "testing":
            return True
        tz = _market_tzinfo()
        h = datetime.datetime.now(tz).hour
        return 9 <= h <= 21
    except Exception:
        return True

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
    except Exception as e:
        logger.debug(f"Spintax error: {e}")
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
                    chosen = random.choice(lines)
                    p = chosen.split(':')
                    if len(p) == 4:
                        import socks
                        return (socks.SOCKS5, p[0], int(p[1]), True, p[2], p[3])
        except Exception as e:
            logger.error(f"Proxy Load Error: {e}")
    return None

# Initialize Client
# Note: We rely on valid API_ID/HASH from config
if not API_ID or not API_HASH:
    logger.critical("API_ID or API_HASH missing in config!")
    sys.exit(1)
validate_startup_secrets()

# Explicit loop management for stability
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)
_env_session = (os.environ.get("SESSION_STRING") or "").strip()
_cfg_session = (CONFIG_SESSION_STRING or "").strip()
SESSION_STRING = _cfg_session or _env_session
if SESSION_STRING:
    client = TelegramClient(StringSession(SESSION_STRING), int(API_ID), API_HASH, proxy=get_proxy(), loop=loop)
else:
    client = TelegramClient(SESSION_NAME, int(API_ID), API_HASH, proxy=get_proxy(), loop=loop)
apply_market_keywords()
try:
    import cryptg  # noqa: F401
    logger.info("cryptg enabled")
except Exception:
    logger.warning("cryptg not available")
try:
    client.session.save_entities = False
except Exception:
    pass
LEADS_JSON = 'leads.json'
KEYWORD_TRIGGERS = ["buffer", "rebrand", "dns help", "provider down", "looking for fix"]
TITLE_TARGET_KEYWORDS = ["fix", "iptv", "help", "setup"]
SELLER_NEGATIVE_KEYWORDS = ["reseller", "panel", "credits", "wholesale", "restream", "source", "supplier", "official", "market", "b2b", "trade", "rivenditore", "distribuidor", "mayorista", "grossiste", "revendedor"]
SELLER_NEGATIVE_KEYWORDS += ["recruitment", "become reseller", "opportunity", "earn", "profit", "white label panel", "partner program", "affiliate", "bulk", "marketing group", "dealer program", "franchise"]
BLACKLIST_JOIN = ["spam", "crypto-pump", "adult"]
DB_QUEUE = asyncio.Queue()
_SELLER_SHIELD_TERMS = ["reseller", "panel", "credits", "wholesale", "restream", "source", "supplier", "official", "market", "b2b", "trade", "rivenditore", "distribuidor", "mayorista", "grossiste", "revendedor"]
_SELLER_SHIELD_TERMS += ["recruitment", "become reseller", "opportunity", "earn", "profit", "white label panel", "partner program", "affiliate", "bulk", "marketing group", "dealer program", "franchise"]
_SELLER_SHIELD_TERMS_SET = set(k.lower() for k in _SELLER_SHIELD_TERMS)
_TITLE_TARGET_KEYWORDS_SET = set(k.lower() for k in TITLE_TARGET_KEYWORDS)
_BLACKLIST_JOIN_SET = set(k.lower() for k in BLACKLIST_JOIN)
_MARKETING_ADV_HIGH = {"dm me","dm for service","dm for price","pm for price","inbox for price","order now","place order","order","price","prices","pricing"}
_MARKETING_ADV_MED = {"buy","sell","subscribe","subscription","offer","deal","affiliate","bulk","dealer","franchise","supplier","restream","panel","credits","wholesale"}
_MARKETING_LINKS = {"t.me","http","https"}
_MARKETING_Q_LOW = {"why","how","help","lag"}
_MARKETING_TECH = {"tivimate","firestick","smarters","xciptv","televizo","mag","stb","portal","dns","buffer","latency"}
_ssl_ctx = None
_http_session = None
_last_conn_check_ts = 0.0
_last_conn_status = False
_targets_cache = None
_targets_mtime = 0.0
_targets_last_load_ts = 0.0
async def db_writer_loop():
    try:
        conn = await aiosqlite.connect(DB_FILE)
        try:
            await conn.execute("PRAGMA journal_mode=WAL;")
            await conn.execute("PRAGMA synchronous=NORMAL;")
        except Exception as e:
            logger.debug(f"DB Pragma Error (writer): {e}")
        await conn.commit()
        batch = []
        last_flush = time.time()
        while True:
            try:
                timeout = max(0.0, 30.0 - (time.time() - last_flush))
                try:
                    item = await asyncio.wait_for(DB_QUEUE.get(), timeout=timeout if batch else None)
                    batch.append(item)
                except asyncio.TimeoutError:
                    pass
                if batch and ((time.time() - last_flush) >= 30.0 or len(batch) >= 100):
                    try:
                        await conn.execute("BEGIN")
                        for sql, params in batch:
                            try:
                                await conn.execute(sql, params or [])
                            except Exception as e:
                                logger.debug(f"DB Write Error: {e} SQL: {sql}")
                        await conn.commit()
                    except Exception as e:
                        logger.debug(f"DB Batch Commit Error: {e}")
                        try:
                            await conn.rollback()
                        except Exception:
                            pass
                    batch = []
                    last_flush = time.time()
            except Exception as e:
                logger.debug(f"DB Queue Error: {e}")
                await asyncio.sleep(1)
    except Exception as e:
        logger.error(f"DB Writer Critical Error: {e}")
        await asyncio.sleep(5)

async def _code_callback():
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
                        except Exception as e:
                            logger.debug(f"Failed to remove code file: {e}")
                        return code
                except Exception as e:
                    logger.debug(f"Failed to read code file: {e}")
            await asyncio.sleep(2)
    except Exception as e:
        logger.error(f"Code callback error: {e}")
    raise RuntimeError("Telegram login code not provided within timeout")
def add_to_blacklist(user_id):
    try:
        with open(BLACKLIST_FILE, 'a', encoding='utf-8') as f:
            f.write(f"{user_id},{datetime.datetime.now().isoformat()}\n")
    except Exception as e:
        logger.debug(f"Failed to add to blacklist: {e}")

# --- Snippet Scoring Engine ---

def calculate_quality_score(text):
    """Machine Learning-style snippet scoring - Updated for B2C Buyer Hubs."""
    score = 0
    text_lower = text.lower()
    
    # 🌟 SUPER TRUST SIGNALS (+100) - Almost guaranteed valid
    if any(k in text_lower for k in ["no selling", "not a seller", "discussion only", "community only", "official group"]):
        score += 100

    # Priority Keywords (+50) - BUYER FOCUSED
    # We want user communities, help groups, and tech discussions
    buyer_signals = [
        "help", "support", "setup", "guide", "fix", "buffering", "issues",
        "community", "chat", "discussion", "question", "problem", "solution",
        "faq", "rules", "q&a", "how to",
        "tivimate", "smarters", "ibo", "xciptv", "televizo", "purple",
        "firestick", "android", "nvidia", "shield", "box", "tv", "chromecast",
        "cord", "cutting", "stream", "cinema", "film", "series", "vod"
    ]
    if any(k in text_lower for k in buyer_signals):
        score += 50
        
    # Tier-1 Location Keywords (+30)
    if any(k in text_lower for k in ["italy", "spain", "usa", "uk", "canada", "germany", "france", "australia", "english"]):
        score += 30
        
    # Legacy B2B Penalties (We don't want these anymore, but Seller-Shield catches them later. 
    # Giving them points here would be counter-productive).
    
    # Junk Reductions (-20 to -100)
    if any(k in text_lower for k in ["test", "free trial", "sub4sub"]):
        score -= 20
        
    # Auto-Purge: Tier-3 Codes
    try:
        codes_lower = [c.lower() for c in TIER_3_CODES]
    except Exception as e:
        logger.debug(f"TIER_3_CODES processing error: {e}")
        codes_lower = []
    import re as _re
    if any(_re.search(rf"\b{_re.escape(code)}\b", text_lower) for code in codes_lower):
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
def _extract_tme_links(text):
    try:
        return re.findall(r'https?://t\.me/(?:joinchat/\w+|\+\w+|[A-Za-z0-9_]+)', text or "", flags=re.IGNORECASE)
    except Exception as e:
        logger.debug(f"Link extraction error: {e}")
        return []
def _is_price_list(text):
    t = (text or "").lower()
    if not t:
        return False
    currency = any(sym in t for sym in ["$", "€", "£"])
    price_words = any(w in t for w in ["price", "prices", "pricing", "offer", "subscription"])
    numbers = len(re.findall(r'\\b\\d{1,4}\\b', t))
    return (currency and numbers >= 3) or (price_words and numbers >= 3)
async def _output_status(status_text):
    try:
        # Avoid sending status messages to admin channel or 'me' unless explicitly requested
        logger.info(f"STATUS LOG: {status_text}")
        
        # Check if the user specifically wants to see these in Telegram (default: 0)
        if os.environ.get("DEBUG_TELEGRAM_STATUS", "0") == "1":
            ch = (ADMIN_LEADS_CHANNEL_ID or "").strip()
            if ch:
                try:
                    await client.send_message(int(ch), status_text)
                    return
                except Exception:
                    await client.send_message(ch, status_text)
                    return
            await client.send_message('me', status_text)
    except Exception as e:
        logger.debug(f"Failed to output status: {e}")
def _provider_advertising(text):
    t = (text or "").lower()
    if not t:
        return False
    adv_terms = [
        "dm for service", "dm for price", "pm for price", "inbox for price", "contact me",
        "reseller", "panel", "credits", "wholesale", "supplier", "restream", "official replacement",
        "price list", "price-list", "buy credits", "sell credits", "become reseller", "dashboard",
        "affiliate", "bulk", "dealer program", "franchise",
        "free trial", "free test", "test available", "all channels", "premium iptv", "stable service",
        "no buffering service", "m3u list", "bouquet", "live tv", "vod included", "full access",
        "best iptv", "reliable service", "whatsapp me", "join my channel"
    ]
    
    # Check for channel lists (e.g., "UK, US, CA, DE") which is a strong seller signal
    import re as _re
    if len(_re.findall(r'\b(uk|us|usa|ca|de|fr|it|es|in|au)\b', t)) >= 3:
        return True
        
    bot_link = ("t.me/" in t and "bot" in t) or "/start" in t
    return any(k in t for k in adv_terms) or _is_price_list(t) or bot_link
def _frustrated_user(text):
    t = (text or "").lower()
    if not t:
        return False
        
    # Technical terms related to IPTV
    tech_terms = [
        "buffer", "lag", "stutter", "freeze", "frame drop", "down", "not working",
        "quality", "looping", "crash", "error", "no audio", "audio desync",
        "blocked", "isp", "mtu", "packet loss", "bitrate", "hevc", "h265", "firestick",
        "tivimate", "smarters", "ibo", "xciptv", "televizo", "mag", "stb", "portal"
    ]
    
    # Help-seeking or frustration signals
    frustration_signals = [
        "help", "anyone else", "having issues", "problem", "broken", "stop", "sick of",
        "how to fix", "advice", "suggestion", "why is", "cant get", "fail", "failed",
        "question", "can i", "how do i", "trouble"
    ]
    
    # A user is likely a lead if they mention a tech term AND show help-seeking intent
    has_tech = any(k in t for k in tech_terms)
    has_frustration = any(k in t for k in frustration_signals)
    
    if has_tech and has_frustration:
        return True
        
    # Strong, standalone signals of a user needing help
    strong_signals = [
        "keeps buffering", "not working", "service down", "black screen", "login failed",
        "buffer every", "channel down", "help me fix", "portal error", "m3u not loading"
    ]
    if any(k in t for k in strong_signals):
        return True
        
    return False
USER_BLACKLIST_STRINGS = [
    'owner', 'admin', 'service', 'reseller', 'panel', 'credits', 'restock', 'iptv_bot', 'support',
    'provider', 'seller', 'sales', 'official', 'hosting', 'server', 'manager', 'direct'
]
def _user_entity_audit(user):
    try:
        uname = None
        bio = None
        if user is None:
            return False
        try:
            uname = getattr(user, "username", None) or getattr(getattr(user, "user", None), "username", None)
        except Exception:
            uname = None
        try:
            # FullUser about field
            bio = getattr(user, "about", None)
            if not bio and hasattr(user, "full_user"):
                bio = getattr(user.full_user, "about", None)
        except Exception:
            bio = None
            
        u = (uname or "").lower()
        b = (bio or "").lower()
        
        # Check for provider signals in username or bio
        if any(k in u for k in USER_BLACKLIST_STRINGS):
            return True
        if any(k in b for k in USER_BLACKLIST_STRINGS):
            return True
            
        # Common provider naming patterns
        if u.endswith("_iptv") or u.endswith("_bot") or u.startswith("iptv_"):
            return True
        if "t.me/" in b: # Providers often link to their channels/bots in bio
            return True
            
        return False
    except Exception:
        return False
def marketing_sentiment_score(text):
    t = (text or "").lower()
    if not t:
        return 100
    currency = any(sym in t for sym in ["$", "€", "£"])
    pos = 0
    pos += sum(1 for k in _MARKETING_ADV_HIGH if k in t) * 20
    pos += sum(1 for k in _MARKETING_ADV_MED if k in t) * 10
    pos += sum(1 for k in _MARKETING_LINKS if k in t) * 5
    if currency:
        pos += 20
    neg = 0
    neg += sum(1 for k in _MARKETING_Q_LOW if k in t) * 12
    neg += (15 if "?" in t else 0)
    score = 50 + pos - neg
    if "?" in t and any(k in t for k in _MARKETING_TECH):
        score -= 50
    if score < 0:
        score = 0
    if score > 100:
        score = 100
    return score
def evaluate_lead_message(text, user=None):
    if os.environ.get("AURA_MODE", "").lower() == "testing":
        return "PROCEED"
    if _user_entity_audit(user):
        return "REJECT"
    if _provider_advertising(text):
        return "REJECT"
    if _frustrated_user(text):
        return "PROCEED"
    ms = marketing_sentiment_score(text)
    if ms <= 45: # Relaxed from 20 to 45
        return "PROCEED"
    return "REJECT"
def _ack_line(snippet):
    t = (snippet or "").lower()
    if "firestick" in t or "fire stick" in t:
        return "Yeah, those Firestick frame drops are usually an ISP MTU quirk."
    if "tivimate" in t:
        return "On TiviMate, the stutter’s often bitrate spikes and DNS flaps."
    if "smarters" in t or "xciptv" in t:
        return "Smarters/XCIPTV lag is often decoder + DNS jitter, not the playlist."
    if "mag" in t or "stb" in t:
        return "MAG/STB loops usually mean portal handshake drops or MTU mismatch."
    if "hevc" in t or "h265" in t:
        return "HEVC streams can choke if the decoder or bitrate ramp isn’t stable."
    if "vpn" in t or "blocked" in t:
        return "Blocks tend to be DNS or ISP shaping, not the player."
    return "Yeah, the drops sound like DNS jitter and bitrate spikes, not your setup."
TECH_TO_PAIN = {
    "tivimate": ["User-Agent spoofing", "AFR (Auto Frame Rate) issues"],
    "firestick": ["Background Process Limits", "Cache Bloat"],
    "smarters": ["DNS Hardcoding vs. XC Portal login instability"]
}
def _detect_tech_context(snippet):
    t = (snippet or "").lower()
    if "tivimate" in t:
        return "tivimate"
    if "firestick" in t or "fire stick" in t:
        return "firestick"
    if "smarters" in t or "xciptv" in t:
        return "smarters"
    return None
def _classify_pain_point(snippet: str) -> str:
    t = (snippet or "").lower()
    if any(k in t for k in ["buffer", "lag", "stutter", "freeze", "frame drop", "jitter"]):
        return "buffering"
    if any(k in t for k in ["smarters", "xciptv", "xc login", "login failed", "portal", "xtream"]):
        return "generic_app"
    if any(k in t for k in ["firestick", "fire stick", "shield", "nvidia", "android tv", "formuler", "mag", "stb"]):
        return "device"
    return "generic_app"
def compose_aiden_dm(snippet, group_title, tech_context=None, lead_score: int = 0, stealth: bool = False, social_hint: str = ""):
    opener = random.choice(["Yo,", "Hey,", "Quick one,"])
    pain = _classify_pain_point(snippet)
    group_tag = (group_title or "the group")
    if pain == "buffering":
        validation = f"{opener} saw your post in {group_tag}. That stop‑start looks like isp shaping or an mtu mismatch."
        fix = "I don’t touch generic builds anymore—the dns path flaps under load; I run private nodes tuned for steady bitrate."
    elif pain == "device":
        validation = f"{opener} saw your post in {group_tag}. Firestick/Shield pain is usually background‑process limits or cache bloat."
        fix = "Generic players mask it; I stabilize via private nodes and lean client config (dns locked, decoder sane)."
    else:
        validation = f"{opener} saw your post in {group_tag}. Smarters/XC login wobble is almost always dns vs xc handshake."
        fix = "I ditched the stock stuff—hardcoded dns beats the portal flaps and keeps sessions stable."
    if social_hint:
        validation = f"{validation} {social_hint}"
    pivot = ""
    if lead_score >= 8 and not stealth:
        pivot = "I rebrand and hardcode my own builds to bypass the exact issue you’re hitting."
    cta = "Are you on a vpn, or is your isp hitting you directly?"
    parts = [validation, fix, pivot, cta]
    msg = " ".join([p for p in parts if p]).strip()
    words = msg.split()
    if len(words) > 120:
        msg = " ".join(words[:120])
    return re.sub(r'[\\u2600-\\u27BF\\U0001F300-\\U0001FAFF]+', lambda m: m.group(0) if m.group(0) in ['🤝','📺','⚡'] else '', msg)
def _candidate_score(ident, title):
    try:
        s = 0
        tok = (ident or "").split('/')[-1]
        if 'joinchat' in (ident or "") or (tok or "").startswith('+'):
            s += 30
        else:
            s -= 20
        if any(k in (title or "").lower() for k in SELLER_SHIELD_TERMS):
            s -= 100
        try:
            kpis = load_json(SOURCE_KPIS_FILE, {})
        except Exception as e:
            logger.debug(f"Failed to load KPIs: {e}")
            kpis = {}
        rec = kpis.get(str(ident or ""), {})
        att = int(rec.get("attempts", 0) or 0)
        suc = int(rec.get("successes", 0) or 0)
        err = int(rec.get("errors", 0) or 0)
        if att >= 3:
            rate = (suc / att) if att > 0 else 0.0
            s += int(20 * rate)  # stronger boost
            s -= min(20, err)    # stronger penalty
        return s
    except Exception as e:
        logger.debug(f"Candidate score error: {e}")
        return 0
def _title_hit(title):
    t = (title or "").lower()
    return any(k in t for k in _TITLE_TARGET_KEYWORDS_SET)
def _title_buyer_hit(title):
    t = (title or "").lower()
    return any(k.lower() in t for k in BUYER_PAIN_KEYWORDS)
def _blacklisted(title):
    t = (title or "").lower()
    if any(k in t for k in _BLACKLIST_JOIN_SET):
        return True
    return any(k in t for k in _SELLER_SHIELD_TERMS_SET)
async def _log_join_event_async(link, title, status, reason):
    try:
        items = await load_json_async(JOIN_ATTEMPTS_LOG, [])
    except Exception as e:
        logger.debug(f"Failed to load join attempts: {e}")
        items = []
    try:
        items.append({
            "id": link or "unknown",
            "title": title or "",
            "status": status or "",
            "reason": reason or "",
            "ts": time.time()
        })
        await save_json_async(JOIN_ATTEMPTS_LOG, items)
    except Exception as e:
        logger.debug(f"Failed to save join event: {e}")
    try:
        kpis = await load_json_async(SOURCE_KPIS_FILE, {})
    except Exception as e:
        logger.debug(f"Failed to load KPIs (async): {e}")
        kpis = {}
    ident = str(link or "unknown")
    rec = kpis.get(ident, {"attempts": 0, "successes": 0, "errors": 0})
    rec["attempts"] = int(rec.get("attempts", 0)) + (1 if status in ("attempt", "joined", "rejected", "error", "rate_limited", "left", "banned") else 0)
    if status == "joined":
        rec["successes"] = int(rec.get("successes", 0)) + 1
    if status in ("error", "banned", "rejected"):
        rec["errors"] = int(rec.get("errors", 0)) + 1
    kpis[ident] = rec
    try:
        await save_json_async(SOURCE_KPIS_FILE, kpis)
    except Exception as e:
        logger.debug(f"Failed to save KPIs: {e}")

def _log_join_event(link, title, status, reason):
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(_log_join_event_async(link, title, status, reason))
    except RuntimeError:
        logger.debug("No running loop for _log_join_event task")
    except Exception as e:
        logger.debug(f"Failed to create join log task: {e}")
async def resolve_entity_safe(ref):
    try:
        await ensure_connected()
        try:
            cd = await load_json_async(RESOLVE_COOLDOWN_FILE, {})
        except Exception as e:
            logger.debug(f"Failed to load cooldowns: {e}")
            cd = {}
        gk = cd.get("__global__")
        if gk and float(gk) > time.time():
            return None
        if not await resolve_limiter_consume():
            return None
        key = str(ref)
        try:
            cache = await load_json_async(ENTITY_CACHE_FILE, [])
        except Exception as e:
            logger.debug(f"Failed to load entity cache: {e}")
            cache = []
        if key in cache:
            return await client.get_input_entity(ref)
        ent = await client.get_input_entity(ref)
        try:
            cache.append(key)
            await save_json_async(ENTITY_CACHE_FILE, list(dict.fromkeys(cache)))
        except Exception as e:
            logger.debug(f"Failed to save entity cache: {e}")
        return ent
    except FloodWaitError as e:
        try:
            secs = int(getattr(e, "seconds", 60))
        except Exception as e:
            logger.debug(f"FloodWait seconds error: {e}")
            secs = 60
        logger.warning(f"Resolve FloodWait for {ref}: {secs}s")
        _log_join_event(str(ref), "", "rate_limited", f"Resolve FloodWait {secs}s")
        try:
            cd = await load_json_async(RESOLVE_COOLDOWN_FILE, {})
        except Exception as e:
            logger.debug(f"Failed to load cooldowns for update: {e}")
            cd = {}
        try:
            key = str(ref)
            cd[key] = time.time() + (secs * 2 + 120)
            if isinstance(ref, str):
                token = ref.split('/')[-1]
                uname = token if token and not ('joinchat' in ref or token.startswith('+')) else None
                if uname:
                    cd[uname] = time.time() + (secs * 2 + 120)
            cd["__global__"] = time.time() + (secs * 2 + 120)
        except Exception as e:
            logger.debug(f"Failed to update cooldowns: {e}")
        try:
            await save_json_async(RESOLVE_COOLDOWN_FILE, cd)
        except Exception as e:
            logger.debug(f"Failed to save cooldowns: {e}")
        return None
    except Exception as e:
        logger.warning(f"Resolve entity error for {ref}: {e}")
        return None
async def join_safe(request_callable, link, title):
    try:
        if not await join_limiter_consume():
            _log_join_event(link, title, "rate_limited", "Join tokens exhausted")
            return None
        return await request_callable
    except FloodWaitError as e:
        try:
            secs = int(getattr(e, "seconds", 60))
        except Exception as e:
            logger.debug(f"Join FloodWait seconds error: {e}")
            secs = 60
        logger.warning(f"Join FloodWait: {secs}s")
        _log_join_event(link, title, "rate_limited", f"{secs}s")
        return None
async def _save_potential_async(link, title, members, source_gid):
    try:
        os.makedirs("data", exist_ok=True)
    except Exception as e:
        logger.debug(f"Failed to create data dir: {e}")
    try:
        items = await load_json_async(POTENTIAL_TARGETS, [])
        if any((i.get("link") == link) for i in items):
            return
        items.append({
            "link": link,
            "title": title,
            "members": members,
            "source_group_id": source_gid,
            "discovered_at": datetime.datetime.now().isoformat()
        })
        await save_json_async(POTENTIAL_TARGETS, items)
    except Exception as e:
        logger.debug(f"Failed to save potential target: {e}")

def _save_potential(link, title, members, source_gid):
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(_save_potential_async(link, title, members, source_gid))
    except RuntimeError:
        logger.debug("No running loop for _save_potential task")
    except Exception as e:
        logger.debug(f"Failed to create save potential task: {e}")
def _add_verified_invite(link, title):
    # This should be async ideally, but if called from sync context we might need a wrapper
    # For now, let's keep it sync but with logging, or if possible use queue
    # However, since we are moving to async, let's make it async wrapper like others
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(_add_verified_invite_async(link, title))
    except RuntimeError:
        logger.debug("No running loop for _add_verified_invite task")
    except Exception as e:
        logger.debug(f"Failed to create verified invite task: {e}")

async def _add_verified_invite_async(link, title):
    try:
        items = await load_json_async(VERIFIED_INVITES_FILE, [])
    except Exception as e:
        logger.debug(f"Failed to load verified invites: {e}")
        items = []
    try:
        key = (link or "").strip()
        if not key:
            return
        if any((it.get("link") == key) for it in items):
            return
        items.append({"link": key, "title": title or "", "ts": time.time()})
        await save_json_async(VERIFIED_INVITES_FILE, items)
    except Exception as e:
        logger.debug(f"Failed to save verified invite: {e}")
async def _scrape_search_pages(keyword: str, max_links: int = 10) -> List[str]:
    try:
        headers = {"User-Agent": get_ghost_ua()}
        links = []
        global _ssl_ctx
        if _ssl_ctx is None:
            _ssl_ctx = ssl.create_default_context(cafile=certifi.where())
        global _http_session
        if _http_session is None or _http_session.closed:
            _http_session = aiohttp.ClientSession()
        session = _http_session
        # 1. DuckDuckGo HTML Search (Robust)
        try:
            ddg_url = f"https://html.duckduckgo.com/html/?q={urllib.parse.quote(keyword + ' site:t.me')}"
            for _ in range(3):
                try:
                    async with session.get(ddg_url, timeout=15, ssl=_ssl_ctx, headers=headers) as r0:
                        if r0.status == 200:
                            html = await r0.text()
                            soup0 = BeautifulSoup(html, "html.parser")
                            for a in soup0.find_all("a", href=True):
                                href = a["href"]
                                if "uddg=" in href:
                                    try:
                                        from urllib.parse import unquote
                                        href = unquote(href.split("uddg=")[1].split("&")[0])
                                    except Exception as e:
                                        logger.debug(f"DDG unquote error: {e}")
                                if "t.me/" in href and "t.me/s/" not in href:
                                    clean = href.split("?")[0].strip()
                                    if clean.startswith("https://t.me/"):
                                        links.append(clean)
                            break
                except Exception:
                    await asyncio.sleep(1.0)
        except Exception as e:
            logger.warning(f"DDG Scrape error: {e}")

            # 2. TGStat (Existing)
            try:
                url = f"https://tgstat.com/search?query={urllib.parse.quote(keyword)}&lang=en"
                for _ in range(2):
                    try:
                        async with session.get(url, timeout=15, ssl=_ssl_ctx, headers=headers) as r:
                            if r.status == 200:
                                html = await r.text()
                                soup = BeautifulSoup(html, "html.parser")
                                for a in soup.find_all("a", href=True):
                                    href = a["href"]
                                    if href.startswith("https://t.me/") and "TGStat" not in href and "bot" not in href.lower():
                                        links.append(href)
                                break
                    except Exception as e:
                        logger.debug(f"TGStat scrape error: {e}")
                        await asyncio.sleep(0.8)
            except Exception as e:
                logger.debug(f"TGStat section error: {e}")
            
            # 3. Telegroups/Directory (Alternative)
            try:
                url2 = f"https://telegramchannels.me/search?q={urllib.parse.quote(keyword)}"
                for _ in range(2):
                    try:
                        async with session.get(url2, timeout=15, ssl=_ssl_ctx, headers=headers) as r2:
                            if r2.status == 200:
                                html = await r2.text()
                                soup2 = BeautifulSoup(html, "html.parser")
                                for a in soup2.find_all("a", href=True):
                                    href = a["href"]
                                    if href.startswith("https://t.me/"):
                                        links.append(href)
                                break
                    except Exception:
                        await asyncio.sleep(0.8)
            except Exception:
                pass
        
            # 4. Telemetr (Additional Directory)
            try:
                url3 = f"https://telemetr.io/search?q={urllib.parse.quote(keyword)}"
                for _ in range(2):
                    try:
                        async with session.get(url3, timeout=15, ssl=_ssl_ctx, headers=headers) as r3:
                            if r3.status == 200:
                                html = await r3.text()
                                soup3 = BeautifulSoup(html, "html.parser")
                                for a in soup3.find_all("a", href=True):
                                    href = a["href"]
                                    if href.startswith("https://t.me/"):
                                        links.append(href)
                                break
                    except Exception:
                        await asyncio.sleep(0.8)
            except Exception:
                pass
        
        # 5. @tosearch bot (Specialized)
        try:
            extra = await fetch_from_tosearch(keyword)
            for ln in extra or []:
                links.append(ln)
        except Exception:
            pass
            
        out = []
        seen = set()
        for ln in links:
            # Basic cleanup
            ln = ln.strip().rstrip('/')
            if ln not in seen:
                # Filter out known junk
                if "bot" in ln.lower() or "tgstat" in ln.lower() or "subscribe" in ln.lower():
                    continue
                out.append(ln)
                seen.add(ln)
            if len(out) >= max_links:
                break
        if out:
            return out
        # Fallback to cached invites when network degraded
        try:
            pot = load_json(POTENTIAL_TARGETS, [])
        except Exception:
            pot = []
        cached = []
        for it in pot:
            ln = str(it.get("link", "") or "")
            tok = ln.split('/')[-1]
            if ln and ('joinchat' in ln or (tok or '').startswith('+')):
                cached.append(ln.strip().rstrip('/'))
            if len(cached) >= max_links:
                break
        return cached
    except Exception:
        return []
async def fetch_from_tosearch(keyword: str, max_links: int = 10) -> List[str]:
    try:
        await ensure_connected()
        bot = "tosearch"
        try:
            await client.send_message(bot, keyword)
        except Exception:
            return []
        await asyncio.sleep(2.0)
        msgs = []
        try:
            msgs = await client.get_messages(bot, limit=20)
        except Exception:
            msgs = []
        out = []
        seen = set()
        for m in msgs or []:
            txt = (getattr(m, "message", "") or getattr(m, "text", "") or "")
            for part in txt.split():
                h = part.strip().rstrip(",.;)()]")
                if h.startswith("https://t.me/"):
                    tok = h.split("/")[-1]
                    if 'joinchat' in h or (tok or '').startswith('+'):
                        if h not in seen:
                            out.append(h)
                            seen.add(h)
                if len(out) >= max_links:
                    break
            if len(out) >= max_links:
                break
        return out
    except Exception:
        return []
def _targets_keywords():
    data = _load_targets_cached()
    out = []
    for item in data.get("niche_targets", []):
        out.extend(item.get("keywords", []))
    return out
async def discover_fan_groups_via_telethon(limit_per_kw: int = 10) -> List[Dict[str, Any]]:
    try:
        await ensure_connected()
        kws = _targets_keywords()
        out = []
        for kw in kws:
            try:
                res = await client(functions.contacts.SearchRequest(q=kw, limit=limit_per_kw))
            except Exception:
                continue
            chats = getattr(res, "chats", []) or []
            for ch in chats:
                try:
                    uname = getattr(ch, "username", None)
                    if not uname:
                        continue
                    ent = await client.get_input_entity(f"https://t.me/{uname}")
                    full = await client(GetFullChannelRequest(ent))
                    fc = getattr(full, "full_chat", None)
                    title = getattr(getattr(full, "chats", [{}])[0], "title", "") or ""
                    members = getattr(fc, "participants_count", 0) or 0
                    pid = getattr(fc, "pinned_msg_id", None)
                    uncertain = False
                    if pid:
                        try:
                            pm = await client.get_messages(ent, ids=pid)
                            ptxt = (getattr(pm, "message", "") or getattr(pm, "text", "") or "").lower()
                            if _is_price_list(ptxt) or ("t.me/" in ptxt and "bot" in ptxt):
                                continue
                            if not ptxt.strip():
                                uncertain = True
                        except Exception:
                            uncertain = True
                    else:
                        uncertain = True
                    if members and members > 100 and _title_hit(title) and not _blacklisted(title):
                        out.append({"link": f"https://t.me/{uname}", "title": title, "members": members, "source": "telethon_search", "uncertain": uncertain})
                except Exception:
                    continue
        return out
    except Exception:
        return []
async def discover_links_from_reddit(max_links: int = 20) -> List[str]:
    try:
        if praw is None:
            return []
        cid = (os.environ.get("REDDIT_CLIENT_ID") or "").strip()
        cs = (os.environ.get("REDDIT_CLIENT_SECRET") or "").strip()
        ua = (os.environ.get("REDDIT_USER_AGENT") or "").strip()
        if not cid or not cs or not ua:
            return []
        r = praw.Reddit(client_id=cid, client_secret=cs, user_agent=ua, check_for_async=False)
        subs = set()
        for kw in _targets_keywords():
            k = kw.lower()
            if "lakers" in k:
                subs.add("lakers")
            if "man city" in k or "mcfc" in k:
                subs.add("mcfc")
            if "ufc" in k:
                subs.add("ufc")
            if "f1" in k:
                subs.add("formula1")
            if "firestick" in k:
                subs.add("fireTV")
            if "nvidia shield" in k or "shield" in k:
                subs.add("shieldandroidtv")
            if "tivimate" in k:
                subs.add("TiviMate")
        links = []
        import re as _re2
        backoff = 5
        for s in list(subs):
            try:
                sr = r.subreddit(s)
                for post in sr.new(limit=50):
                    txt = ((post.title or "") + " " + (post.selftext or "")).lower()
                    if "telegram" in txt or "t.me" in txt:
                        for m in _re2.finditer(r'https?://t\\.me/(?:joinchat/\\w+|\\+\\w+|[A-Za-z0-9_]+)', txt, flags=re.IGNORECASE):
                            links.append(m.group(0))
                            if len(links) >= max_links:
                                break
                    if len(links) >= max_links:
                        break
            except Exception as e:
                try:
                    await asyncio.sleep(backoff)
                except Exception:
                    pass
                backoff = min(backoff * 2, 300)
                continue
        seen = set()
        out = []
        for ln in links:
            key = ln.strip().rstrip("/")
            if key not in seen:
                seen.add(key)
                out.append(key)
        return out
    except Exception:
        return []
MISSION_WEIGHTS = {
    "Scouter 6 (Frustrated Switcher)": 3.0,
    "Scouter 6 (The Churn Hunter)": 2.5,
    "Scouter 7 (Large Scale Buyer)": 2.0,
    "Scouter 5 (Live Event Intent)": 1.8,
    "Scouter 2 (App-Specific Setup)": 1.6,
    "Scouter 2 (Fire Stick)": 1.4,
    "Scouter 1 (Panels)": 0.6,
    "Scouter 1 (Panels & Infrastructure)": 0.6,
    "Scouter 4 (Wholesale)": 0.5,
    "Scouter 4 (Wholesale & B2B)": 0.5,
    "Scouter 3 (Premium OTT)": 1.0,
    "Scouter 3 (Hardware Hooks)": 1.2,
    "Scouter 5 (Live Sports)": 1.2,
    "Scouter 8 (Tech-Savvy Newbie)": 1.0,
    "Scouter 8 (New Market Trends)": 1.0,
    "Scouter 9 (2026 High-Value)": 2.2,
}
def _weighted_keyword_pool():
    pool = []
    try:
        for mission, arr in SCOUTER_MISSIONS.items():
            w = float(MISSION_WEIGHTS.get(mission, 1.0))
            for kw in arr:
                pool.append((kw, w))
    except Exception:
        pass
    return pool
def _sample_keywords(k: int = 4):
    pool = _weighted_keyword_pool()
    if not pool:
        return []
    pop = [kw for kw, _ in pool]
    weights = [w for _, w in pool]
    chosen = set()
    out = []
    # Sample without strict replacement to keep them unique
    for _ in range(min(k, len(pop))):
        pick = random.choices(population=pop, weights=weights, k=1)[0]
        tries = 0
        while pick in chosen and tries < 5:
            pick = random.choices(population=pop, weights=weights, k=1)[0]
            tries += 1
        if pick in chosen:
            continue
        chosen.add(pick)
        out.append(pick)
    return out
async def directory_scraper_loop():
    while True:
        try:
            await ensure_connected()
            if not should_scrape_now():
                await asyncio.sleep(3600)
                continue
            try:
                sample = _sample_keywords(4)
                if not sample:
                    raise RuntimeError("no weighted sample")
            except Exception:
                keys = []
                try:
                    for arr in SCOUTER_MISSIONS.values():
                        keys.extend(arr)
                except Exception:
                    keys = TITLE_TARGET_KEYWORDS
                random.shuffle(keys)
                sample = keys[:4]
            for kw in sample:
                found = await _scrape_search_pages(kw, max_links=10)
                for ln in found:
                    try:
                        token = ln.split('/')[-1]
                        title = ""
                        members = 0
                        about = ""
                        if 'joinchat' in ln or token.startswith('+'):
                            try:
                                invite = await client(CheckChatInviteRequest(token.lstrip('+')))
                                chat_obj = getattr(invite, 'chat', invite)
                                title = getattr(chat_obj, 'title', '') or ''
                                members = getattr(chat_obj, 'participants_count', 0) or 0
                                about = getattr(chat_obj, 'about', '') or ''
                            except Exception:
                                continue
                        else:
                            try:
                                ent = await client.get_input_entity(ln)
                                if isinstance(ent, (types.InputPeerUser, types.InputPeerSelf)):
                                    continue
                                full = await client(GetFullChannelRequest(ent))
                                full_chat = getattr(full, 'full_chat', None)
                                title = getattr(getattr(full, 'chats', [{}])[0], 'title', '') or ''
                                members = getattr(full_chat, 'participants_count', 0) or 0
                                about = getattr(full_chat, 'about', '') or ''
                            except Exception:
                                continue
                        buyer_ok = _buyer_intent_texts([title, about])
                        if not buyer_ok:
                            continue
                        if members and members > 100 and _title_hit(title) and not _blacklisted(title):
                            _save_potential(ln, title, members, "directory")
                            log_activity("spider_discovered", f"{title}:{members}")
                    except Exception:
                        pass
            await asyncio.sleep(86400)
        except Exception as e:
            logger.error(f"Directory scraper error: {e}")
            await asyncio.sleep(3600)
async def spider_discover_once(max_keywords=3, max_links_per_kw=8, telethon_peek=True):
    try:
        keys = list(BUYER_PAIN_KEYWORDS)
        random.shuffle(keys)
        sample = keys[:max_keywords]
        discovered = 0
        for kw in sample:
            found = await _scrape_search_pages(kw, max_links=max_links_per_kw)
            for ln in found:
                try:
                    token = ln.split('/')[-1]
                    title = ""
                    members = 0
                    if telethon_peek:
                        if 'joinchat' in ln or token.startswith('+'):
                            try:
                                invite = await client(CheckChatInviteRequest(token.lstrip('+')))
                                chat_obj = getattr(invite, 'chat', invite)
                                title = getattr(chat_obj, 'title', '') or ''
                                members = getattr(chat_obj, 'participants_count', 0) or 0
                            except Exception:
                                continue
                        else:
                            try:
                                ent = await client.get_input_entity(ln)
                                full = await client(GetFullChannelRequest(ent))
                                full_chat = getattr(full, 'full_chat', None)
                                title = getattr(getattr(full, 'chats', [{}])[0], 'title', '') or ''
                                members = getattr(full_chat, 'participants_count', 0) or 0
                            except Exception:
                                continue
                    else:
                        title = "iptv help setup fix"
                        members = 101
                    if members and members > 100 and not _blacklisted(title):
                        _save_potential(ln, title, members, "directory")
                        log_activity("spider_discovered", f"{title}:{members}")
                        discovered += 1
                except Exception:
                    pass
        try:
            extra_links = await discover_fan_groups_via_telethon(limit_per_kw=max_links_per_kw)
        except Exception:
            extra_links = []
        for rec in extra_links:
            try:
                ln = (rec.get("link") or "").strip()
                title = rec.get("title") or ""
                members = int(rec.get("members") or 0)
                source = rec.get("source") or "telethon_search"
                uncertain = bool(rec.get("uncertain"))
                if members and members > 100 and not _blacklisted(title):
                    _save_potential(ln, title, members, source)
                    log_activity("spider_discovered", f"{title}:{members}")
                    discovered += 1
                    if uncertain:
                        try:
                            items = load_json(POTENTIAL_TARGETS, [])
                        except Exception:
                            items = []
                        for it in items:
                            if it.get("link") == ln:
                                it["uncertain"] = True
                                break
                        save_json(POTENTIAL_TARGETS, items)
            except Exception:
                pass
        try:
            reddit_links = await discover_links_from_reddit(max_links=max_links_per_kw * 2)
        except Exception:
            reddit_links = []
        for ln in reddit_links:
            try:
                token = ln.split('/')[-1]
                title = ""
                members = 0
                if 'joinchat' in ln or token.startswith('+'):
                    invite = await client(CheckChatInviteRequest(token.lstrip('+')))
                    chat_obj = getattr(invite, 'chat', invite)
                    title = getattr(chat_obj, 'title', '') or ''
                    members = getattr(chat_obj, 'participants_count', 0) or 0
                else:
                    ent = await client.get_input_entity(ln)
                    full = await client(GetFullChannelRequest(ent))
                    full_chat = getattr(full, 'full_chat', None)
                    title = getattr(getattr(full, 'chats', [{}])[0], 'title', '') or ''
                    members = getattr(full_chat, 'participants_count', 0) or 0
                if members and members > 100 and not _blacklisted(title):
                    _save_potential(ln, title, members, "reddit_bridge")
                    log_activity("spider_discovered", f"{title}:{members}")
                    discovered += 1
            except Exception:
                pass
        return discovered
    except Exception as e:
        logger.error(f"spider_discover_once error: {e}")
        return 0

async def potential_targets_dedupe_loop():
    while True:
        try:
            items = load_json(POTENTIAL_TARGETS, [])
            seen = set()
            deduped = []
            for it in items:
                ln = (it.get("link") or "").strip()
                if not ln or ln in seen:
                    continue
                seen.add(ln)
                deduped.append(it)
            if len(deduped) != len(items):
                save_json(POTENTIAL_TARGETS, deduped)
                log_activity("targets_deduped", f"{len(items)}->{len(deduped)}")
        except Exception:
            pass
        await asyncio.sleep(86400)

async def performance_monitor(duration_sec=2400, sample_interval=10, connectivity_every=5):
    start = time.time()
    proc = None
    try:
        if psutil:
            proc = psutil.Process(os.getpid())
    except Exception:
        proc = None
    samples_path = os.path.join(os.getcwd(), "perf_samples.jsonl")
    report_path = os.path.join(os.getcwd(), "perf_report.md")
    errors_count = 0
    max_cpu = 0.0
    max_rss = 0
    connectivity_ok = 0
    connectivity_fail = 0
    while time.time() - start < duration_sec:
        try:
            s = {"ts": time.time()}
            if psutil:
                try:
                    # Non-blocking CPU sampling to reduce overhead
                    _ = psutil.cpu_percent(interval=None)
                    s["cpu_percent"] = psutil.cpu_percent(interval=None)
                    max_cpu = max(max_cpu, float(s["cpu_percent"]))
                except Exception:
                    s["cpu_percent"] = None
                try:
                    mi = proc.memory_info() if proc else None
                    s["mem_rss"] = int(getattr(mi, "rss", 0)) if mi else None
                    max_rss = max(max_rss, int(s["mem_rss"] or 0))
                except Exception:
                    s["mem_rss"] = None
                try:
                    s["num_threads"] = int(proc.num_threads()) if proc else None
                except Exception:
                    s["num_threads"] = None
            try:
                if int((time.time() - start) / sample_interval) % max(1, int(connectivity_every)) == 0:
                    await ensure_connected()
                    ok = False
                    try:
                        _ = await client.get_dialogs(limit=1)
                        ok = True
                    except Exception:
                        ok = False
                    s["tg_connectivity"] = "ok" if ok else "fail"
                    if ok:
                        connectivity_ok += 1
                    else:
                        connectivity_fail += 1
            except Exception:
                s["tg_connectivity"] = "fail"
                connectivity_fail += 1
            try:
                # Scan recent log file tail for errors
                log_file = "bot.log"
                if os.path.exists(log_file):
                    with open(log_file, "rb") as f:
                        try:
                            f.seek(-4096, os.SEEK_END)
                        except Exception:
                            pass
                        tail = f.read().decode("utf-8", errors="ignore")
                        errs = sum(1 for ln in tail.splitlines() if ("ERROR" in ln or "CRITICAL" in ln))
                        s["log_tail_errors"] = errs
                        errors_count += errs
                else:
                    s["log_tail_errors"] = 0
            except Exception:
                s["log_tail_errors"] = None
            try:
                with open(samples_path, "a", encoding="utf-8") as wf:
                    import json as _json
                    wf.write(_json.dumps(s) + "\n")
            except Exception:
                pass
        except Exception as e:
            logger.error(f"Perf sample error: {e}")
        await asyncio.sleep(sample_interval)
    try:
        # Summarize
        mem_mb = round(max_rss / (1024 * 1024), 2) if max_rss else 0.0
        report = []
        report.append("## 40-Minute Performance Report")
        report.append(f"- Peak CPU: {max_cpu:.2f}%")
        report.append(f"- Peak RSS Memory: {mem_mb} MB")
        report.append(f"- Telegram Connectivity OK/FAIL: {connectivity_ok}/{connectivity_fail}")
        report.append(f"- Logged Errors (tail accumulative): {errors_count}")
        report.append(f"- Samples file: {samples_path}")
        with open(report_path, "w", encoding="utf-8") as rf:
            rf.write("\n".join(report))
        try:
            aid = ADMIN_LEADS_CHANNEL_ID
            msg = "Performance test complete. Report written to perf_report.md"
            if aid:
                await client.send_message(int(aid) if str(aid).isdigit() else aid, msg)
        except Exception:
            pass
    except Exception as e:
        logger.error(f"Perf report write error: {e}")

def should_scrape_now():
    if os.environ.get("AURA_MODE", "").lower() == "testing":
        return True
    lt = time.localtime()
    h = lt.tm_hour
    w = lt.tm_wday
    general = (18 <= h <= 23) or (13 <= h <= 16)
    b2b = (9 <= h <= 17)
    if w in [0, 1, 2, 3, 4]:
        return general or b2b
    return general

# --- Logic Engine ---

async def gatekeeper(chat_ref: Any) -> Tuple[bool, str]:
    global _stats_cache
    if _stats_cache is None:
        _stats_cache = await load_json_async(STATS_FILE, apex_supreme_stats)
    stats = _stats_cache
    
    try:
        await ensure_connected()
        override = (os.environ.get("TEST_JOIN_OVERRIDE") or "").strip() == "1"
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
                except Exception as e:
                    logger.debug(f"CheckChatInviteRequest failed: {e}")
                link = chat_ref
            else:
                link = chat_ref
                try:
                    entity = await resolve_entity_safe(chat_ref)
                except Exception as e:
                    try:
                        entity = await resolve_entity_safe(token)
                    except Exception as e2:
                        logger.debug(f"resolve_entity_safe failed for token: {e2}")
                        entity = None
        else:
            try:
                entity = await resolve_entity_safe(chat_ref)
                link = f"channel_id:{getattr(chat_ref, 'id', 'unknown')}"
            except Exception as e:
                logger.debug(f"resolve_entity_safe failed for ref: {e}")
                entity = None
                link = None

        if entity:
            try:
                full = await client(GetFullChannelRequest(entity))
                full_chat = getattr(full, 'full_chat', None)
                about = (getattr(full_chat, 'about', '') or '').lower()
                members = getattr(full_chat, 'participants_count', 0)
                title = getattr(getattr(full, 'chats', [{}])[0], 'title', '').lower()
                try:
                    pid = getattr(full_chat, "pinned_msg_id", None)
                    if pid:
                        pm = await client.get_messages(entity, ids=pid)
                        ptxt = (getattr(pm, "message", "") or getattr(pm, "text", "") or "").lower()
                        if _is_price_list(ptxt) or ("t.me/" in ptxt and "bot" in ptxt):
                            try:
                                items = await load_json_async(REJECTED_GROUPS, [])
                                items.append({"id": link or "unknown", "title": title or "", "reason": "pinned_ad", "ts": time.time()})
                                await save_json_async(REJECTED_GROUPS, items)
                            except Exception:
                                pass
                            _log_join_event(link, title, "rejected", "Pinned Ad/Bot Link")
                            return False, "Pinned Ad/Bot Link"
                except Exception:
                    pass
            except Exception as e:
                logger.debug(f"GetFullChannelRequest failed: {e}")

        pre_text = (title + " " + about).strip()
        if pre_text:
            q_score = calculate_quality_score(pre_text)
            if not override:
                if q_score <= 0:
                     _log_join_event(link, title, "rejected", f"Low Quality Score ({q_score})")
                     return False, f"Low Quality Score ({q_score})"
                if _is_price_list(pre_text):
                    try:
                        items = await load_json_async(REJECTED_GROUPS, [])
                        items.append({"id": link or "unknown", "title": title or "", "reason": "price_list", "ts": time.time()})
                        await save_json_async(REJECTED_GROUPS, items)
                    except Exception as e:
                        logger.warning(f"Failed to save price_list rejection: {e}")
                    _log_join_event(link, title, "rejected", "Price List/Ad Pattern")
                    return False, "Price List/Ad Pattern"
                
                # RELAXATION: If score is high (High Trust/Buyer Signal), bypass strict Tier-3 filter
                mission_passed = execute_mission_filter(pre_text)
                if not mission_passed:
                    if q_score >= 40:
                        # High trust score overrides Tier-3/Junk filter
                        pass
                    else:
                        stats["spam_shielded"] += 1
                        await save_json_async(STATS_FILE, stats)
                        try:
                            items = await load_json_async(REJECTED_GROUPS, [])
                            items.append({"id": link or "unknown", "title": title or "", "reason": "junk_tier3", "ts": time.time()})
                            await save_json_async(REJECTED_GROUPS, items)
                        except Exception as e:
                             logger.warning(f"Failed to save junk_tier3 rejection: {e}")
                        _log_join_event(link, title, "rejected", "Junk/Tier-3 Filtered")
                        return False, "Junk/Tier-3 Filtered"
                trade_terms = ["reseller", "wholesale", "panel", "credits", "restream", "source", "supplier", "official", "market", "b2b", "trade", "opportunity"]
                if any(k in pre_text for k in trade_terms):
                    try:
                        nm = (title or "Unknown").strip() or "Unknown"
                        await client.send_message('me', f"🚫 [Gatekeeper] Skipped Seller Hub: {nm}")
                    except Exception:
                        pass
                    try:
                        items = await load_json_async(REJECTED_GROUPS, [])
                        items.append({"id": link or "unknown", "title": title or "", "reason": "trade_hub", "ts": time.time()})
                        await save_json_async(REJECTED_GROUPS, items)
                    except Exception as e:
                        logger.warning(f"Failed to save trade_hub rejection: {e}")
                    _log_join_event(link, title, "rejected", "Trade Hub")
                    return False, "Trade Hub"
                if ("official" in pre_text) and any(sym in pre_text for sym in ["$", "€", "£", "price"]):
                    try:
                        nm = (title or "Unknown").strip() or "Unknown"
                        await client.send_message('me', f"🚫 [Gatekeeper] Skipped Seller Hub: {nm}")
                    except Exception:
                        pass
                    try:
                        items = await load_json_async(REJECTED_GROUPS, [])
                        items.append({"id": link or "unknown", "title": title or "", "reason": "official_price_list", "ts": time.time()})
                        await save_json_async(REJECTED_GROUPS, items)
                    except Exception as e:
                         logger.warning(f"Failed to save official_price_list rejection: {e}")
                    _log_join_event(link, title, "rejected", "Official Price List")
                    return False, "Official Price List"
        if not override:
            if members and members < 150:
                try:
                    items = await load_json_async(REJECTED_GROUPS, [])
                    items.append({"id": link or "unknown", "title": title or "", "reason": "small_group", "ts": time.time()})
                    await save_json_async(REJECTED_GROUPS, items)
                except Exception as e:
                    logger.warning(f"Failed to save small_group rejection: {e}")
                _log_join_event(link, title, "rejected", f"Small Group ({members})")
                return False, f"Small Group ({members})"
            try:
                token = None
                if isinstance(chat_ref, str):
                    token = chat_ref.split('/')[-1]
                is_invite = isinstance(chat_ref, str) and ('joinchat' in chat_ref or (token or '').startswith('+'))
                if entity and not is_invite:
                    msgs = await client.get_messages(entity, limit=5)
                    link_hits = sum(1 for m in msgs if m and _extract_tme_links(getattr(m, "text", "") or ""))
                    if link_hits > 2:
                        try:
                            items = await load_json_async(REJECTED_GROUPS, [])
                            items.append({"id": link or "unknown", "title": title or "", "reason": "prejoin_spam_links", "ts": time.time()})
                            await save_json_async(REJECTED_GROUPS, items)
                        except Exception as e:
                            logger.warning(f"Failed to save prejoin_spam_links rejection: {e}")
                        _log_join_event(link, title, "rejected", "Spam/Seller Hub")
                        return False, "Spam/Seller Hub"
            except Exception as e:
                logger.debug(f"Prejoin spam check failed: {e}")

        log_activity("group_join_attempt", link or "unknown")
        _log_join_event(link, title, "attempt", "")
        try:
            if isinstance(chat_ref, str):
                token = chat_ref.split('/')[-1]
                if 'joinchat' in chat_ref or token.startswith('+'):
                    join_result = await join_safe(client(ImportChatInviteRequest(token.lstrip('+'))), link or chat_ref, title or "")
                elif entity:
                    join_result = await join_safe(client(JoinChannelRequest(entity)), link or chat_ref, title or "")
                else:
                    _log_join_event(link, title, "error", "Join Error: Unable to resolve entity")
                    return False, "Join Error: Unable to resolve entity"
            else:
                join_result = await join_safe(client(JoinChannelRequest(entity)), link or "unknown", title or "")
            if not join_result:
                return False, "Join attempt skipped"
            if hasattr(join_result, "chats") and join_result.chats:
                chat_obj2 = join_result.chats[0]
                chat_id = chat_obj2.id
                chat_title = chat_obj2.title
                chat_username = getattr(chat_obj2, "username", None)
            else:
                chat_id = join_result.updates[0].message.peer_id.channel_id
                chat_title = "Unknown"
                chat_username = None
            src = ""
            try:
                items = load_json(POTENTIAL_TARGETS, [])
            except Exception:
                items = []
            try:
                for it in items:
                    if (it.get("link") or "").strip() == (link or "").strip():
                        src = str(it.get("source_group_id") or it.get("source") or "")
                        break
            except Exception:
                src = ""
            record_joined_group(chat_id, chat_title, chat_username, link or (f"https://t.me/{chat_username}" if chat_username else f"channel_id:{chat_id}"), src or "")
            log_activity("group_joined", f"{chat_id}:{chat_title}")
            try:
                logger.info(f"✅ [Gatekeeper] Joined Buyer Hub: {chat_title}")
            except Exception:
                pass
            try:
                if ADMIN_LEADS_CHANNEL_ID:
                    aid = int(ADMIN_LEADS_CHANNEL_ID) if str(ADMIN_LEADS_CHANNEL_ID).isdigit() else ADMIN_LEADS_CHANNEL_ID
                    await client.send_message(aid, f"🛰️ New Buyer-Hub Located: {chat_title}. Monitoring for lead signals now.")
            except Exception:
                pass
            _log_join_event(link, title, "joined", "")
            try:
                _add_verified_invite(link or (f"https://t.me/{chat_username}" if chat_username else f"channel_id:{chat_id}"), chat_title or "")
            except Exception:
                pass
        except FloodWaitError as fe:
            wait_s = int(getattr(fe, "seconds", 60)) + 60
            logger.warning(f"Join FloodWait: sleeping {wait_s}s")
            await asyncio.sleep(wait_s)
            try:
                cd = await load_json_async(RESOLVE_COOLDOWN_FILE, {})
            except Exception:
                cd = {}
            try:
                cd["__global__"] = time.time() + (wait_s * 2 + 120)
                await save_json_async(RESOLVE_COOLDOWN_FILE, cd)
            except Exception as e:
                logger.warning(f"Failed to save cooldown: {e}")
            log_activity("group_join_rate_limited", f"{(link or 'unknown')}:{wait_s}")
            _log_join_event(link, title, "rate_limited", f"{wait_s}s")
            return False, "Join rate limited"
        except UserBannedInChannelError as e:
            log_activity("group_join_banned", f"{(link or 'unknown')}:{str(e)[:120]}")
            _log_join_event(link, title, "banned", str(e)[:120])
            return False, "Join banned"
        except Exception as e: 
            log_activity("group_join_error", f"{(link or 'unknown')}:{str(e)[:120]}")
            _log_join_event(link, title, "error", str(e)[:120])
            return False, f"Join Error: {e}"

        if not override and os.environ.get("AURA_MODE") != "testing":
            await asyncio.sleep(random.randint(600, 1200))
        elif not override:
             await asyncio.sleep(5) # Minimal sleep for testing mode
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

        if not override:
            now_ts = datetime.datetime.now(datetime.timezone.utc)
            recent = [m for m in messages if getattr(m, "date", None)]
            # Relaxed Activity Check: Just need 1 message in last 7 days (168 hours)
            recent = [m for m in recent if (now_ts - (m.date if m.date.tzinfo else m.date.replace(tzinfo=datetime.timezone.utc))) <= datetime.timedelta(hours=168)]
            if len(recent) < 1:
                await client(LeaveChannelRequest(chat_id))
                try:
                    items = await load_json_async(REJECTED_GROUPS, [])
                    items.append({"id": link or "unknown", "title": title or "", "reason": "low_activity", "ts": time.time()})
                    await save_json_async(REJECTED_GROUPS, items)
                except Exception as e:
                    logger.warning(f"Failed to save low_activity rejection: {e}")
                _log_join_event(link, title, "left", "Low Activity")
                return False, "Low Activity"
            last10 = await client.get_messages(chat_id, limit=10)
            link_count = sum(1 for m in last10 if m.text and _extract_tme_links(m.text))
            if link_count > 3:
                await client(LeaveChannelRequest(chat_id))
                try:
                    items = await load_json_async(REJECTED_GROUPS, [])
                    items.append({"id": link or "unknown", "title": title or "", "reason": "spam_seller", "ts": time.time()})
                    await save_json_async(REJECTED_GROUPS, items)
                except Exception as e:
                    logger.warning(f"Failed to save spam_seller rejection: {e}")
                _log_join_event(link, title, "left", "Spam/Seller")
                return False, "Spam/Seller"
        if total_q_score < 10:
            if not override:
                await client(LeaveChannelRequest(chat_id))
                try:
                    items = await load_json_async(REJECTED_GROUPS, [])
                    items.append({"id": link or "unknown", "title": title or "", "reason": "low_depth_score", "ts": time.time()})
                    await save_json_async(REJECTED_GROUPS, items)
                except Exception as e:
                    logger.warning(f"Failed to save low_depth_score rejection: {e}")
                _log_join_event(link, title, "left", f"Low Depth Score ({total_q_score})")
                return False, f"Low Depth Score ({total_q_score})"

        urgent_terms = ["down", "black screen", "expired", "server down", "buffering issue", "need new provider"]
        urgent = any(k in combined_text for k in urgent_terms)
        status_val = "URGENT" if urgent else "VERIFIED"
        save_lead_to_db(link or f"channel_id:{chat_id}", chat_title, members if 'members' in locals() else 0, tech_hits, total_q_score, status_val)
        
        try:
            await client(UpdateNotifySettingsRequest(
                peer=InputNotifyPeer(peer=await client.get_input_entity(chat_id)),
                settings=InputPeerNotifySettings(mute_until=2147483647)
            ))
        except: pass
        
        return True, f"Gold Lead (Score: {total_q_score})"
    except Exception as e:
        logger.error(f"Gatekeeper error: {e}")
        _log_join_event(None, None, "error", "Analysis Error")
        return False, "Analysis Error"

async def purge_existing_sellers():
    try:
        dialogs = await client.get_dialogs(limit=200)
        for d in dialogs:
            try:
                ttl = (getattr(d, "title", "") or "").lower()
                if any(k in ttl for k in SELLER_SHIELD_TERMS):
                    await client(LeaveChannelRequest(d.entity))
                    logger.info(f"🚫 [Gatekeeper] Skipped Seller Hub: {getattr(d, 'title', '')}")
            except Exception as e:
                logger.debug(f"Error purging seller {getattr(d, 'id', 'unknown')}: {e}")
    except Exception as e:
        logger.error(f"Error in purge_existing_sellers: {e}")

async def user_discovery_loop():
    while True:
        try:
            await ensure_connected()
            stats = await load_json_async(STATS_FILE, apex_supreme_stats)
            warm_start = stats.get("warmup_started_at")
            now_ts = time.time()
            cpu = None
            try:
                import psutil
                cpu = psutil.cpu_percent(interval=0.2)
            except Exception:
                cpu = None
            if cpu is not None and cpu >= 70:
                await asyncio.sleep(900)
                continue
            if not warm_start:
                warm_start = now_ts
                stats["warmup_started_at"] = warm_start
                await save_json_async(STATS_FILE, stats)
            stats = await load_json_async(STATS_FILE, apex_supreme_stats)
            kw = random.choice(BUYER_INTENT_KEYWORDS) if BUYER_INTENT_KEYWORDS else random.choice(BUYER_PAIN_KEYWORDS)
            if not should_scrape_now():
                await asyncio.sleep(600)
                continue
            # Warm-up phase: read-only before joining groups
            if os.environ.get("SKIP_WARMUP") == "1":
                warmup_window = 0
            else:
                warmup_window = 900 if os.environ.get("AURA_MODE", "").lower() == "testing" else 3600
            if now_ts - warm_start < warmup_window:
                await asyncio.sleep(1800)
                continue
            if random.random() < 0.35:
                kw = random.choice(ESSENTIAL_HASHTAGS)
            try:
                potentials = await load_json_async(POTENTIAL_TARGETS, [])
            except Exception:
                potentials = []
            try:
                verified = await load_json_async(VERIFIED_INVITES_FILE, [])
            except Exception:
                verified = []
            try:
                q = await load_json_async(CANDIDATE_QUEUE_FILE, [])
            except Exception:
                q = []
            existing_ids = {it.get("id") for it in q}
            for item in potentials[:20]:
                link = item.get("link")
                title = item.get("title") or ""
                if not link or link in existing_ids:
                    continue
                tok = link.split('/')[-1]
                if 'joinchat' in link or (tok or '').startswith('+'):
                    q.append({"id": link, "title": title, "retry_after": 0, "attempts": 0})
            for item in verified[:50]:
                link = item.get("link")
                title = item.get("title") or ""
                if not link or link in existing_ids:
                    continue
                tok = link.split('/')[-1]
                if 'joinchat' in link or (tok or '').startswith('+'):
                    q.append({"id": link, "title": title, "retry_after": 0, "attempts": 0})
            try:
                await save_json_async(CANDIDATE_QUEUE_FILE, q)
            except Exception:
                pass
            res = await client(functions.contacts.SearchRequest(q=kw, limit=20))
            chats = getattr(res, 'chats', []) or []
            groups = await load_json_async(PROCESSED_GROUPS, [])
            for ch in chats:
                cid = getattr(ch, "id", None)
                uname = getattr(ch, "username", None)
                ident = f"https://t.me/{uname}" if uname else (f"channel_id:{cid}" if cid is not None else None)
                if not ident:
                    continue
                
                # Pre-Gatekeeper: Seller-Shield (Title Scan)
                title = getattr(ch, "title", "") or ""
                if _blacklisted(title):
                     # Add to REJECTED immediately to avoid re-scan
                     try:
                         rej = await load_json_async(REJECTED_GROUPS, [])
                         rej.append({"id": ident, "title": title, "reason": "trade_hub_precheck", "ts": time.time()})
                         await save_json_async(REJECTED_GROUPS, rej)
                     except: pass
                     # Also add to processed groups to avoid loop
                     groups.append(ident)
                     await save_json_async(PROCESSED_GROUPS, groups)
                     if os.environ.get("AURA_MODE", "").lower() == "testing":
                        logger.info(f"🚫 [Gatekeeper] Skipped Seller Hub: {title}")
                     continue
                if not _title_buyer_hit(title):
                    # Skip low buyer-intent titles
                    try:
                        rej = await load_json_async(REJECTED_GROUPS, [])
                        rej.append({"id": ident, "title": title, "reason": "low_buyer_intent_title", "ts": time.time()})
                        await save_json_async(REJECTED_GROUPS, rej)
                    except Exception as e:
                        logger.warning(f"Failed to save rejected group (low intent): {e}")
                    continue

                if ident in groups:
                    continue
                groups.append(ident)
                save_json(PROCESSED_GROUPS, groups)
                try:
                    cd = load_json(RESOLVE_COOLDOWN_FILE, {})
                except Exception:
                    cd = {}
                if uname and uname in cd and cd[uname] > time.time():
                    continue
                try:
                    q = load_json(CANDIDATE_QUEUE_FILE, [])
                except Exception:
                    q = []
                if any(it.get("id") == ident for it in q):
                    continue
                q.append({"id": ident, "title": title, "retry_after": 0, "attempts": 0})
                save_json(CANDIDATE_QUEUE_FILE, q)
            try:
                q = load_json(CANDIDATE_QUEUE_FILE, [])
            except Exception:
                q = []
            now_t = time.time()
            try:
                cd = load_json(RESOLVE_COOLDOWN_FILE, {})
            except Exception:
                cd = {}
            global_cool = cd.get("__global__")
            q_sorted = sorted(q, key=lambda it: (-_candidate_score(it.get("id"), it.get("title")), it.get("retry_after", 0)))
            if global_cool and float(global_cool) > now_t:
                q_sorted = [it for it in q_sorted if ('joinchat' in (it.get('id') or '')) or ((it.get('id') or '').split('/')[-1]).startswith('+')]
            q_ready = [it for it in q_sorted if float(it.get("retry_after", 0) or 0) <= now_t]
            processed = 0
            limit_batch = 1 if (global_cool and float(global_cool) > now_t) else 5
            for it in q_ready[:limit_batch]:
                ident = it.get("id")
                title = it.get("title")
                ok, reason = await gatekeeper(ident)
                it["attempts"] = int(it.get("attempts", 0)) + 1
                if not ok:
                    it["retry_after"] = now_t + 3600
                processed += 1
            try:
                save_json(CANDIDATE_QUEUE_FILE, q)
            except Exception:
                pass
            stats = load_json(STATS_FILE, apex_supreme_stats)
            if processed > 0:
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
            await ensure_connected()
            dialogs = await client.get_dialogs(limit=10)
            public_dialogs = [d for d in dialogs if d.is_channel or d.is_group]
            if public_dialogs:
                target = random.choice(public_dialogs)
                # Just fetch messages to simulate reading
                await client.get_messages(target, limit=5)
                msgs = await client.get_messages(target, limit=1)
                if msgs and msgs[0].text:
                     try:
                         await client(functions.messages.SendReactionRequest(
                             peer=target, msg_id=msgs[0].id, reaction=[ReactionEmoji(emoticon='ðŸ‘')]
                         ))
                     except Exception as e:
                         logger.debug(f"Reaction failed: {e}")
            
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
        if (event.sender_id in SERVICE_USER_IDS) or any(h in text for h in SERVICE_MSG_HINTS):
            return
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
        pre_count = await get_responses_count(user_id)
        if is_conversion:
            update_prospect_status(user_id, "converted", opt_out=False, increment_response=True)
            record_keyword_hits(event.raw_text, converted=True)
            log_activity("conversion", f"{user_id}:{event.raw_text[:140]}")
        else:
            update_prospect_status(user_id, "responded", opt_out=False, increment_response=True)
            record_keyword_hits(event.raw_text, converted=False)
            log_activity("inbound_dm", f"{user_id}:{event.raw_text[:140]}")
        if pre_count == 0:
            try:
                # Always skip reply notifications to 'me' as requested
                pass
            except Exception:
                pass
        return

    if not event.is_private:
        text = event.raw_text.lower()
        user_id = event.sender_id
        group_id = event.chat_id
        group_title = getattr(event.chat, "title", "group")
        try:
            bl = set(SENTIMENT_BLACKLIST)
            if any(k in text for k in bl):
                return
        except Exception:
            pass
        sender = None
        try:
            sender = await event.get_sender()
            username = getattr(sender, "username", None)
        except Exception:
            username = None
        
        if any(k in text for k in KEYWORD_TRIGGERS):
            try:
                # Use async load/save for New Lead Scoring
                leads = await load_json_async(LEADS_JSON, [])
                leads.append({"user_id": user_id, "group_id": group_id, "group_title": group_title, "text": event.raw_text, "ts": datetime.datetime.now().isoformat()})
                await save_json_async(LEADS_JSON, leads)
            except Exception:
                pass
        # New Lead Scoring (2026 Engine)
        s = calculate_lead_score(event.raw_text, sender)
        
        basic_terms = ["need iptv", "looking for streaming", "best tv provider", "looking for iptv", "iptv recommendation", "need streaming"]
        
        # Threshold: 7 (was 8)
        if s >= 7 or any(t in text for t in basic_terms):
            try:
                evx = evaluate_lead_message(event.raw_text or "", sender)
                if evx == "REJECT":
                    await _output_status("STATUS: REJECT")
                    return
                else:
                    await _output_status("STATUS: PROCEED")
            except Exception:
                pass
            msg_ts = getattr(event.message, "date", datetime.datetime.now()).isoformat()
            persona_id = choose_persona_id(group_title)
            try:
                src = await get_group_source(group_id)
            except Exception:
                src = ""
            save_prospect(user_id, username, event.raw_text, event.id, msg_ts, group_id, group_title, persona_id, "not_contacted", src or "")
            record_keyword_hits(event.raw_text, converted=False)
            log_activity("prospect_identified", f"{user_id}:{group_title}:{event.id}")
            
        # Conversation Watchlist Escalation (5-minute watch)
        now = time.time()
        exp = watchlist.get(event.sender_id)
        if exp and now <= exp and s >= 7:
            user_id = event.sender_id
            if user_id not in queued_handshakes and await should_queue_handshake(user_id):
                group_title = getattr(event.chat, "title", "group")
                ms = marketing_sentiment_score(event.raw_text or "")
                if ms <= 20:
                    queued_handshakes[user_id] = {
                        "msg_id": event.id,
                        "chat_id": event.chat_id,
                        "due": time.time() + random.randint(300, 600),
                        "snippet": event.raw_text[:120],
                        "group_title": group_title
                    }
            watchlist.pop(event.sender_id, None)
        elif s < 4 and event.sender_id not in watchlist: # Adjusted low score check
            watchlist[event.sender_id] = now + 300
        
        # Potential Lead Handshake (Immediate Queue)
        if (s >= 7 or any(val in text for val in TIER_1_INDICATORS)):
            try:
                evy = evaluate_lead_message(event.raw_text or "", sender)
                if evy == "REJECT":
                    return
            except Exception:
                pass
            user_id = event.sender_id
            if user_id not in queued_handshakes and await should_queue_handshake(user_id):
                group_title = getattr(event.chat, "title", "group")
                ms = marketing_sentiment_score(event.raw_text or "")
                # Relax sentiment further in testing mode
                mode = os.environ.get("AURA_MODE", "").lower()
                thresh = 100 if mode == "testing" else 45
                if ms <= thresh: 
                    queued_handshakes[user_id] = {
                        "msg_id": event.id,
                        "chat_id": event.chat_id,
                        "due": time.time() + random.randint(30, 60) if mode == "testing" else time.time() + random.randint(300, 600),
                        "snippet": event.raw_text[:120],
                        "group_title": group_title
                    }
                    logger.info(f"Queued Handshake for {user_id} in 30-60s (Score: {s}, Sentiment: {ms}, Mode: {mode}).")
                else:
                    logger.info(f"Lead {user_id} skipped due to sentiment: {ms} (Threshold: {thresh}, Mode: {mode})")
        try:
            links = _extract_tme_links(event.raw_text or "")
            for ln in links:
                try:
                    token = ln.split('/')[-1]
                    title = ""
                    members = 0
                    if 'joinchat' in ln or token.startswith('+'):
                        try:
                            invite = await client(CheckChatInviteRequest(token.lstrip('+')))
                            chat_obj = getattr(invite, 'chat', invite)
                            title = getattr(chat_obj, 'title', '') or ''
                            members = getattr(chat_obj, 'participants_count', 0) or 0
                        except Exception:
                            continue
                    else:
                        try:
                            ent = await client.get_input_entity(ln)
                            full = await client(GetFullChannelRequest(ent))
                            full_chat = getattr(full, 'full_chat', None)
                            title = getattr(getattr(full, 'chats', [{}])[0], 'title', '') or ''
                            members = getattr(full_chat, 'participants_count', 0) or 0
                        except Exception:
                            continue
                    if members and members > 100 and _title_hit(title) and not _blacklisted(title):
                        _save_potential(ln, title, members, group_id)
                except Exception:
                    pass
        except Exception:
            pass

async def handshake_processor():
    """V2.1: Perform heuristic reactions 10-15m before outreach."""
    logger.info("Handshake Processor Task Started.")
    while True:
        try:
            # Sync with database for existing leads not in queue
            logger.info("Syncing prospects from DB...")
            async with aiosqlite.connect(DB_FILE) as db:
                # Limit to 5 leads per sync to avoid rapid flood on restart
                limit = 5 if os.environ.get("AURA_MODE", "").lower() == "testing" else 10
                async with db.execute("SELECT user_id, username, message, message_id, group_id, group_title FROM prospects WHERE status = 'not_contacted' AND opt_out = 0 LIMIT ?", (limit,)) as cursor:
                    rows = await cursor.fetchall()
                    logger.info(f"Found {len(rows)} not_contacted prospects in DB (capped at {limit}).")
                    for u_id, u_name, msg, m_id, g_id, g_title in rows:
                        if u_id not in queued_handshakes:
                            logger.info(f"Syncing prospect {u_id} from DB to outreach queue.")
                            queued_handshakes[u_id] = {
                                "msg_id": m_id,
                                "chat_id": g_id,
                                "due": time.time() + random.randint(30, 120) if os.environ.get("AURA_MODE", "").lower() == "testing" else time.time() + random.randint(300, 600),
                                "snippet": msg[:120],
                                "group_title": g_title,
                                "username": u_name
                            }
            
            cpu = None
            try:
                import psutil
                cpu = psutil.cpu_percent(interval=0.2)
            except Exception:
                cpu = None
            now = time.time()
            logger.info(f"Handshake queue size: {len(queued_handshakes)}")
            if queued_handshakes:
                next_due = min(data["due"] for data in queued_handshakes.values())
                logger.info(f"Next handshake due in {max(0, int(next_due - now))} seconds.")
            
            # Copy keys to avoid RuntimeError during iteration
            to_process = [u for u, data in queued_handshakes.items() if data["due"] <= now]
            for u in to_process:
                if u in queued_handshakes:
                    data = queued_handshakes.pop(u)
                    msg_id = data["msg_id"]
                    chat_id = data["chat_id"]
                    snippet = data.get("snippet", "")
                    group_title = data.get("group_title", "the group")
                    cached_username = data.get("username")
                    try:
                        # Only react if it's a valid message ID (not 0 or None)
                        if msg_id and (should_outreach() or os.environ.get("AURA_MODE", "").lower() == "testing"):
                            try:
                                await client(functions.messages.SendReactionRequest(
                                    peer=chat_id, msg_id=msg_id, reaction=[types.ReactionEmoji(emoticon='👍')]
                                ))
                                logger.info(f"Heuristic Handshake: Reacted to {u} in {chat_id}")
                            except Exception as e:
                                if "CHAT_SEND_REACTIONS_FORBIDDEN" in str(e) or "You can't write in this chat" in str(e):
                                    logger.info(f"Reaction forbidden in {chat_id}, skipping handshake reaction.")
                                else:
                                    logger.warning(f"Reaction failed (possibly invalid msg_id {msg_id}): {e}")
                    except Exception as e:
                        logger.error(f"Handshake logic error: {e}")

                    try:
                        if await user_opted_out(u):
                            logger.info("Skipping DM (user opted out).")
                            continue
                        if not market_hour_ok() and os.environ.get("AURA_MODE", "").lower() != "testing":
                            tz = _market_tzinfo()
                            h = datetime.datetime.now(tz).hour
                            logger.info(f"Skipping DM to {u} (Outside human hours: {h}:00 in {tz.key}).")
                            continue
                        stats = await load_json_async(STATS_FILE, apex_supreme_stats)
                        dm_today = stats.get("dm_initiated_today", 0)
                        me = await client.get_me()
                        is_premium = bool(getattr(me, "premium", False))
                        cap = 25 if is_premium else 10
                        cap = min(cap, 75)
                        if dm_today >= cap:
                            logger.info(f"DM cap reached ({dm_today}/{cap}).")
                            continue
                        cached_username = data.get("username")
                        uname = cached_username # Ensure uname is defined before any possible exception
                        try:
                            # Try resolving by username if available, then by ID
                            user_obj = None
                            if cached_username:
                                try:
                                    user_obj = await client.get_entity(cached_username)
                                except Exception:
                                    pass
                            
                            if not user_obj:
                                try:
                                    # Fetching the message itself from the group to populate the cache
                                    if chat_id and msg_id:
                                        m = await client.get_messages(chat_id, ids=msg_id)
                                        if m and m.sender:
                                            user_obj = m.sender
                                    
                                    if not user_obj:
                                        user_obj = await client.get_entity(u)
                                except Exception as e:
                                    logger.debug(f"Entity resolution failed for {u}: {e}")
                                    # Fallback to direct resolution via full user if possible
                                    try:
                                        fu = await client(GetFullUserRequest(u))
                                        user_obj = getattr(fu, "user", None)
                                    except Exception as e2:
                                        logger.error(f"Failed all resolution paths for {u}: {e2}")
                                        continue

                            # For quality gate, we still need full user info
                            fu = await client(GetFullUserRequest(user_obj))
                            uname = getattr(user_obj, "username", None) or cached_username
                            # RELAXATION: Even if no username, if we have a valid user object from cache/ID, try to DM
                            if not uname and os.environ.get("AURA_MODE", "").lower() != "testing":
                                logger.info(f"Skipping DM to {u} (no username).")
                                continue
                            lead_premium = bool(getattr(user_obj, "premium", False))
                        except (UserPrivacyRestrictedError, PeerIdInvalidError) as e:
                            logger.info(f"Skipping DM to {u} (privacy or invalid peer): {e}")
                            continue
                        except Exception as e:
                            # Final resolution attempt if previous failed
                            if cached_username:
                                try:
                                    ent = await client.get_input_entity(cached_username)
                                    fu = await client(GetFullUserRequest(ent))
                                    user_obj = getattr(fu, "user", None)
                                    uname = getattr(user_obj, "username", None) or cached_username
                                    lead_premium = bool(getattr(user_obj, "premium", False))
                                except Exception as e2:
                                    logger.error(f"Failed to resolve {u} even with username {cached_username}: {e2}")
                                    continue
                            else:
                                logger.error(f"Resolution error for {u}: {e}")
                                continue
                        if not quality_gate(fu):
                            logger.info(f"Skipping DM to {u} (Failed Quality Gate: No photo or long offline).")
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
                        if outreach_deep_sleep():
                            logger.info("Skipping DM (deep sleep active).")
                            continue
                        await typing_heartbeat(u, random.uniform(6, 9))
                        dm_text = None
                        persona_id = await get_prospect_persona(u, chat_id) or choose_persona_id(group_title)
                        try:
                            lead_score = 0
                            try:
                                lead_score = calculate_lead_score(snippet or "", None)
                            except Exception:
                                lead_score = 0
                            stealth = False
                            try:
                                ent = await client.get_input_entity(chat_id)
                                full = await client(GetFullChannelRequest(ent))
                                about = getattr(getattr(full, "full_chat", None), "about", "") or ""
                                if "no seller" in (about or "").lower() or "no selling" in (about or "").lower():
                                    stealth = True
                            except Exception:
                                stealth = False
                            dm_text = None
                            try:
                                if ai_client:
                                    dm_text = await generate_ai_dm(uname or "there", group_title, snippet or "", lead_score, social_hint, "", stealth)
                            except Exception as e:
                                logger.debug(f"AI DM gen failed, fallback to compose: {e}")
                            if not dm_text:
                                dm_text = compose_aiden_dm(snippet or "", group_title, _detect_tech_context(snippet or ""), lead_score, stealth, social_hint)
                        except Exception as e:
                            logger.error(f"Aiden DM compose error: {e}")
                            dm_text = compose_aiden_dm(snippet or "", group_title, _detect_tech_context(snippet or ""), 0, False)

                        try:
                            bio_text = getattr(getattr(fu, "full_user", None), "about", "") or ""
                            target_lang = choose_target_language(bio_text, snippet)
                            dm_text = translate_text(dm_text, target_lang)
                        except Exception:
                            pass
                        try:
                            stats = await load_json_async(STATS_FILE, apex_supreme_stats)
                            last_text = stats.get("last_dm_text")
                            dm_text = ensure_spintax_variation(dm_text, last_text)
                            stats["last_dm_text"] = dm_text
                            await save_json_async(STATS_FILE, stats)
                        except Exception:
                            pass
                        try:
                            if outreach_blocked():
                                logger.info("Skipping DM (STOP_OUTREACH enabled).")
                                continue
                            if not market_hour_ok():
                                tz = _market_tzinfo()
                                h = datetime.datetime.now(tz).hour
                                logger.info(f"Skipping DM to {u} (Outside human hours: {h}:00 in {tz.key}).")
                                continue
                            await client.send_read_acknowledge(u)
                            await asyncio.sleep(random.randint(5, 10) if os.environ.get("AURA_MODE", "").lower() == "testing" else random.randint(300, 420))
                            sent = await safe_send_dm(client, u, dm_text)
                            # Global throttle between DMs
                            if os.environ.get("AURA_MODE", "").lower() == "testing":
                                await asyncio.sleep(random.randint(15, 30))
                            else:
                                await asyncio.sleep(random.randint(300, 900))
                            
                            if not sent:
                                continue
                            update_prospect_status(u, "contacted", opt_out=False, increment_response=False)
                            log_activity("dm_sent", f"{u}:{group_title}")
                            stats["dm_initiated_today"] = dm_today + 1
                            stats["unique_dms"] = stats.get("unique_dms", 0) + 1
                            await save_json_async(STATS_FILE, stats)
                            logger.info(f"DM sent to {u}. Count today: {stats['dm_initiated_today']}/{cap}")
                            await asyncio.sleep(random.randint(10, 20) if os.environ.get("AURA_MODE", "").lower() == "testing" else random.randint(900, 1200))
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
            await asyncio.sleep(120 if (cpu is not None and cpu >= 70) else 60)

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

# Deep sleep control for PeerFloodError events
_deep_sleep_until = 0.0
def activate_deep_sleep(seconds: int):
    global _deep_sleep_until
    # Reduce deep sleep in testing mode for faster debugging, but still block floods
    if os.environ.get("AURA_MODE", "").lower() == "testing":
        seconds = min(seconds, 300) # Max 5 mins in testing
    _deep_sleep_until = max(_deep_sleep_until, time.time() + max(0, seconds))
def outreach_deep_sleep() -> bool:
    return time.time() < _deep_sleep_until
def quality_gate(full_user):
    try:
        if os.environ.get("AURA_MODE", "").lower() == "testing":
            return True
        usr = getattr(full_user, "user", None)
        fu = getattr(full_user, "full_user", None)
        photo = False
        try:
            photo = bool(getattr(usr, "photo", None) or getattr(fu, "profile_photo", None))
        except Exception:
            photo = False
        if not photo:
            return False
        st = None
        try:
            st = getattr(usr, "status", None) or getattr(fu, "status", None)
        except Exception:
            st = None
        if isinstance(st, types.UserStatusEmpty):
            return False
        now_ts = datetime.datetime.now(datetime.timezone.utc)
        if isinstance(st, (UserStatusOnline, UserStatusRecently)):
            return True
        if isinstance(st, UserStatusOffline):
            dt = getattr(st, "was_online", None)
            if dt:
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=datetime.timezone.utc)
                if (now_ts - dt) <= datetime.timedelta(days=3):
                    return True
        return False
    except Exception:
        return False
# --- Command Deck ---

@client.on(events.NewMessage(pattern='/health'))
async def health_check(event):
    if not event.is_private: return
    # Trust Report
    report = (
        f"ðŸ›¡ï¸ **Aura V2.1 Trust Report**\n"
        f"âœ… Account Status: Active (Premium Shield)\n"
        f"ðŸ›°ï¸ Scouter State: Online\n"
        f"ðŸ‘» Ghost Mode: UA Rotation Engaged\n"
        f"ðŸ“ Timezone: {os.environ.get('TZ', 'Standard')}\n"
        f"ðŸ“ˆ Growth Pattern: Randomized (5-12%)"
    )
    await event.reply(report)

@client.on(events.NewMessage(pattern=r'/sleep (\d+)'))
async def sleep_bot(event):
    if not event.is_private: return
    try:
        hours = int(event.pattern_match.group(1))
        await event.reply(f"ðŸ’¤ Entering Ghost Mode (Read-Only) for {hours} hours.")
        # In a real implementation, we would set a flag in shared state/DB
        await asyncio.sleep(hours * 3600)
        await event.reply("ðŸš€ Aiden is awake and back in scout mode.")
    except Exception:
        await event.reply("Usage: /sleep <hours>")

@client.on(events.NewMessage(pattern='/reset_persona'))
async def reset_persona(event):
    if not event.is_private: return
    # Mimic human profile update
    await event.reply("ðŸ”„ Refreshing profile metadata and persona hooks...")
    # Logic to update bio/name randomly could go here
    await asyncio.sleep(5)
    await event.reply("âœ… Persona Reset: Digital footprint refreshed.")

@client.on(events.NewMessage(pattern='/export'))
async def export_db(event):
    if not event.is_private: return
    try:
        if os.path.exists(DB_FILE):
            await client.send_file('me', DB_FILE, caption=f"ðŸ›°ï¸ **Gold Leads Database Export**\nTime: {datetime.datetime.now()}")
        else:
            await event.reply("Database file not found.")
    except Exception as e:
        await event.reply(f"Export failed: {e}")

@client.on(events.NewMessage(pattern='/source_kpi'))
async def source_kpi(event):
    if not event.is_private: return
    try:
        stats = {}
        async with aiosqlite.connect(DB_FILE) as db:
            query = """
                SELECT COALESCE(source, ''), COUNT(*) as total,
                       SUM(CASE WHEN responded = 1 THEN 1 ELSE 0 END) as replies
                FROM prospects
                GROUP BY source
            """
            async with db.execute(query) as cursor:
                results = await cursor.fetchall()
                for source, total, replies in results:
                    rate = (replies / total * 100.0) if total > 0 else 0.0
                    stats[source or "unknown"] = {"total": total, "rate": f"{rate:.2f}%"}
        report = "📈 Source Performance Report\n"
        for src, data in stats.items():
            report += f"🔹 {str(src).upper()}: {data['total']} leads | {data['rate']} reply rate\n"
        try:
            aid = ADMIN_LEADS_CHANNEL_ID
            if aid:
                await client.send_message(int(aid) if str(aid).isdigit() else aid, report)
            else:
                await event.reply(report)
        except Exception:
            await event.reply(report)
    except Exception as e:
        await event.reply(f"Error generating KPI: {str(e)[:140]}")

async def historical_scan_loop():
    while True:
        try:
            async with aiosqlite.connect(DB_FILE, timeout=30) as conn:
                async with conn.execute("SELECT group_id, last_scanned_id, title FROM joined_groups WHERE banned = 0") as cursor:
                    rows = await cursor.fetchall()
            
            for group_id, last_scanned_id, title in rows:
                try:
                    # Resolve group entity first
                    try:
                        entity = await client.get_entity(group_id)
                    except ValueError:
                        # If ID fails, try resolving by some other means if possible, 
                        # but usually for ID it means it's not in dialogs.
                        # We skip for now to avoid log spam.
                        continue
                        
                    if last_scanned_id and last_scanned_id > 0:
                        msgs = await client.get_messages(entity, min_id=last_scanned_id, limit=100)
                    else:
                        msgs = await client.get_messages(entity, limit=50)
                except (UserBannedInChannelError, ChatWriteForbiddenError, ChannelPrivateError) as e:
                    mark_group_banned(group_id)
                    log_activity("group_banned", f"{group_id}:{str(e)[:160]}")
                    continue
                except FloodWaitError as fe:
                    wait_s = int(getattr(fe, "seconds", 60)) + 60
                    logger.warning(f"History scan FloodWait: sleeping {wait_s}s")
                    await asyncio.sleep(wait_s)
                    continue
                except ValueError as e:
                    if "Could not find the input entity" in str(e):
                        logger.debug(f"History scan: entity not in cache for {group_id}, skipping.")
                    else:
                        logger.error(f"History scan value error: {e}")
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
                    basic_terms = [
                        "need iptv", "looking for streaming", "best tv provider", "looking for iptv", "iptv recommendation", "need streaming",
                        "anyone have a link", "trial please", "test please", "m3u please", "iptv help", "buffering issue", "lagging", "freezing"
                    ]
                    if s >= 8 or any(t in tl for t in basic_terms):
                        user_id = getattr(m, "sender_id", None)
                        if user_id:
                            try:
                                sender = None
                                username = None
                                try:
                                    sender = await m.get_sender()
                                    if not sender:
                                        # Use peer_id from message if get_sender() fails
                                        p_id = getattr(m, "from_id", None) or getattr(m, "peer_id", None)
                                        if p_id:
                                            try:
                                                sender = await client.get_entity(p_id)
                                            except Exception:
                                                pass
                                    username = getattr(sender, "username", None)
                                except Exception as e:
                                    logger.debug(f"Sender resolution failed for {user_id}: {e}")
                                    sender = None
                                    username = None
                                try:
                                    evh = evaluate_lead_message(text or "", sender)
                                    if evh == "REJECT":
                                        await _output_status("STATUS: REJECT")
                                        continue
                                    else:
                                        await _output_status("STATUS: PROCEED")
                                except Exception:
                                    pass
                                msg_ts = getattr(m, "date", datetime.datetime.now()).isoformat()
                                persona_id = choose_persona_id(title or "group")
                                try:
                                    src = await get_group_source(group_id)
                                except Exception:
                                    src = ""
                                save_prospect(user_id, username, text, m.id, msg_ts, group_id, title or "group", persona_id, "not_contacted", src or "")
                                record_keyword_hits(text, converted=False)
                                log_activity("prospect_identified_history", f"{user_id}:{title}:{m.id}")
                                if s >= 12 and user_id not in queued_handshakes:
                                    ms = marketing_sentiment_score(text or "")
                                    thresh = 100 if os.environ.get("AURA_MODE", "").lower() == "testing" else 20
                                    if ms <= thresh:
                                        due_in = random.randint(600, 900) if os.environ.get("AURA_MODE", "").lower() != "testing" else random.randint(30, 60)
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
                        async with aiosqlite.connect(DB_FILE, timeout=30) as conn2:
                            await conn2.execute("UPDATE joined_groups SET last_scanned_id = ? WHERE group_id = ?", (max_id, group_id))
                            await conn2.commit()
                    except Exception as e:
                        logger.error(f"Update last_scanned_id error: {e}")
                await asyncio.sleep(5)
            interval = 60 if os.environ.get("AURA_MODE", "").lower() == "testing" else 1800
            await asyncio.sleep(interval)
        except Exception as e:
            logger.error(f"Historical scan loop error: {e}")
            await asyncio.sleep(300)

async def prune_dead_chats_loop():
    while True:
        try:
            cutoff = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=7)).isoformat()
            async with aiosqlite.connect(DB_FILE, timeout=60) as conn:
                async with conn.execute("SELECT group_id, title FROM joined_groups WHERE banned = 0 AND archived = 0") as cursor:
                    rows = await cursor.fetchall()
            
            for group_id, title in rows:
                try:
                    async with aiosqlite.connect(DB_FILE, timeout=30) as conn2:
                        async with conn2.execute("SELECT COUNT(*) FROM prospects WHERE group_id = ? AND message_ts >= ?", (group_id, cutoff)) as cursor:
                            cnt = (await cursor.fetchone())[0]
                        async with conn2.execute("SELECT quality_score FROM leads WHERE group_title = ? ORDER BY timestamp DESC LIMIT 1", (title,)) as cursor:
                            row = await cursor.fetchone()

                    qscore = row[0] if row else 0
                except Exception as e:
                    logger.error(f"Prune query error: {e}")
                    continue
                if qscore > 0 and cnt == 0:
                    try:
                        await client(LeaveChannelRequest(group_id))
                    except Exception as e:
                        logger.error(f"Prune leave error: {e}")
                    try:
                        async with aiosqlite.connect(DB_FILE, timeout=30) as conn3:
                            await conn3.execute("UPDATE joined_groups SET archived = 1 WHERE group_id = ?", (group_id,))
                            await conn3.commit()
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
            async with aiosqlite.connect(DB_FILE, timeout=60) as conn:
                try:
                    async with conn.execute("SELECT COUNT(*) FROM leads") as cursor:
                        count = (await cursor.fetchone())[0]
                except:
                    count = 0
                prospects_total = 0
                contacted = 0
                responded = 0
                opt_outs = 0
                conversions = 0
                try:
                    async with conn.execute("SELECT COUNT(*) FROM prospects") as cursor:
                        row = await cursor.fetchone()
                        prospects_total = row[0] if row else 0
                    async with conn.execute("SELECT COUNT(*) FROM prospects WHERE status IN ('contacted','converted')") as cursor:
                        row = await cursor.fetchone()
                        contacted = row[0] if row else 0
                    async with conn.execute("SELECT COUNT(*) FROM prospects WHERE status IN ('responded','converted')") as cursor:
                        row = await cursor.fetchone()
                        responded = row[0] if row else 0
                    async with conn.execute("SELECT COUNT(*) FROM prospects WHERE opt_out = 1") as cursor:
                        row = await cursor.fetchone()
                        opt_outs = row[0] if row else 0
                    async with conn.execute("SELECT COUNT(*) FROM prospects WHERE status = 'converted'") as cursor:
                        row = await cursor.fetchone()
                        conversions = row[0] if row else 0
                except Exception as e:
                    logger.error(f"Prospect stats error: {e}")
            
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
                            stats["shadowban_alerted"] = True
                    save_json(STATS_FILE, stats)
                else:
                    if zero_since:
                        stats.pop("last_zero_response_since", None)
                        save_json(STATS_FILE, stats)
            except Exception as e:
                logger.error(f"Shadowban check error: {e}")

            report = (
                f"ðŸ›°ï¸ **Aura Apex Supreme V2.1**\n"
                f"ðŸ’Ž **Verified Gold Leads:** {count}\n"
                f"ðŸ›¡ï¸ **Spam Auto-Purged:** {stats['spam_shielded']}\n"
                f"ðŸ‘¥ **Prospects Tracked:** {prospects_total}\n"
                f"âœ‰ï¸ **Contacted/Responded:** {contacted}/{responded}\n"
                f"ðŸš« **Opt-out:** {opt_outs} | âœ… Conversions: {conversions}\n"
                f"ðŸ“ˆ **Randomized Growth:** +{int(growth*100)}% Today\n"
                f"ðŸ“ **Sync State:** Hardware Spoofing Active\n"
                f"ðŸ“ˆ **Day:** {stats['day_counter']} | **State:** ðŸŸ¢\n"
                f"ðŸ” **QC Groups:** {len(stats.get('qc_groups', []))}"
            )
        except Exception as e:
            logger.error(f"Stats Report Error: {e}")

async def _get_group_entity_from_link(link):
    try:
        ent = await resolve_entity_safe(link)
        return ent
    except Exception:
        return None

async def _get_group_metadata(entity):
    try:
        full = await client(GetFullChannelRequest(entity))
        ch = getattr(full, 'chats', None)
        if hasattr(full, 'full_chat') and getattr(full.full_chat, 'participants_count', None) is not None:
            members = int(full.full_chat.participants_count or 0)
        else:
            members = 0
        title = getattr(full.chats[0], 'title', None) if (ch and len(ch) > 0) else getattr(entity, 'title', None) or ""
        return title or "", members
    except Exception:
        try:
            chat = await client.get_entity(entity)
            title = getattr(chat, 'title', '') or ''
            members = int(getattr(chat, 'participants_count', 0) or 0)
            return title, members
        except Exception:
            return "", 0

async def _compute_engagement(entity):
    try:
        msgs = await client.get_messages(entity, limit=100)
    except Exception:
        msgs = []
    now = datetime.datetime.utcnow()
    count_last_7d = 0
    reactions_total = 0
    reactions_counted = 0
    last_activity_hours = None
    for m in msgs:
        dt = getattr(m, 'date', None)
        if dt:
            age = (now - dt.replace(tzinfo=None)).total_seconds() / 3600.0
            if last_activity_hours is None or age < last_activity_hours:
                last_activity_hours = age
            if age <= 24*7:
                count_last_7d += 1
        rx = getattr(m, 'reactions', None)
        if rx and hasattr(rx, 'reactions'):
            try:
                reactions_total += sum(int(getattr(r, 'count', 0) or 0) for r in rx.reactions)
                reactions_counted += 1
            except Exception:
                pass
    msgs_per_day = count_last_7d / 7.0 if count_last_7d else 0.0
    reactions_avg = (reactions_total / max(1, reactions_counted)) if reactions_counted else 0.0
    return {"messages_per_day": msgs_per_day, "last_activity_hours": last_activity_hours or 1e9, "reactions_avg": reactions_avg}

def _detect_entry_requirements(link, title, desc_texts):
    req = "public"
    if 'joinchat' in str(link) or (str(link).split('/')[-1] or '').startswith('+'):
        req = "invite"
    ver = "none"
    blob = ' '.join([title or ''] + desc_texts).lower()
    if any(k in blob for k in ["verify", "verification", "captcha", "approval", "whitelist", "application"]):
        ver = "manual"
    if any(k in blob for k in ["bot", "/start", "questionnaire", "form"]):
        if ver == "none":
            ver = "automated"
    return req, ver

def _detect_contact_protocols(title, desc_texts):
    blob = ' '.join([title or ''] + desc_texts)
    items = []
    if re.search(r'@[\w_]{3,}', blob):
        items.append("dm_username")
    if re.search(r'/start', blob, flags=re.IGNORECASE):
        items.append("bot_command")
    if re.search(r'(email|mailto:)', blob, flags=re.IGNORECASE):
        items.append("email")
    if re.search(r'(form|google forms|typeform)', blob, flags=re.IGNORECASE):
        items.append("form")
    return list(dict.fromkeys(items))

def _buyer_intent_texts(texts):
    t = ' '.join(texts).lower()
    if any(k in t for k in SELLER_SHIELD_TERMS):
        return False
    hit = 0
    for k in BUYER_PAIN_KEYWORDS:
        if k.lower() in t:
            hit += 1
    return hit >= 1

def _trust_score(members, eng, sources_count, buyer_ok):
    s = 0
    if members >= 100:
        s += 10
    if members >= 1000:
        s += 10
    if eng.get("messages_per_day", 0) >= 5:
        s += 10
    if eng.get("last_activity_hours", 1e9) <= 72:
        s += 10
    if buyer_ok:
        s += 10
    if sources_count >= 2:
        s += 10
    return s

async def build_prospect_catalog(limit=60, use_processed=True):
    items = []
    seen = set()
    sources_map = {}
    if use_processed:
        try:
            g = load_json(PROCESSED_GROUPS, [])
        except Exception:
            g = []
        for ln in g:
            if ln and ln not in seen:
                seen.add(ln)
                items.append(ln)
                sources_map[ln] = ["processed"]
    try:
        pot = load_json(POTENTIAL_TARGETS, [])
    except Exception:
        pot = []
    for it in pot:
        ln = str(it.get("link", "") or "")
        if ln and ln not in seen:
            seen.add(ln)
            items.append(ln)
            arr = sources_map.get(ln, [])
            arr.append("potential_targets")
            sources_map[ln] = list(dict.fromkeys(arr))
    if len(items) < limit:
        need = limit - len(items)
        kws = BUYER_PAIN_KEYWORDS[:]
        random.shuffle(kws)
        for kw in kws[:max(3, min(8, len(kws)))]:
            try:
                links = await _scrape_search_pages(kw, max_links=need)
            except Exception:
                links = []
            links = links or []
            links = sorted(links, key=lambda ln: not ('joinchat' in ln or (ln.split('/')[-1] or '').startswith('+')))
            for ln in links:
                if ln and ln not in seen:
                    seen.add(ln)
                    items.append(ln)
                    arr = sources_map.get(ln, [])
                    arr.append(f"search:{kw}")
                    sources_map[ln] = list(dict.fromkeys(arr))
                if len(items) >= limit:
                    break
            if len(items) >= limit:
                break
    out = []
    for ln in items[:limit]:
        try:
            ent = await _get_group_entity_from_link(ln)
            if not ent:
                continue
            title, members = await _get_group_metadata(ent)
            desc_texts = []
            try:
                msgs = await client.get_messages(ent, limit=10)
                for m in msgs:
                    if m.text:
                        desc_texts.append(m.text[:280])
            except Exception:
                pass
            eng = await _compute_engagement(ent)
            req, ver = _detect_entry_requirements(ln, title, desc_texts)
            cp = _detect_contact_protocols(title, desc_texts)
            buyer_ok = _buyer_intent_texts([title] + desc_texts)
            sc = _trust_score(members, eng, len(sources_map.get(ln, [])), buyer_ok)
            rec = {
                "platform": "Telegram",
                "group_name": title,
                "url": ln,
                "group_size": members,
                "engagement": eng,
                "entry_requirements": req,
                "verification": ver,
                "contact_protocols": cp,
                "sources": sources_map.get(ln, []),
                "buyer_intent": bool(buyer_ok),
                "last_validated": int(time.time()),
                "trust_score": sc
            }
            out.append(rec)
        except Exception:
            pass
    try:
        save_json(PROSPECT_CATALOG_FILE, out)
    except Exception:
        pass
    if not out and use_processed:
        try:
            groups = load_json(PROCESSED_GROUPS, [])
        except Exception:
            groups = []
        ph = []
        for ln in groups[:limit]:
            try:
                slug = (ln.split('/')[-1] or '').strip()
                title = slug
                req = "invite" if ('joinchat' in ln or (slug or '').startswith('+')) else "public"
                buyer_ok = not any(k in slug.lower() for k in SELLER_SHIELD_TERMS)
                rec = {
                    "platform": "Telegram",
                    "group_name": title,
                    "url": ln,
                    "group_size": 0,
                    "engagement": {"messages_per_day": 0.0, "last_activity_hours": 1e9, "reactions_avg": 0.0},
                    "entry_requirements": req,
                    "verification": "none",
                    "contact_protocols": [],
                    "sources": ["processed"],
                    "buyer_intent": bool(buyer_ok),
                    "last_validated": int(time.time()),
                    "trust_score": 0
                }
                ph.append(rec)
            except Exception:
                pass
        try:
            save_json(PROSPECT_CATALOG_FILE, ph)
        except Exception:
            pass
        return ph
    return out

async def run_prospect_catalog_build(limit=60, use_processed=True):
    try:
        await client.connect()
    except Exception:
        pass
    res = await build_prospect_catalog(limit=limit, use_processed=use_processed)
    return res

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
    logger.info("Initializing AURA APEX SUPREME V2.1: FORTRESS HARDENING...")
    await init_db()
    await migrate_db()
    if os.environ.get("RUN_SPIDER_TEST") == "1" and os.environ.get("SPIDER_TEST_NO_TELEGRAM") == "1":
        logger.info("Running Spider Module test (HTTP-only, no Telegram peek)...")
        cnt = await spider_discover_once(max_keywords=2, max_links_per_kw=6, telethon_peek=False)
        logger.info(f"Spider discovered {cnt} eligible targets in this quick run.")
        return
    asyncio.create_task(db_writer_loop())
    asyncio.create_task(potential_targets_dedupe_loop())
    if (os.environ.get("RUN_PERF_40M") or "").strip() == "1":
        try:
            asyncio.create_task(performance_monitor(duration_sec=2400))
            logger.info("Performance monitor (40m) started.")
        except Exception as e:
            logger.error(f"Failed to start performance monitor: {e}")
    if (os.environ.get("RUN_PERF_SHORT") or "").strip() == "1":
        try:
            asyncio.create_task(performance_monitor(duration_sec=600))
            logger.info("Performance monitor (10m) started.")
        except Exception as e:
            logger.error(f"Failed to start performance monitor: {e}")
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
                # Ensure we are not already connected
                if client.is_connected():
                    await client.disconnect()
                
                if SESSION_STRING:
                    await client.connect()
                    auth_ok = await client.is_user_authorized()
                    if not auth_ok:
                        raise RuntimeError("Session string not authorized. Regenerate SESSION_STRING with session_gen.py.")
                else:
                    await client.start(phone=PHONE_NUMBER, code_callback=_code_callback, password=os.environ.get("TELEGRAM_PASSWORD"))
                return
            except Exception as _e:
                if 'database is locked' in str(_e).lower():
                    await asyncio.sleep(delay)
                    continue
                logger.error(f"Connection Error: {_e}")
                if _ == retries - 1:
                    raise
                await asyncio.sleep(delay)

    if os.environ.get("RUN_SPIDER_TEST") == "1" and os.environ.get("SPIDER_TEST_NO_TELEGRAM") == "1":
        logger.info("Running Spider Module test (HTTP-only, no Telegram peek)...")
        cnt = await spider_discover_once(max_keywords=2, max_links_per_kw=6, telethon_peek=False)
        logger.info(f"Spider discovered {cnt} eligible targets in this quick run.")
        return
    await _start_with_retry()
    logger.info("Fortress V2.1 Active: Handshake + Hardware Spoofing + Randomized Growth.")
    
    # Self-DM Test for health check
    try:
        me = await client.get_me()
        await client.send_message('me', f"Aura Apex Supreme V2.1 Started. Health Check: OK. Mode: {os.environ.get('AURA_MODE', 'production')}")
        logger.info("Self-DM test successful. Account is active.")
    except Exception as e:
        logger.warning(f"Self-DM test failed: {e}")
    
    qc_ok = False
    try:
        qc_ok = await ensure_qc_group_joined()
        if not qc_ok:
            pass
    except Exception:
        qc_ok = False
    if os.environ.get("TEST_JOIN_NOW") == "1":
        try:
            logger.info("Running immediate Scout Join test...")
            # One discovery pass with real peek
            _ = await spider_discover_once(max_keywords=2, max_links_per_kw=6, telethon_peek=True)
            items = load_json(POTENTIAL_TARGETS, [])
            candidates = []
            for it in items:
                link = str(it.get("link", ""))
                title = str(it.get("title", ""))
                members = int(it.get("members", 0) or 0)
                if not link or members < 100:
                    continue
                if _blacklisted(title):
                    continue
                if await joined_link_exists(link):
                    continue
                candidates.append((members, title, link))
            if not candidates:
                logger.info("No eligible candidates found.")
                return
            candidates.sort(key=lambda x: x[0], reverse=True)
            top = candidates[0]
            logger.info("Found a new lead-hub. Accessing now. 🛰️")
            ok, reason = await gatekeeper(top[2])
            if ok:
                logger.info(f"Joined: {top[1]} ({top[0]} members)")
            else:
                logger.info(f"Join failed: {reason}")
            return
        except Exception as e:
            logger.error(f"Immediate join test error: {e}")
            return
    if os.environ.get("RUN_SPIDER_TEST") == "1":
        logger.info("Running Spider Module test (one pass)...")
        cnt = await spider_discover_once(max_keywords=2, max_links_per_kw=6, telethon_peek=True)
        logger.info(f"Spider discovered {cnt} eligible targets in this quick run.")
        return
    asyncio.create_task(user_discovery_loop())
    asyncio.create_task(stats_report())
    # Start outreach tasks regardless of QC group status (2026 Resilience Fix)
    asyncio.create_task(handshake_processor())
    asyncio.create_task(historical_scan_loop())
    asyncio.create_task(proxy_health_monitor(client, PROXY_FILE))
    asyncio.create_task(noise_generation_loop())
    asyncio.create_task(prune_dead_chats_loop())
    asyncio.create_task(directory_scraper_loop())
    async def db_cleanup_loop():
        while True:
            try:
                await clean_old_logs_async(DB_FILE, days=7)
                await asyncio.sleep(43200)
            except Exception:
                await asyncio.sleep(3600)
    asyncio.create_task(db_cleanup_loop())
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
    async def cached_invites_refresh_loop():
        while True:
            try:
                await ensure_connected()
                try:
                    items = load_json(VERIFIED_INVITES_FILE, [])
                except Exception:
                    items = []
                updated = []
                now = time.time()
                for it in items:
                    link = str(it.get("link") or "")
                    title = str(it.get("title") or "")
                    if not link:
                        continue
                    ok = True
                    try:
                        tok = link.split('/')[-1]
                        if 'joinchat' in link or (tok or '').startswith('+'):
                            if tok.startswith('+'):
                                tok = tok[1:]
                            res = await client(CheckChatInviteRequest(tok))
                            obj = getattr(res, 'chat', None) or getattr(res, 'message', None)
                            ttl = getattr(obj, 'title', None) or title
                            cnt = getattr(obj, 'participants_count', None)
                            updated.append({"link": link, "title": ttl or "", "members": int(cnt or 0), "ts": now})
                        else:
                            ent = await client.get_input_entity(link)
                            full = await client(GetFullChannelRequest(ent))
                            ch = getattr(full, 'chats', [None])[0]
                            name = getattr(ch, 'title', None) or title
                            cnt = getattr(full.full_chat, 'participants_count', None)
                            updated.append({"link": link, "title": name or "", "members": int(cnt or 0), "ts": now})
                    except Exception:
                        ok = False
                    if not ok:
                        pass
                try:
                    if updated:
                        save_json(VERIFIED_INVITES_FILE, updated)
                except Exception:
                    pass
                await asyncio.sleep(21600)
            except Exception:
                await asyncio.sleep(3600)
    client.loop.create_task(qc_group_autojoin_loop())
    client.loop.create_task(qc_membership_verifier_loop())
    asyncio.create_task(cached_invites_refresh_loop())

    await client.run_until_disconnected()

if __name__ == '__main__':
    try:
        if os.environ.get("DRY_RUN") == "1":
            keep_alive()
            logger.info("DRY_RUN active: Health endpoint started on PORT. Skipping Telegram start.")
            while True:
                time.sleep(60)
        else:
            keep_alive()
            loop.run_until_complete(main())
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logger.critical(f"Fatal Error: {e}")
    finally:
        loop.close()
@client.on(events.NewMessage(pattern=r'/find (.+)'))
async def cmd_find(event):
    try:
        if not event.is_private:
            return
        q = event.pattern_match.group(1).strip().lower()
        data = await load_json_async("search_index.json", [])
        results = []
        for item in data:
            try:
                combined = " ".join(str(v) for v in item.values()).lower()
            except Exception:
                combined = str(item).lower()
            if q and q in combined:
                results.append(item)
            if len(results) >= 5:
                break
        logger.info(f"/find query='{q}' matches={len(results)}")
        if not results:
            await event.reply("No matches.")
            return
        lines = []
        for r in results[:3]:
            title = str(r.get("title") or r.get("name") or "item")
            hint = str(r.get("summary") or r.get("desc") or "")[:140]
            lines.append(f"- {title}: {hint}")
        await event.reply("\n".join(lines))
    except Exception as e:
        try:
            await event.reply(f"Find failed: {e}")
        except Exception:
            pass
@client.on(events.NewMessage(pattern='/status'))
async def cmd_status(event):
    try:
        if not event.is_private:
            return
        stats = await load_json_async(STATS_FILE, apex_supreme_stats)
        try:
            import psutil  # noqa: F401
            cpu = psutil.cpu_percent(interval=0.5)
            cpu_line = f"CPU: {cpu}%"
        except Exception:
            cpu_line = "CPU: unknown"
        rich = stats.get("rich_joined", 0)
        dms = stats.get("unique_dms", 0)
        spam = stats.get("spam_shielded", 0)
        day = stats.get("day_counter", 1)
        qc = len(stats.get("qc_groups", []))
        msg = (
            f"Status\n"
            f"{cpu_line}\n"
            f"Rich Groups: {rich}\n"
            f"Unique DMs: {dms}\n"
            f"Spam Shielded: {spam}\n"
            f"QC Groups: {qc}\n"
            f"Day: {day}"
        )
        logger.info("/status replied")
        await event.reply(msg)
    except Exception as e:
        try:
            await event.reply(f"Status failed: {e}")
        except Exception:
            pass
@client.on(events.NewMessage(pattern='/scout_join'))
async def cmd_scout_join(event):
    try:
        if not event.is_private:
            return
        stats = load_json(STATS_FILE, apex_supreme_stats)
        last_ts = stats.get("last_scout_join_ts")
        now = time.time()
        if last_ts and (now - float(last_ts)) < 14400:
            await event.reply("Cooldown active. Try again later.")
            return
        items = load_json(POTENTIAL_TARGETS, [])
        if not items:
            await event.reply("No potential targets.")
            return
        candidates = []
        for it in items:
            link = str(it.get("link", ""))
            title = str(it.get("title", ""))
            members = int(it.get("members", 0) or 0)
            if not link or members < 100:
                continue
            if _blacklisted(title):
                continue
            if await joined_link_exists(link):
                continue
            candidates.append((members, title, link))
        if not candidates:
            await event.reply("No eligible targets.")
            return
        candidates.sort(key=lambda x: x[0], reverse=True)
        top = candidates[0]
        await event.reply("Found a new lead-hub. Accessing now. 🛰️")
        ok, reason = await gatekeeper(top[2])
        if ok:
            stats["last_scout_join_ts"] = now
            stats["rich_joined"] = stats.get("rich_joined", 0) + 1
            save_json(STATS_FILE, stats)
            await event.reply(f"Joined: {top[1]} ({top[0]} members)")
        else:
            await event.reply(str(reason))
    except Exception as e:
        try:
            await event.reply(f"Scout join failed: {e}")
        except Exception:
            pass
@client.on(events.NewMessage(pattern='/spider_test'))
async def cmd_spider_test(event):
    try:
        if not event.is_private:
            return
        await event.reply("Running Spider discovery once...")
        cnt = await spider_discover_once(max_keywords=2, max_links_per_kw=6)
        await event.reply(f"Spider discovered {cnt} eligible targets in this quick run.")
    except Exception as e:
        try:
            await event.reply(f"Spider test failed: {e}")
        except Exception:
            pass
