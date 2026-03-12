from typing import List, Tuple, Dict, Optional, Any, Set
import asyncio
import os
import re
import time
import json
from datetime import datetime, timezone, timedelta
import io
import random
import logging
from urllib.parse import urljoin

import aiohttp
import ssl
import certifi
from bs4 import BeautifulSoup
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError, PeerFloodError, ChatWriteForbiddenError, ChannelPrivateError, UserAlreadyParticipantError
from telethon.tl.functions.messages import CheckChatInviteRequest, ImportChatInviteRequest
from groq import AsyncGroq
from PIL import Image, ImageDraw, ImageFont
from zoneinfo import ZoneInfo

from aura_core import setup_logging
from config import (
    API_ID, API_HASH, SESSION_STRING, GROQ_API_KEY, CURATOR_CHANNEL_ID, JUNK_KEYWORDS,
    REQUEST_TIMEOUT, CHECK_INTERVAL_SECONDS, BRAND_COLORS, PLATFORM_SPECS, PRO_TIPS,
    REBRAND_KEYWORDS, URGENCY_KEYWORDS, COMMERCIAL_KEYWORDS, COMPETITOR_KEYWORDS,
    BUYER_PAIN_KEYWORDS, SELLER_SHIELD_TERMS, GUIDE_KEYWORDS, FIX_KEYWORDS, NEWS_KEYWORDS
)
from config import rules as CONFIG_RULES

try:
    import sentry_sdk
    _SENTRY_DSN = (os.environ.get("SENTRY_DSN") or "").strip()
    if _SENTRY_DSN:
        sentry_sdk.init(dsn=_SENTRY_DSN, traces_sample_rate=float(os.environ.get("SENTRY_TRACES", "0.0") or 0.0))
except Exception:
    pass

setup_logging()
logger = logging.getLogger(__name__)

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
try:
    from fake_useragent import UserAgent
    _UA_GEN = UserAgent()
except Exception:
    _UA_GEN = None
def _build_headers() -> Dict[str, str]:
    try:
        ua = _UA_GEN.random if _UA_GEN else USER_AGENT
    except Exception:
        ua = USER_AGENT
    try:
        import random as _r
        lang = _r.choice(["en-US,en;q=0.9", "en-GB,en;q=0.9", "es-ES,es;q=0.9"])
    except Exception:
        lang = "en-US,en;q=0.9"
    return {
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": lang,
        "DNT": "1",
        "Upgrade-Insecure-Requests": "1"
    }
try:
    from curl_cffi import requests as curl_requests
except Exception:
    curl_requests = None
try:
    from selectolax.parser import HTMLParser as _SEL_HTML
except Exception:
    _SEL_HTML = None
try:
    BS4_PARSER = "html.parser"
except Exception:
    BS4_PARSER = "html.parser"
_URL_HTML_CACHE: Dict[str, Tuple[float, str]] = {}
_URL_HTML_TTL = float(os.environ.get("CURATOR_HTML_CACHE_TTL", "900") or 900)
_LLM_PAGE_CACHE: Dict[str, Tuple[float, List[Tuple[str, str, str]]]] = {}
_LLM_PAGE_TTL = float(os.environ.get("CURATOR_LLM_CACHE_TTL", "86400") or 86400)
_TME_SANITIZE_RE = re.compile(r'(https?://\S+|t\s?\.\s?me/\S+)', re.IGNORECASE)

# Pre-compiled Regex for Performance
_TOK_RE = re.compile(r"[a-z0-9]+")
_PROTIP_CLEAN_RE = re.compile(r'^[\s\*💡]*(?:Aiden[\'’]s\s+)?Pro-Tip:[\s\*💡]*', re.IGNORECASE)
_LINKS_RE = re.compile(r'https?://\S+')
_HTML_TAG_RE = re.compile(r'<[^>]+>')
_WHITESPACE_RE = re.compile(r'\s+')
_JSON_EXTRACT_RE = re.compile(r'\\[.*\\]', re.S)
_FILENAME_CLEAN_RE = re.compile(r'[^A-Za-z0-9_\-]+')
_NON_DIGIT_RE = re.compile(r"\D")
_STEP_LIST_RE = re.compile(r'^\d+[\.\)]\s+')
_STEP_WORD_RE = re.compile(r'\bstep\s*\d+\b', re.IGNORECASE)
_TOP_CLEAN_RE = re.compile(r'^\d+\s+(Best|Top|Greatest|Most)\s+', re.IGNORECASE)
_INVITE_HASH_RE = re.compile(r't\.me/\+([A-Za-z0-9_\-]+)')
_JOINCHAT_HASH_RE = re.compile(r'joinchat/([A-Za-z0-9_\-]+)')

_DEDUP_PATH = os.path.join("data", "curator_dedup.jsonl")
_DEDUP_CACHE: List[Dict[str, Any]] = []
_POSTED_LINKS_CACHE: Set[str] = set()
_POSTED_TITLES_HASH: Set[int] = set()
_CORE_TERMS = ["iptv", "tivimate", "smarters", "firestick", "m3u"]

import math

def _tok(title: str) -> Dict[str, int]:
    t = _TOK_RE.findall((title or "").lower())
    d: Dict[str, int] = {}
    for w in t:
        d[w] = d.get(w, 0) + 1
    return d

def _norm(v: Dict[str, int]) -> float:
    n2 = sum(x*x for x in v.values())
    return max(1e-9, math.sqrt(n2))

def _cos(a: Dict[str, int], norm_a: float, b: Dict[str, int], norm_b: float) -> float:
    if not a or not b:
        return 0.0
    keys = set(a.keys()) & set(b.keys())
    num = sum(a[k]*b[k] for k in keys)
    den = norm_a * norm_b
    return 0.0 if den == 0 else num/den

def _load_dedup():
    global _DEDUP_CACHE, _POSTED_LINKS_CACHE, _POSTED_TITLES_HASH
    if _DEDUP_CACHE:
        return
    try:
        if os.path.exists(_DEDUP_PATH):
            with open(_DEDUP_PATH, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rec = json.loads(line)
                        # Pre-calculate norm for cached vectors
                        if "vec" in rec and "norm" not in rec:
                            rec["norm"] = _norm(rec["vec"])
                        _DEDUP_CACHE.append(rec)
                        if "link" in rec:
                            _POSTED_LINKS_CACHE.add(rec["link"])
                        if "title" in rec:
                            _POSTED_TITLES_HASH.add(hash(rec["title"].lower().strip()))
                    except Exception:
                        continue
    except Exception:
        _DEDUP_CACHE = []
        _POSTED_LINKS_CACHE = set()
        _POSTED_TITLES_HASH = set()

def _save_dedup_sync(title: str, link: str = ""):
    try:
        os.makedirs("data", exist_ok=True)
        vec = _tok(title)
        norm = _norm(vec)
        rec = {"title": title, "vec": vec, "norm": norm, "link": link, "ts": int(time.time())}
        with open(_DEDUP_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        _DEDUP_CACHE.append(rec)
        if link:
            _POSTED_LINKS_CACHE.add(link)
        if title:
            _POSTED_TITLES_HASH.add(hash(title.lower().strip()))
    except Exception:
        pass

async def _save_dedup(title: str, link: str = ""):
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, _save_dedup_sync, title, link)

def _is_duplicate(title: str, link: str = "", th: float = 0.90) -> bool:
    _load_dedup()
    if link and link in _POSTED_LINKS_CACHE:
        return True
    
    title_clean = (title or "").lower().strip()
    if title_clean and hash(title_clean) in _POSTED_TITLES_HASH:
        return True
    
    vec_a = _tok(title)
    norm_a = _norm(vec_a)
    
    # Only check the last 1500 items to keep CPU usage low
    # Most duplicates appear within recent history
    for rec in reversed(_DEDUP_CACHE[-1500:]):
        try:
            vec_b = rec.get("vec") or {}
            norm_b = rec.get("norm") or _norm(vec_b)
            if _cos(vec_a, norm_a, vec_b, norm_b) >= th:
                return True
        except Exception:
            continue
    return False

SYSTEM_PROMPT = (
    "Act as Aiden, an expert in IPTV player optimization.\n"
    "When you share a link:\n"
    "- Use a casual opener like 'Quick one —' or 'Just saw this —'.\n"
    "- Keep it under 60 words in 1–3 short sentences.\n"
    "- Explain why the fix is better than just clearing cache.\n"
    "- Include one concrete 'Pro Tip' beyond the summary.\n"
    "- Reference the source name.\n"
    "- End with a low-pressure note. No links inside the body."
)

# Load additional env vars not exported by config
IMAGE_GEN_ENDPOINT = os.getenv("IMAGE_GEN_ENDPOINT")
IMAGE_GEN_API_KEY = os.getenv("IMAGE_GEN_API_KEY")
IMAGE_GEN_MODEL = os.getenv("IMAGE_GEN_MODEL", "sdxl")

# Initialize AI Client
try:
    if GROQ_API_KEY:
        ai_client = AsyncGroq(api_key=GROQ_API_KEY)
    else:
        ai_client = None
except Exception as e:
    logger.warning(f"Groq Init Failed: {e}")
    ai_client = None

_AI_MAX_PER_MIN = int(os.environ.get("CURATOR_AI_MAX_PER_MIN", "8"))
_ai_calls_ts: List[float] = []
def _ai_allow_now() -> bool:
    now = time.time()
    cutoff = now - 60.0
    # prune
    while _ai_calls_ts and _ai_calls_ts[0] < cutoff:
        _ai_calls_ts.pop(0)
    if len(_ai_calls_ts) < _AI_MAX_PER_MIN:
        _ai_calls_ts.append(now)
        return True
    return False

_SCRAPE_CONCURRENCY = max(1, int(os.environ.get("CURATOR_CONCURRENCY", "3")))
_scrape_sem = asyncio.Semaphore(_SCRAPE_CONCURRENCY)
_SOURCE_CB: Dict[str, Dict[str, Any]] = {}
_SOURCE_METRICS: Dict[str, Dict[str, Any]] = {}
def _source_allowed(name: str) -> bool:
    st = _SOURCE_CB.get(name) or {}
    until = float(st.get("reopen_at", 0.0) or 0.0)
    return time.time() >= until
def _record_source_result(name: str, ok: bool, threshold: int = 3, backoff_sec: int = 1800):
    st = _SOURCE_CB.get(name) or {"fail": 0, "reopen_at": 0.0}
    if ok:
        st["fail"] = 0
        st["reopen_at"] = 0.0
    else:
        st["fail"] = int(st.get("fail", 0)) + 1
        if st["fail"] >= threshold:
            st["reopen_at"] = time.time() + backoff_sec
    _SOURCE_CB[name] = st
    m = _SOURCE_METRICS.get(name) or {"attempts": 0, "success": 0, "fail": 0}
    m["attempts"] = int(m.get("attempts", 0)) + 1
    if ok:
        m["success"] = int(m.get("success", 0)) + 1
    else:
        m["fail"] = int(m.get("fail", 0)) + 1
    _SOURCE_METRICS[name] = m

def _curator_tzinfo():
    """Returns ZoneInfo based on CURATOR_TZ or CURATOR_MARKET (e.g., 'US', 'UK')."""
    try:
        # 1. Explicit TZ takes precedence
        tz_env = (os.environ.get("CURATOR_TZ") or "").strip()
        if tz_env:
            return ZoneInfo(tz_env)
            
        # 2. Market-based lookup (matching Aura Apex Supreme logic)
        market = (os.environ.get("CURATOR_MARKET") or "").strip().lower()
        if market in ("us", "usa", "en-us", "ny", "new_york"):
            return ZoneInfo("America/New_York")
        if market in ("uk", "gb", "en-uk", "london"):
            return ZoneInfo("Europe/London")
        if market in ("eu", "europe", "berlin"):
            return ZoneInfo("Europe/Berlin")
            
        return ZoneInfo("UTC")
    except Exception:
        return ZoneInfo("UTC")

async def _run_source(name: str, coro_func):
    if not _source_allowed(name):
        return []
    try:
        t0 = time.time()
        async with _scrape_sem:
            res = await coro_func()
        dt = (time.time() - t0) * 1000.0
        ok = bool(res)
        _record_source_result(name, ok)
        try:
            m = _SOURCE_METRICS.get(name) or {"attempts": 0, "success": 0, "fail": 0, "lat_ms_sum": 0.0, "lat_samples": 0}
            m["lat_ms_sum"] = float(m.get("lat_ms_sum", 0.0)) + dt
            m["lat_samples"] = int(m.get("lat_samples", 0)) + 1
            _SOURCE_METRICS[name] = m
        except Exception:
            pass
        return res
    except Exception:
        _record_source_result(name, False)
        return []

def validate_curator_env():
    errs = []
    if not API_ID or not str(API_ID).isdigit() or int(API_ID) <= 0:
        errs.append("Invalid API_ID")
    if not API_HASH or not re.fullmatch(r'[0-9a-fA-F]{32}', str(API_HASH)):
        errs.append("Invalid API_HASH")
    sess = (SESSION_STRING or os.environ.get("SESSION_STRING") or "").strip()
    if not sess or len(sess) < 50:
        errs.append("Missing or invalid SESSION_STRING")
    try:
        int(CURATOR_CHANNEL_ID or "0")
    except Exception:
        pass
    if errs:
        for e in errs:
            logger.critical(e)
        raise SystemExit(1)

STRICT_IPTV_KEYWORDS = ["iptv", "firestick", "tivimate", "smarters", "streaming", "buffering", "dns", "rebrand"]
IPTV_FILTER_KEYWORDS = list(set(STRICT_IPTV_KEYWORDS + (REBRAND_KEYWORDS or []) + (URGENCY_KEYWORDS or []) + (COMMERCIAL_KEYWORDS or []) + (GUIDE_KEYWORDS or []) + (FIX_KEYWORDS or []) + (NEWS_KEYWORDS or [])))

COMPETITOR_TERMS = COMPETITOR_KEYWORDS or ["top 10 providers", "best sellers", "best providers", "top sellers", "alternative providers", "apollo", "xtream", "stb", "stbemu", "apollo group tv", "apollo group"]
THREAD_MAP = {"#AuraNews": 9, "#AuraGuide": 4, "#AuraFix": 2, "#AuraUpdate": 3}
DYNAMIC_THREAD_MAP = {}
TOPIC_MAP = {
    "guide": {"hashtag": "#AuraGuide", "thread": 4},
    "fix": {"hashtag": "#AuraFix", "thread": 2},
    "news": {"hashtag": "#AuraNews", "thread": 9},
    "update": {"hashtag": "#AuraUpdate", "thread": 3}
}

POST_IMAGE_CACHE = {}
IMAGE_GEN_ENDPOINT = os.environ.get("IMAGE_GEN_ENDPOINT", "").strip()
IMAGE_GEN_API_KEY = os.environ.get("IMAGE_GEN_API_KEY", "").strip()
IMAGE_GEN_MODEL = os.environ.get("IMAGE_GEN_MODEL", "sdxl").strip()
 

def classify_topic(title: str, description: str = "") -> Dict[str, Any]:
    s = ((title or "") + " " + (description or "")).lower()
    
    # Priority for Update/Recap (AuraUpdate)
    if any(k in s for k in ["weekly recap", "performance recap", "changelog", "new version"]):
        m = TOPIC_MAP["update"]
        return {"topic": "update", "hashtag": m["hashtag"], "thread": m["thread"]}
        
    if any(k in s for k in (GUIDE_KEYWORDS or [])):
        m = TOPIC_MAP["guide"]
        return {"topic": "guide", "hashtag": m["hashtag"], "thread": m["thread"]}
    if any(k in s for k in (FIX_KEYWORDS or [])):
        m = TOPIC_MAP["fix"]
        return {"topic": "fix", "hashtag": m["hashtag"], "thread": m["thread"]}
    if any(k in s for k in (NEWS_KEYWORDS or [])):
        m = TOPIC_MAP["news"]
        return {"topic": "news", "hashtag": m["hashtag"], "thread": m["thread"]}
    h = assign_group_hashtag(title)
    th = THREAD_MAP.get(h) or THREAD_MAP.get("#AuraNews")
    return {"topic": "news", "hashtag": h, "thread": th}

def is_high_value_topic(title: str) -> bool:
    t = (title or "").lower()
    hv = [
        "tivimate vs smarters pro",
        "isp throttling in uk/usa",
        "optimizing iptv buffer size for 4k playback",
        "setup guide: m3u vs stalker portal"
    ]
    return any(p in t for p in hv)

async def get_pro_tip_for_topic(topic: str) -> str:
    """
    Generates a contextual 'Pro Tip' using LLM.
    """
    import random
    
    # Fallback tips if AI is unavailable or fails
    fallbacks = {
        "guide": [
            "Large buffer size reduces jitter on 4K streams.",
            "Favor Software decoder when hardware acceleration stutters.",
            "Keep EPG refresh at 24h for smoother navigation."
        ],
        "fix": [
            "Switch VPN to TCP on match days for stable handshakes.",
            "Pin DNS to 1.1.1.1 or 8.8.8.8 to stop portal flaps.",
            "Reduce reconnect loops by lowering retry aggressiveness."
        ],
        "news": [
            "Stick with Aura Apex for secure, consistent streaming.",
            "Own hardware beats resellers when services flap.",
            "Hardened APKs avoid third‑party scanner noise."
        ]
    }
    
    default_tip = f"***💡 Aiden’s Pro-Tip:*** {random.choice(fallbacks.get(topic, fallbacks['news']))}"

    if not ai_client or (os.environ.get("AURA_AI_SAVINGS", "0") == "1" and random.random() > 0.3):
        return default_tip

    try:
        if not _ai_allow_now():
             await asyncio.sleep(1.0)

        sys_prompt = (
            "You are Aiden, an elite IPTV engineer. Write ONE short, advanced pro-tip "
            "related to streaming stability, security, or setup. "
            "Style: Insider knowledge, technical, concise. "
            "Formatting: Use **bold** and _italics_ sparingly. "
            "The label 'Aiden's Pro-Tip:' MUST be both bold and italic: ***Aiden’s Pro-Tip:***. "
            "Length: Max 20 words."
        )
        
        user_prompt = f"Topic: {topic}. Give me a technical tip."

        resp = await ai_client.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[
                {"role": "system", "content": sys_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.8,
            max_tokens=60
        )
        
        tip = (resp.choices[0].message.content or "").strip()
        
        # Strip any existing labels/emojis/formatting to normalize
        # This removes variations of "Aiden's Pro-Tip", emojis, and asterisks from the start
        clean_tip = _PROTIP_CLEAN_RE.sub('', tip).strip()
        
        # Re-apply the canonical label
        tip = f"***💡 Aiden’s Pro-Tip:*** {clean_tip}"
            
        return _TME_SANITIZE_RE.sub('', tip)

    except Exception as e:
        logger.error(f"Pro-Tip AI Error: {e}")
        return default_tip

def is_iptv_content(title: str, description: str = "") -> bool:
    combined = ((title or "") + " " + (description or "")).lower()
    return any(k in combined for k in _CORE_TERMS)

def strict_iptv_allowed(title: str, description: str = "") -> bool:
    s = ((title or "") + " " + (description or "")).lower()
    return any(k.lower() in s for k in STRICT_IPTV_KEYWORDS)

def competitor_banned(title: str, description: str = "") -> bool:
    s = ((title or "") + " " + (description or "")).lower()
    return any(k.lower() in s for k in COMPETITOR_TERMS)
def safe_image_allowed(text: str) -> bool:
    s = (text or "").lower()
    try:
        for w in (JUNK_KEYWORDS or []):
            if (w or "").lower() in s:
                return False
    except Exception as e:
        logger.debug(f"Error checking junk keywords: {e}")
    bad = ["adult", "nsfw", "violent", "gore", "hate", "racist", "sex"]
    return not any(b in s for b in bad)
async def _build_dynamic_tags_async(title: str, body: str) -> List[str]:
    """
    Generates dynamic hashtags based on content using AI if available,
    falling back to keyword extraction.
    """
    if ai_client and _ai_allow_now() and (os.environ.get("AURA_AI_SAVINGS", "0") != "1" or random.random() < 0.3):
        try:
            sys_prompt = (
                "Extract 3-5 high-value, technical hashtags for an IPTV Telegram post. "
                "Include the primary topic (News, Guide, or Fix). "
                "Output ONLY the hashtags separated by spaces. Example: #IPTV #Firestick #DNSFix"
            )
            user_prompt = f"Title: {title}\nBody: {body[:500]}"
            resp = await ai_client.chat.completions.create(
                model="llama-3.1-8b-instant",
                messages=[
                    {"role": "system", "content": sys_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.5,
                max_tokens=50
            )
            tags_text = (resp.choices[0].message.content or "").strip()
            # Clean up
            tags = [t if t.startswith("#") else f"#{t}" for t in tags_text.split() if len(t) > 1]
            return tags[:5]
        except Exception as e:
            logger.debug(f"Dynamic Tag AI Error: {e}")

    # Fallback to local keyword extraction
    t = (title + " " + body).lower()
    extra = []
    keywords = {
        "firestick": "#Firestick",
        "tivimate": "#TiviMate",
        "dns": "#DNS",
        "buffer": "#NoBuffering",
        "rebrand": "#WhiteLabelIPTV",
        "white label": "#WhiteLabelIPTV",
        "stream": "#Streaming",
        "handshake": "#HandshakeFix",
        "packet loss": "#NetworkOptimization",
        "isp": "#ISPThrottling",
        "vpn": "#VPNSafety",
        "android": "#AndroidTV",
        "nvidia": "#NvidiaShield",
        "smarters": "#IPTVPro"
    }
    for k, v in keywords.items():
        if k in t:
            extra.append(v)
    
    # Unique and limited
    out = []
    for tag in extra:
        if tag not in out:
            out.append(tag)
        if len(out) >= 4:
            break
    return out
def generate_4k_image(title: str, concept_text: str = "") -> Optional[bytes]:
    try:
        topic = "guide" if any(k in (concept_text or title or "").lower() for k in GUIDE_KEYWORDS) else "fix" if any(k in (concept_text or title or "").lower() for k in FIX_KEYWORDS) else "news"
        t0 = time.time()
        main, platforms, metrics = generate_post_images(title, concept_text or "", topic)
        elapsed = time.time() - t0
        metrics["elapsed_sec"] = round(elapsed, 3)
        if elapsed > 5.0 and not main:
            main = _template_default_image(title, topic)
            metrics["fallback_timed"] = True
        return main
    except Exception as e:
        logger.error(f"Image gen failed: {e}")
        return None

async def _build_image_prompt(title: str, body: str, topic: str) -> str:
    try:
        # Strict high-fidelity style
        base_style = "cinematic lighting, 8k resolution, photorealistic, octane render, unreal engine 5, highly detailed, sharp focus"
        
        if not ai_client:
            return f"A visually stunning concept art representing '{title}', {base_style}, no text"

        sys_prompt = (
            "You are an expert prompt engineer for Stable Diffusion XL. "
            "Create a HIGHLY SPECIFIC, LITERAL visual description of the subject matter. "
            "Do NOT use generic 'tech' backgrounds unless the topic is abstract. "
            "If the topic is about a Firestick, describe a Firestick. "
            "If it's about a specific app, describe its logo style or interface abstractly (but NO TEXT). "
            "If it's about a server raid, describe police lights and server racks. "
            "Focus on: ACTION, OBJECTS, SPECIFIC DETAILS. "
            "Forbidden: Text, letters, words, UI overlays, generic 'cyberpunk' filler. "
            "Style: Cinematic, photorealistic, dramatic. "
            "Keep it under 70 words."
        )
        
        user_prompt = f"Title: {title}\nContext: {body[:250]}\nTask: Write a precise visual art prompt that depicts exactly what this news is about."
        
        try:
            resp = await ai_client.chat.completions.create(
                model="llama-3.1-8b-instant",
                messages=[{"role": "system", "content": sys_prompt}, {"role": "user", "content": user_prompt}],
                temperature=0.7,
                max_tokens=100
            )
            txt = (resp.choices[0].message.content or "").strip()
            
            # Post-process to ensure style keywords are present
            if "8k" not in txt.lower():
                txt += f", {base_style}"
            
            return txt
        except Exception:
            return f"Concept art of {title}, {base_style}"
    except Exception:
        return f"Concept art of {title}, {base_style}"

async def _ai_generate_image(prompt: str, size: Tuple[int, int] = (1280, 720)) -> Optional[bytes]:
    try:
        if not IMAGE_GEN_ENDPOINT or not IMAGE_GEN_API_KEY:
            return None
        
        # Determine provider based on endpoint or env var
        is_stability = "stability.ai" in IMAGE_GEN_ENDPOINT or os.environ.get("IMAGE_GEN_PROVIDER") == "stability"
        
        if is_stability:
            # Stability AI Format (SDXL)
            width = 1344
            height = 768
            payload = {
                "text_prompts": [{"text": prompt}],
                "cfg_scale": 7,
                "height": height,
                "width": width,
                "samples": 1,
                "steps": 30,
            }
        else:
            payload = {"prompt": prompt, "model": IMAGE_GEN_MODEL, "width": size[0], "height": size[1]}
            
        headers = {
            "Content-Type": "application/json", 
            "Accept": "image/png" if is_stability else "application/json",
            "Authorization": f"Bearer {IMAGE_GEN_API_KEY}"
        }
            
        ssl_ctx = ssl.create_default_context(cafile=certifi.where())
        async with aiohttp.ClientSession(headers=headers) as session:
            for _ in range(2):
                try:
                    async with session.post(IMAGE_GEN_ENDPOINT, json=payload, timeout=60, ssl=ssl_ctx) as r:
                        if r.status == 200:
                            ct = r.headers.get("Content-Type", "")
                            if "image" in ct:
                                raw = await r.read()
                                return raw if raw else None
                            elif "application/json" in ct:
                                data = await r.json()
                                if "artifacts" in data:
                                    b64 = data["artifacts"][0].get("base64")
                                else:
                                    b64 = (data.get("image_base64") or "").strip()
                                
                                if not b64:
                                    return None
                                import base64
                                return base64.b64decode(b64)
                        elif r.status == 429 or r.status == 402:
                            # 429 = Rate Limit, 402 = Payment Required (Insufficient Balance)
                            logger.warning(f"Stability AI {r.status}: Insufficient balance or rate limit. Skipping AI image.")
                            return None
                        else:
                            try:
                                err_text = await r.text()
                                logger.error(f"Stability AI Error {r.status}: {err_text}")
                            except:
                                pass
                except Exception as e:
                    logger.error(f"Image Gen Request Error: {e}")
                    await asyncio.sleep(1.0)
        return None
    except Exception:
        return None

async def generate_curator_image_async(title: str, body: str, topic: str) -> Optional[bytes]:
    try:
        # AI Optimization: Images are expensive (Stability Credits)
        # Only generate if CURATOR_GEN_IMAGES is set to '1'
        if os.environ.get("CURATOR_GEN_IMAGES", "0") != "1":
            return await generate_4k_image_async(title, body)

        if not safe_image_allowed(title + " " + body):
            return None
        prompt = await _build_image_prompt(title, body, topic)
        ai_bytes = await _ai_generate_image(prompt, (1280, 720))
        if ai_bytes:
            try:
                img = Image.open(io.BytesIO(ai_bytes))
                img = img.convert("RGB")
                img = img.resize((1280, 720), Image.LANCZOS)
                out = io.BytesIO()
                img.save(out, format="JPEG", quality=90)
                out.seek(0)
                return out.read()
            except Exception:
                pass
        return await generate_4k_image_async(title, body)
    except Exception:
        return await generate_4k_image_async(title, body)

def extract_links(text: str) -> List[str]:
    if not text:
        return []
    return _LINKS_RE.findall(text)

_session: Optional[aiohttp.ClientSession] = None

async def get_session() -> aiohttp.ClientSession:
    global _session
    if _session is None or _session.closed:
        ssl_context = ssl.create_default_context(cafile=certifi.where())
        try:
            _lim = max(1, int(os.environ.get("CURATOR_CONCURRENCY", "3"))) * 2
        except Exception:
            _lim = 6
        _session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=ssl_context, limit=_lim))
    return _session

async def get_html(url: str) -> str:
    try:
        attempts = 2
        delay = 0.6
        for i in range(attempts):
            try:
                session = await get_session()
                async with session.get(url, timeout=REQUEST_TIMEOUT, headers=_build_headers()) as r:
                    if r.status == 200:
                        return await r.text()
            except Exception:
                pass
            if i < attempts - 1:
                import random as _r
                jitter = max(0.1, _r.gauss(delay, delay*0.25))
                await asyncio.sleep(jitter)
                delay = min(8.0, delay * 1.8)
        if curl_requests:
            try:
                resp = curl_requests.get(url, impersonate="chrome120", timeout=REQUEST_TIMEOUT)
                if getattr(resp, "status_code", 0) == 200:
                    return resp.text
            except Exception:
                pass
        if (os.environ.get("CURATOR_BROWSER_FALLBACK","").strip().lower() in ("1","true","yes")):
            try:
                content = await get_html_browser(url)
                if content:
                    return content
            except Exception:
                pass
        return ""
    except Exception as e:
        logger.error(f"Request failed for {url}: {e}")
        if curl_requests:
            try:
                resp = curl_requests.get(url, impersonate="chrome120", timeout=REQUEST_TIMEOUT)
                if getattr(resp, "status_code", 0) == 200:
                    return resp.text
            except Exception:
                pass
        if (os.environ.get("CURATOR_BROWSER_FALLBACK","").strip().lower() in ("1","true","yes")):
            try:
                content = await get_html_browser(url)
                if content:
                    return content
            except Exception:
                pass
        return ""

async def get_html_browser(url: str) -> str:
    try:
        from playwright.async_api import async_playwright
    except Exception:
        return ""
    try:
        ua = (_UA_GEN.random if _UA_GEN else USER_AGENT)
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True, args=["--disable-blink-features=AutomationControlled"])
            context = await browser.new_context(user_agent=ua, viewport={"width":1280,"height":800})
            page = await context.new_page()
            await page.goto(url, wait_until="domcontentloaded", timeout=15000)
            content = await page.content()
            await context.close()
            await browser.close()
            return content or ""
    except Exception:
        return ""

async def ai_semantic_extract_links(html: str, source_name: str) -> List[Tuple[str, str, str]]:
    try:
        if not ai_client or not html:
            return []
        if not _ai_allow_now():
            await asyncio.sleep(1.0)
        txt = _HTML_TAG_RE.sub(' ', html or '')
        txt = _WHITESPACE_RE.sub(' ', txt).strip()
        if not txt:
            return []
        sys_p = "You are a headless data extractor. Your input is raw HTML or text. Your output must be a strict JSON array of objects with keys: title (string), url (string). No explanations or code fences."
        user_p = "Extract up to 10 IPTV-relevant articles with absolute URLs. Only include items where both title and url exist. If none exist, return an empty JSON array. Content: " + txt[:8000]
        resp = await ai_client.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[{"role": "system", "content": sys_p}, {"role": "user", "content": user_p}],
            temperature=0.2,
            max_tokens=400
        )
        raw = (resp.choices[0].message.content or "").strip()
        data = None
        try:
            data = json.loads(raw)
        except Exception:
            m = _JSON_EXTRACT_RE.search(raw)
            if m:
                try:
                    data = json.loads(m.group(0))
                except Exception:
                    data = None
        items = []
        if isinstance(data, list):
            for it in data:
                t = (it or {}).get("title") or ""
                u = (it or {}).get("url") or ""
                if t and u and isinstance(t, str) and isinstance(u, str) and u.startswith("http") and is_iptv_content(t):
                    items.append((t.strip(), u.strip(), source_name))
        return items
    except Exception:
        return []

async def generate_4k_image_async(title: str, concept_text: str = "") -> Optional[bytes]:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, generate_4k_image, title, concept_text)


async def scrape_troypoint() -> List[Tuple[str, str, str]]:
    try:
        html = await get_html("https://troypoint.com/category/tutorials/")
        if not html:
            logger.error("Aura Radar: Source blocked (TroyPoint). Shifting to secondary nodes.")
            return []
        soup = BeautifulSoup(html, BS4_PARSER)
        items = []
        if _SEL_HTML:
            try:
                dom = _SEL_HTML(html)
                main = dom.css_first("main") or dom.body
                if main:
                    for tag in ["h1","h2","h3"]:
                        for h in main.css(tag):
                            a = h.css_first("a")
                            if a and a.attributes.get("href"):
                                title = (a.text() or "").strip()
                                link = a.attributes.get("href","").strip()
                                if link and is_iptv_content(title):
                                    items.append((title, link, "TROYPOINT"))
            except Exception:
                pass
        try:
            main = soup.find(attrs={"role": "main"}) or soup.find("main")
            if main:
                for h in main.find_all(["h1", "h2", "h3"]):
                    a = h.find("a", href=True)
                    if a:
                        title = (a.get_text() or "").strip()
                        link = (a.get("href") or "").strip()
                        if link and is_iptv_content(title):
                            items.append((title, link, "TROYPOINT"))
        except Exception:
            pass
        articles = soup.select("article") or soup.select("div.post") or soup.select("div.entry") or soup.select("div.type-post") or soup.select("div.blog-post")
        for art in articles:
            a = art.select_one("h2 a") or art.select_one("h3 a")
            if not a:
                a = art.find("a", href=True)
            if not a:
                continue
            title = (a.get_text() or "").strip()
            link = (a.get("href") or "").strip()
            if link and is_iptv_content(title):
                items.append((title, link, "TROYPOINT"))
        if not items:
            items.extend(await ai_semantic_extract_links(html, "TROYPOINT"))
        return items
    except Exception:
        logger.error("Aura Radar: Source blocked (TroyPoint). Shifting to secondary nodes.")
        return []

async def scrape_iptvwire() -> List[Tuple[str, str, str]]:
    try:
        html = await get_html("https://iptvwire.com/category/news/")
        if not html:
            html = await get_html("https://iptvwire.com/")
        if not html:
            logger.error("Aura Radar: Source blocked (IPTVWire). Shifting to secondary nodes.")
            return []
        soup = BeautifulSoup(html, BS4_PARSER)
        items = []
        if _SEL_HTML:
            try:
                dom = _SEL_HTML(html)
                main = dom.css_first("main") or dom.body
                if main:
                    for tag in ["h1","h2","h3"]:
                        for h in main.css(tag):
                            a = h.css_first("a")
                            if a and a.attributes.get("href"):
                                title = (a.text() or "").strip()
                                link = a.attributes.get("href","").strip()
                                if link and is_iptv_content(title):
                                    items.append((title, link, "IPTVWire"))
            except Exception:
                pass
        try:
            main = soup.find(attrs={"role": "main"}) or soup.find("main")
            if main:
                for h in main.find_all(["h1", "h2", "h3"]):
                    a = h.find("a", href=True)
                    if a:
                        title = (a.get_text() or "").strip()
                        link = (a.get("href") or "").strip()
                        if link and is_iptv_content(title):
                            items.append((title, link, "IPTVWire"))
        except Exception:
            pass
        articles = soup.find_all("article")
        if not articles:
             articles = soup.select("div.post") or soup.select("div.entry")
        for art in articles:
            a = art.find("a") 
            h2 = art.find("h2")
            if h2:
                a = h2.find("a")
            if not a:
                continue
            title = (a.get_text() or "").strip()
            link = (a.get("href") or "").strip()
            if link and is_iptv_content(title):
                items.append((title, link, "IPTVWire"))
        if not items:
            items.extend(await ai_semantic_extract_links(html, "IPTVWire"))
        return items
    except Exception:
        logger.error("Aura Radar: Source blocked (IPTVWire). Shifting to secondary nodes.")
        return []

async def scrape_guru99() -> List[Tuple[str, str, str]]:
    try:
        html = await get_html("https://www.guru99.com/iptv-guide.html")
        if not html:
            logger.error("Aura Radar: Source blocked (Guru99). Shifting to secondary nodes.")
            return []
        soup = BeautifulSoup(html, "html.parser")
        items = []
        for a in soup.select("a"):
            title = (a.get_text() or "").strip()
            link = (a.get("href") or "").strip()
            if not link or not title:
                continue
            if link.startswith("/"):
                link = "https://www.guru99.com" + link
            if is_iptv_content(title) and "guru99.com" in link:
                items.append((title, link, "Guru99"))
        if not items:
            items.extend(await ai_semantic_extract_links(html, "Guru99"))
        return items
    except Exception:
        logger.error("Aura Radar: Source blocked (Guru99). Shifting to secondary nodes.")
        return []

async def scrape_aftvnews() -> List[Tuple[str, str, str]]:
    html = await get_html("https://www.aftvnews.com/")
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")
    items = []
    articles = soup.select("div.post-listing article")
    if not articles:
        articles = soup.select("article") # Fallback
    if not articles:
        articles = soup.select("div.post") or soup.select("div.entry")
    for art in articles:
        a = art.select_one("h2.post-title a[rel='bookmark']")
        if not a:
            a = art.select_one("h2.post-title a")
        if not a:
             a = art.find("a", href=True)
        if not a:
            continue
        title = (a.get_text() or "").strip()
        link = (a.get("href") or "").strip()
        if link and is_iptv_content(title):
            items.append((title, link, "AFTVnews"))
    if not items:
        items.extend(await ai_semantic_extract_links(html, "AFTVnews"))
    return items

async def scrape_torrentfreak() -> List[Tuple[str, str, str]]:
    html = await get_html("https://torrentfreak.com/")
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")
    items = []
    articles = soup.select("article.post")
    if not articles:
        articles = soup.select("article") # Fallback
    for art in articles:
        a = art.select_one("h2.entry-title a")
        if not a:
             a = art.find("a", href=True)
        if not a:
            continue
        title = (a.get_text() or "").strip()
        link = (a.get("href") or "").strip()
        if link and is_iptv_content(title):
            items.append((title, link, "TorrentFreak"))
    if not items:
        items.extend(await ai_semantic_extract_links(html, "TorrentFreak"))
    return items

async def _scrape_reddit_generic(url: str, source_name: str) -> List[Tuple[str, str, str]]:
    try:
        html = await get_html(url)
        if not html:
            logger.error(f"Aura Radar: Source blocked ({source_name}). Shifting to secondary nodes.")
            return []
        soup = BeautifulSoup(html, "html.parser")
        items = []
        for a in soup.select("div#siteTable .thing a.title"):
            title = (a.get_text() or "").strip()
            link = (a.get("href") or "").strip()
            if not link or not title:
                continue
            if is_iptv_content(title):
                if link.startswith("/"):
                    link = "https://old.reddit.com" + link
                items.append((title, link, source_name))
        if not items:
            items.extend(await ai_semantic_extract_links(html, source_name))
        return items
    except Exception:
        logger.error(f"Aura Radar: Source blocked ({source_name}). Shifting to secondary nodes.")
        return []

async def scrape_reddit_detailediptv() -> List[Tuple[str, str, str]]:
    return await _scrape_reddit_generic("https://old.reddit.com/r/DetailedIPTV/", "Reddit r/DetailedIPTV")

async def scrape_reddit_tivimate() -> List[Tuple[str, str, str]]:
    return await _scrape_reddit_generic("https://old.reddit.com/r/TiviMate/", "Reddit r/TiviMate")

async def scrape_reddit_iptv() -> List[Tuple[str, str, str]]:
    return await _scrape_reddit_generic("https://old.reddit.com/r/IPTV/", "Reddit r/IPTV")

async def humanize_post(source: str, title: str, educational: bool = False) -> str:
    """
    Generates a human-like post summary using an LLM (Groq/Llama).
    The persona 'Aiden' is an expert in IPTV/Streaming optimization.
    It avoids hardcoded templates and dynamically synthesizes the content.
    """
    if not ai_client:
        # Fallback only if no AI client is available (should rarely happen in prod)
        return f"Just saw this update from {source}: {title}. Check the link for details."

    try:
        # Wait for rate limit slot
        if not _ai_allow_now():
             await asyncio.sleep(2.0)

        sys_prompt = (
            "You are Aiden, a cynical but helpful streaming engineer and IPTV expert. "
            "You write short, punchy updates for a Telegram channel. "
            "Style: Tech-savvy, slightly underground, very direct. Use terms like 'handshake', 'packet loss', 'rendering path'. "
            "Formatting: "
            "- Use **bold** sparingly for only the most critical technical breakthroughs. "
            "- Use _italics_ sparingly for key emphasis. "
            "- Any introductory labels before a colon (e.g., 'Aiden's Take:') MUST be both bold and italic: ***Label:***. "
            "Structure: "
            "1. A hook (casual opener). "
            "2. The core technical problem/solution from the title. "
            "3. Why manual fixes (clearing cache) fail vs the real fix. "
            "4. A final 'Aiden's Take' on the impact. "
            "Constraints: No hashtags in body. No links. Max 100 words."
        )
        
        user_prompt = f"Source: {source}\nTitle: {title}\nContext: Analyze this topic and explain the technical implication for IPTV users (buffering, blocking, or setup)."

        resp = await ai_client.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[
                {"role": "system", "content": sys_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.7,
            max_tokens=200
        )
        
        text = (resp.choices[0].message.content or "").strip()
        
        # Sanitize any accidental links or self-referential junk
        text = _TME_SANITIZE_RE.sub('', text)
        return text

    except Exception as e:
        logger.error(f"LLM Post Gen Error: {e}")
        return f"Heads up regarding {title}. Worth a read if you're seeing stability issues on {source}."

async def load_recent_links(client: TelegramClient, channel_id: int) -> set:
    seen = set()
    try:
        msgs = await client.get_messages(channel_id, limit=50)
        for m in msgs:
            for url in extract_links(getattr(m, "text", "") or ""):
                seen.add(url.strip())
    except Exception:
        pass
    return seen

risky_skipped_today = 0
educational_rephrased_today = 0
last_audit_date = None

def is_sensitive_url(link: str) -> bool:
    link_lower = (link or "").lower()
    return ("copyright" in link_lower) or ("infring" in link_lower)

async def maybe_send_daily_audit(client: TelegramClient) -> None:
    global last_audit_date, risky_skipped_today, educational_rephrased_today
    try:
        today = datetime.now(timezone.utc).date()
        if last_audit_date is None:
            last_audit_date = today
            return
        if today != last_audit_date:
            msg = (
                f"[Curator Audit] Risky skipped: {risky_skipped_today}; "
                f"Educational rephrased: {educational_rephrased_today}; "
                f"Deletions: 0"
            )
            try:
                send_dm = (os.environ.get("CURATOR_AUDIT_DM") or "").strip().lower() in ("1", "true", "yes")
                if send_dm:
                    # Status update to 'me' disabled
                    # await client.send_message('me', msg)
                    pass
            except Exception:
                pass
            risky_skipped_today = 0
            educational_rephrased_today = 0
            last_audit_date = today
    except Exception:
        pass

def get_random_pro_tip() -> str:
    import random
    return random.choice(PRO_TIPS)

def _resolve_url(base: str, link: str) -> str:
    try:
        if not link:
            return ""
        if link.startswith("http"):
            return link
        return urljoin(base, link)
    except Exception:
        return link or ""

def _make_gradient(w: int, h: int, c1: Tuple[int, int, int], c2: Tuple[int, int, int]) -> Image.Image:
    g = Image.new("L", (1, h))
    px = g.load()
    for y in range(h):
        v = int(255 * (y / max(1, h - 1)))
        px[0, y] = v
    g = g.resize((w, h))
    base = Image.new("RGB", (w, h), c1)
    overlay = Image.new("RGB", (w, h), c2)
    return Image.composite(overlay, base, g)

def _lighten(color: Tuple[int, int, int], amt: float) -> Tuple[int, int, int]:
    r, g, b = color
    return (min(255, int(r + 255 * amt)), min(255, int(g + 255 * amt)), min(255, int(b + 255 * amt)))

def _darken(color: Tuple[int, int, int], amt: float) -> Tuple[int, int, int]:
    r, g, b = color
    return (max(0, int(r * (1 - amt))), max(0, int(g * (1 - amt))), max(0, int(b * (1 - amt))))

def get_sentiment(text: str) -> str:
    s = (text or "").lower()
    neg = sum(1 for k in ["error", "fail", "blocked", "freeze", "stutter", "buffer", "throttle"] if k in s)
    pos = sum(1 for k in ["fix", "stable", "smooth", "success", "optimized"] if k in s)
    if neg > pos + 1:
        return "negative"
    if pos > neg + 1:
        return "positive"
    return "neutral"

def choose_palette(topic: str, sentiment: str) -> Tuple[Tuple[int, int, int], Tuple[int, int, int]]:
    base1, base2, accent = BRAND_COLORS
    c1, c2 = (base1, base2)
    if topic == "fix":
        c1, c2 = (base2, base1)
    elif topic == "guide":
        c1, c2 = (base1, base2)
    else:
        c1, c2 = (base1, base1)
    if sentiment == "negative":
        c1, c2 = (_darken(c1, 0.2), _darken(c2, 0.2))
    elif sentiment == "positive":
        c1, c2 = (_lighten(c1, 0.1), _lighten(c2, 0.1))
    return c1, c2

def _place_logo(img: Image.Image) -> Image.Image:
    try:
        logo_path = os.path.join("brand", "ax_logo.png")
        if not os.path.exists(logo_path):
            return img
        logo = Image.open(logo_path).convert("RGBA")
        bw, bh = img.size
        lw, lh = logo.size
        max_w = int(bw * 0.22)
        scale = max_w / lw if lw > max_w else 1.0
        nw = int(lw * scale)
        nh = int(lh * scale)
        logo = logo.resize((nw, nh), Image.LANCZOS)
        alpha = logo.split()[-1].point(lambda a: int(a * 0.85))
        logo.putalpha(alpha)
        base = img.convert("RGBA")
        x = bw - nw - int(bw * 0.02)
        y = bh - nh - int(bh * 0.02)
        base.paste(logo, (max(0, x), max(0, y)), logo)
        return base.convert("RGB")
    except Exception:
        return img

def _draw_text(img: Image.Image, title: str, topic: str) -> Image.Image:
    draw = ImageDraw.Draw(img)
    w, h = img.size
    tt = (title or "").upper()[:56]
    try:
        font_main = ImageFont.truetype("arial.ttf", max(36, int(h * 0.06)))
    except Exception:
        font_main = ImageFont.load_default()
    try:
        font_sub = ImageFont.truetype("arial.ttf", max(24, int(h * 0.035)))
    except Exception:
        font_sub = ImageFont.load_default()
    
    try:
        tw, th = draw.textsize(tt, font=font_main)
    except AttributeError:
        bbox = draw.textbbox((0, 0), tt, font=font_main)
        tw = bbox[2] - bbox[0]
        th = bbox[3] - bbox[1]

    draw.text((int((w - tw) / 2), int(h * 0.18)), tt, fill=(255, 255, 255), font=font_main)
    sub = "AURA APEX SUPREME"
    
    try:
        sw, sh = draw.textsize(sub, font=font_sub)
    except AttributeError:
        bbox = draw.textbbox((0, 0), sub, font=font_sub)
        sw = bbox[2] - bbox[0]
        sh = bbox[3] - bbox[1]

    draw.text((int((w - sw) / 2), int(h * 0.18) + th + 30), sub, fill=(255, 215, 0), font=font_sub)
    tag = "#AuraGuide" if topic == "guide" else "#AuraFix" if topic == "fix" else "#AuraNews"
    
    try:
        tw2, th2 = draw.textsize(tag, font=font_sub)
    except AttributeError:
        bbox = draw.textbbox((0, 0), tag, font=font_sub)
        tw2 = bbox[2] - bbox[0]
        th2 = bbox[3] - bbox[1]

    draw.text((int((w - tw2) / 2), int(h * 0.18) + th + sh + 50), tag, fill=(200, 200, 200), font=font_sub)
    return img

def _brand_compliant(img: Image.Image) -> bool:
    try:
        w, h = img.size
        p = img.getpixel((int(w*0.5), int(h*0.1)))
        return isinstance(p, tuple) and len(p) >= 3
    except Exception:
        return True

def _relevance_score(title: str, topic: str) -> float:
    s = (title or "").lower()
    if topic == "guide":
        k = ["setup", "tutorial", "tivimate", "smarters", "m3u", "how to"]
    elif topic == "fix":
        k = ["vpn", "buffer", "error", "handshake", "throttling", "fix"]
    else:
        k = ["news", "update", "global", "broadcast"]
    hits = sum(1 for x in k if x in s)
    return min(1.0, hits / max(1, len(k)))

def _generate_image_for_size(title: str, topic: str, size: Tuple[int, int]) -> bytes | None:
    try:
        w, h = size
        sent = get_sentiment(title or "")
        c1, c2 = choose_palette(topic, sent)
        bg = _make_gradient(w, h, c1, c2)
        bg = _draw_text(bg, title, topic)
        bg = _place_logo(bg)
        if not _brand_compliant(bg):
            return None
        out = io.BytesIO()
        bg.save(out, format="JPEG", quality=92)
        out.seek(0)
        return out.read()
    except Exception:
        return None

def _fallback_library_image(topic: str) -> bytes | None:
    try:
        lib = {
            "guide": os.path.join("brand", "library", "guide.jpg"),
            "fix": os.path.join("brand", "library", "fix.jpg"),
            "news": os.path.join("brand", "library", "news.jpg"),
        }
        p = lib.get(topic)
        if not p or not os.path.exists(p):
            return None
        img = Image.open(p).convert("RGB")
        out = io.BytesIO()
        img.save(out, format="JPEG", quality=92)
        out.seek(0)
        return out.read()
    except Exception:
        return None

def _template_default_image(title: str, topic: str) -> bytes | None:
    try:
        w, h = PLATFORM_SPECS["4k"]
        c1, c2 = BRAND_COLORS[0], BRAND_COLORS[1]
        bg = _make_gradient(w, h, c1, c2)
        bg = _draw_text(bg, title, topic)
        bg = _place_logo(bg)
        out = io.BytesIO()
        bg.save(out, format="JPEG", quality=88)
        out.seek(0)
        return out.read()
    except Exception:
        return None

def _save_platform_images(title: str, topic: str, images: dict) -> None:
    try:
        persist = (os.environ.get("PERSIST_IMAGES", "").strip().lower() in ("1", "true", "yes"))
        if not persist:
            return
        os.makedirs(os.path.join("data", "images"), exist_ok=True)
        base = re.sub(r'[^A-Za-z0-9_\-]+', '_', (title or 'aura_apex'))[:60]
        for name, b in (images or {}).items():
            if not b:
                continue
            fp = os.path.join("data", "images", f"{base}_{topic}_{name}.jpg")
            try:
                with open(fp, "wb") as f:
                    f.write(b)
            except Exception:
                pass
    except Exception:
        pass

def generate_post_images(title: str, body: str, topic: str) -> Tuple[bytes | None, dict, dict]:
    key = re.sub(r'[^a-z0-9]+', '_', (title or '').lower()) + "_" + (topic or "news")
    if key in POST_IMAGE_CACHE:
        return POST_IMAGE_CACHE[key]["4k"], POST_IMAGE_CACHE[key]["platforms"], POST_IMAGE_CACHE[key]["metrics"]
    images = {}
    metrics = {}
    r = _relevance_score(title, topic)
    for name, size in PLATFORM_SPECS.items():
        b = _generate_image_for_size(title, topic, size)
        images[name] = b
    if not images.get("4k"):
        fb = _fallback_library_image(topic) or _template_default_image(title, topic)
        images["4k"] = fb
    ok = sum(1 for v in images.values() if v)
    rate = ok / max(1, len(images))
    metrics["success_rate"] = round(rate, 2)
    metrics["relevance"] = round(r, 2)
    metrics["brand_ok"] = all(bool(v) for v in images.values())
    main = images.get("4k")
    _save_platform_images(title, topic, images)
    POST_IMAGE_CACHE[key] = {"4k": main, "platforms": images, "metrics": metrics}
    return main, images, metrics

async def extract_feature_image_url(article_url: str) -> str:
    try:
        session = await get_session()
        async with session.get(article_url, timeout=5) as r:
            if r.status != 200:
                return ""
            html = await r.text()
    except Exception as e:
        logger.debug(f"Failed to fetch feature image: {e}")
        return ""
    try:
        soup = BeautifulSoup(html, "html.parser")
        m = soup.select_one("meta[property='og:image'], meta[name='og:image']")
        if m:
            u = (m.get("content") or "").strip()
            u = _resolve_url(article_url, u)
            if u:
                return u
        imgs = soup.find_all("img")
        for im in imgs:
            src = (im.get("src") or "").strip()
            if not src:
                continue
            w = im.get("width") or im.get("data-width") or ""
            try:
                wv = int(re.sub(r"\D", "", str(w))) if w else 0
            except Exception:
                wv = 0
            u = _resolve_url(article_url, src)
            if wv and wv >= 400:
                return u
        for im in imgs:
            src = (im.get("src") or "").strip()
            u = _resolve_url(article_url, src)
            if u:
                return u
    except Exception as e:
        logger.debug(f"Error parsing feature image: {e}")
        return ""
    return ""

def _text_clean(x: str) -> str:
    return re.sub(r'\s+', ' ', (x or '').strip())

def _collect_list_items(root) -> List[str]:
    items = []
    for ol in root.find_all(['ol', 'ul']):
        for li in ol.find_all('li'):
            t = _text_clean(li.get_text())
            if t and len(t) >= 3:
                items.append(t)
    return items

def _collect_step_paragraphs(root) -> List[str]:
    items = []
    for p in root.find_all('p'):
        t = _text_clean(p.get_text())
        if not t:
            continue
        if re.match(r'^\d+[\.\)]\s+', t) or re.search(r'\bstep\s*\d+\b', t, flags=re.I):
            items.append(t)
        elif any(k in t.lower() for k in ["click ", "open ", "go to ", "select ", "enable ", "disable ", "set ", "enter ", "choose "]):
            items.append(t)
    return items

def _main_content_container(soup: BeautifulSoup):
    for sel in ['article', '.entry-content', '.post-content', 'main', '#content', '.content']:
        m = soup.select_one(sel)
        if m:
            return m
    return soup

async def ai_refine_steps(raw_steps: List[str]) -> List[str]:
    try:
        if not ai_client or not raw_steps:
            return raw_steps
        prompt = "Refine and order the following IPTV tutorial steps into concise actionable lines. Keep numbering and imperative phrasing. Return as lines only."
        up = prompt + "\n\n" + "\n".join([f"{i+1}. {s}" for i, s in enumerate(raw_steps)])
        resp = await ai_client.chat.completions.create(
            messages=[{"role": "system", "content": "You improve procedural instructions."}, {"role": "user", "content": up}],
            model="llama-3.3-70b-versatile",
            temperature=0.2,
            max_tokens=500
        )
        out = (resp.choices[0].message.content or "").strip().splitlines()
        cleaned = []
        for line in out:
            lt = _text_clean(line)
            if lt:
                lt = re.sub(r'^\s*(\d+[\.\)]\s*)?', '', lt)
                cleaned.append(lt)
        if cleaned:
            return cleaned
        return raw_steps
    except Exception:
        return raw_steps

def compute_extraction_metrics(steps: List[str], containers_found: int, lists_found: int) -> dict:
    verbs = ["click", "open", "go to", "select", "enable", "disable", "set", "enter", "choose", "install", "configure"]
    actionable = sum(1 for s in steps if any(v in s.lower() for v in verbs))
    accuracy = (actionable / max(1, len(steps)))
    completeness = min(1.0, len(steps) / max(1, lists_found * 3))
    hierarchy = 1.0 if containers_found > 0 and lists_found > 0 else 0.5 if lists_found > 0 else 0.0
    return {"accuracy": round(accuracy, 2), "completeness": round(completeness, 2), "hierarchy": round(hierarchy, 2)}

async def extract_instructional_content(article_url: str) -> Tuple[List[str], dict]:
    try:
        html = await get_html(article_url)
        if not html:
            return [], {"accuracy": 0.0, "completeness": 0.0, "hierarchy": 0.0}
        soup = BeautifulSoup(html, "html.parser")
        root = _main_content_container(soup)
        lists = _collect_list_items(root)
        paras = _collect_step_paragraphs(root)
        raw = []
        seen = set()
        for t in lists + paras:
            k = t.lower()
            if k in seen:
                continue
            seen.add(k)
            raw.append(t)
        refined = await ai_refine_steps(raw)
        metrics = compute_extraction_metrics(refined, 1 if root else 0, len(root.find_all(['ol', 'ul'])) if root else 0)
        return refined, metrics
    except Exception:
        return [], {"accuracy": 0.0, "completeness": 0.0, "hierarchy": 0.0}

async def format_instructional_body(title: str, steps: List[str]) -> str:
    if not steps:
        return ""
    
    # If no AI client, fallback to simple list
    if not ai_client:
        parts = []
        parts.append(f"Just caught this—{title}. Actionable walkthrough:")
        for i, s in enumerate(steps, 1):
            parts.append(f"{i}. {s}")
        txt = "\n".join(parts)
        return _TME_SANITIZE_RE.sub('', txt)[:1400]

    try:
        if not _ai_allow_now():
            await asyncio.sleep(2.0)
            
        sys_prompt = (
            "You are Aiden, an IPTV expert. Convert these raw steps into a clean, "
            "easy-to-follow guide for a Telegram post. "
            "Style: Direct, imperative, helpful. "
            "Format: Numbered list. "
            "Formatting: "
            "- Use **bold** and _italics_ sparingly to maintain high readability. "
            "- Any step headers or labels before a colon (e.g., 'Step 1:') MUST be both bold and italic: ***Step 1:***. "
            "Constraint: Max 200 words. No hashtags."
        )
        
        user_prompt = f"Title: {title}\nRaw Steps:\n" + "\n".join(steps)
        
        resp = await ai_client.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[
                {"role": "system", "content": sys_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.3,
            max_tokens=300
        )
        
        txt = (resp.choices[0].message.content or "").strip()
        return _TME_SANITIZE_RE.sub('', txt)

    except Exception as e:
        logger.error(f"LLM Instructional Body Error: {e}")
        # Fallback
        parts = []
        parts.append(f"Guide: {title}")
        for i, s in enumerate(steps, 1):
            parts.append(f"{i}. {s}")
        return "\n".join(parts)[:1400]

async def download_image(url: str) -> Image.Image | None:
    try:
        session = await get_session()
        async with session.get(url, timeout=5) as r:
            if r.status != 200:
                return None
            content = await r.read()
            b = io.BytesIO(content)
        b.seek(0)
        img = Image.open(b)
        return img
    except Exception as e:
        logger.debug(f"Image download failed: {e}")
        return None

def apply_watermark(img: Image.Image) -> Image.Image:
    try:
        logo_path = os.path.join("brand", "ax_logo.png")
        if not os.path.exists(logo_path):
            return img
        logo = Image.open(logo_path).convert("RGBA")
        bw, bh = img.size
        lw, lh = logo.size
        max_w = int(bw * 0.25)
        scale = max_w / lw if lw > max_w else 1.0
        nw = int(lw * scale)
        nh = int(lh * scale)
        logo = logo.resize((nw, nh), Image.LANCZOS)
        if logo.mode != "RGBA":
            logo = logo.convert("RGBA")
        alpha = logo.split()[-1]
        alpha = alpha.point(lambda a: int(a * 0.7))
        logo.putalpha(alpha)
        base = img.convert("RGBA")
        x = bw - nw - 16
        y = bh - nh - 16
        base.paste(logo, (max(0, x), max(0, y)), logo)
        return base.convert("RGB")
    except Exception:
        return img

async def prepare_photo(article_url: str) -> bytes | None:
    try:
        iu = await extract_feature_image_url(article_url)
        if not iu:
            return None
        img = await download_image(iu)
        if img is None:
            return None
        w, h = img.size
        if w < 150 or h < 150:
            return None
        img = apply_watermark(img)
        out = io.BytesIO()
        fmt = "JPEG"
        try:
            if img.mode in ("RGBA", "LA"):
                fmt = "PNG"
        except Exception:
            fmt = "JPEG"
        img.save(out, format=fmt, quality=90)
        out.seek(0)
        return out.read()
    except Exception:
        return None

def local_store_enabled() -> bool:
    v = os.environ.get("PERSIST_POSTS", "").strip().lower()
    if v in ("1", "true", "yes"):
        try:
            os.makedirs("data", exist_ok=True)
        except Exception:
            pass
        return True
    try:
        os.makedirs("data", exist_ok=True)
        return True
    except Exception:
        return False

def load_local_links() -> set:
    s = set()
    if not local_store_enabled():
        return s
    p = os.path.join("data", "posted_links.json")
    try:
        if os.path.exists(p):
            with open(p, "r", encoding="utf-8") as f:
                for q in (f.read().splitlines() or []):
                    if q.strip():
                        s.add(q.strip())
    except Exception:
        return s
    return s

def append_local_link(link: str) -> None:
    if not local_store_enabled():
        return
    try:
        os.makedirs("data", exist_ok=True)
    except Exception:
        pass
    p1 = os.path.join("data", "posted_links.json")
    p2 = os.path.join("data", "posted_links.jsonl")
    try:
        with open(p1, "a", encoding="utf-8") as f:
            f.write(link.strip() + "\n")
    except Exception:
        pass
    try:
        rec = {"link": link.strip(), "ts": int(time.time())}
        with open(p2, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec) + "\n")
    except Exception:
        pass

WELCOME_TAG = "[Aura Welcome]"

async def load_forum_thread_map(client: TelegramClient, channel_id: int) -> dict:
    try:
        from telethon.tl.functions.channels import GetForumTopicsRequest
        res = await client(GetForumTopicsRequest(channel=channel_id, limit=50))
        items = getattr(res, "topics", []) or []
        out = {}
        for t in items:
            nm = (getattr(t, "title", "") or "").lower()
            top_id = getattr(t, "top_msg_id", None)
            if not top_id:
                continue
            if "guide" in nm or "tutorial" in nm:
                out["#AuraGuide"] = top_id
            elif "fix" in nm or "issue" in nm or "troubleshoot" in nm:
                out["#AuraFix"] = top_id
            elif "news" in nm or "update" in nm or "announcement" in nm:
                out["#AuraNews"] = top_id
            elif "update" in nm or "release" in nm:
                out["#AuraUpdate"] = top_id
        return out
    except Exception:
        return {}

def emoji_for_hashtag(tag: str) -> str:
    if tag == "#AuraFix":
        return "🛠️"
    if tag == "#AuraUpdate":
        return "🚀"
    if tag == "#AuraGuide":
        return "📖"
    if tag == "#AuraNews":
        return "🛰️"
    return "📡"

async def rewrite_headline(title: str) -> str:
    """
    Rewrites listicle/clickbait titles to be direct and professional.
    Example: "10 Best IPTV Apps" -> "The Best IPTV Apps"
    """
    # AI Savings: Only use AI for rewriting if savings is disabled or with 50% chance
    if not ai_client or (os.environ.get("AURA_AI_SAVINGS", "0") == "1" and random.random() > 0.5):
        # Fallback regex cleanup for common listicle patterns
        clean = re.sub(r'^\d+\s+(Best|Top|Greatest|Most)\s+', r'\1 ', title, flags=re.IGNORECASE)
        return clean

    try:
        sys_prompt = (
            "You are a copy editor. Rewrite the headline to remove listicle numbers and clickbait. "
            "Make it direct, authoritative, and concise. "
            "Examples:\n"
            "- '10 Best Firestick Apps' -> 'Essential Firestick Apps'\n"
            "- '7 Ways to Fix Buffering' -> 'How to Fix Buffering'\n"
            "- 'Top 5 IPTV Players' -> 'Top IPTV Players Ranked'\n"
            "Do NOT use quotes. Output ONLY the new headline."
        )
        
        resp = await ai_client.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[{"role": "system", "content": sys_prompt}, {"role": "user", "content": f"Rewrite: {title}"}],
            temperature=0.3,
            max_tokens=20
        )
        
        new_title = (resp.choices[0].message.content or "").strip()
        # Sanity check: if it returns something too long or empty, keep original
        if len(new_title) > len(title) + 20 or not new_title:
            return title
        return _TME_SANITIZE_RE.sub('', new_title)
    except Exception:
        return title

async def post_to_channel(client: TelegramClient, channel_id: int, source: str, title: str, link: str, verified: bool = False):
    try:
        testing = (os.environ.get("AURA_MODE", "").strip().lower() == "testing")
        sensitive = is_sensitive_url(link)
        if sensitive and not ai_client:
            global risky_skipped_today
            risky_skipped_today += 1
            return False
        if not testing:
            if not strict_iptv_allowed(title) or competitor_banned(title):
                return False
        
        # Rewrite title to remove listicle numbers
        final_title = await rewrite_headline(title)
        
        topic_info = classify_topic(final_title)
        if topic_info["topic"] == "guide":
            steps, metrics = await extract_instructional_content(link)
            body = await format_instructional_body(final_title, steps) if steps else await humanize_post(source, final_title, educational=sensitive)
        else:
            body = await humanize_post(source, final_title, educational=sensitive)
        if sensitive:
            global educational_rephrased_today
            educational_rephrased_today += 1
        hashtag = topic_info["hashtag"]
        top_bar = "🌟━━━━━━━━━━━━━━━🌟"
        title_block = f"🔥**{(final_title or '').upper()}**🔥"
        pt = await get_pro_tip_for_topic(topic_info["topic"])
        pro_tip_block = pt
        
        # Build dynamic hashtags based on content
        dynamic_tags = await _build_dynamic_tags_async(final_title, body)
        ver_tag = " #Verified" if verified else ""
        
        # Assemble tag block
        # Start with core system tags, then add verified, then dynamic ones
        tags_block = f"#AuraApex #ApexSupreme #IPTVTech{ver_tag}"
        if dynamic_tags:
             tags_block += " " + " ".join(dynamic_tags)
        
        text = f"{top_bar}\n{title_block}\n{top_bar}\n\n{body}\n\n{pro_tip_block}\n{top_bar}\n{tags_block}"
        thread_id = topic_info["thread"] or THREAD_MAP.get(hashtag) or THREAD_MAP.get("#AuraNews")
        thread_id = DYNAMIC_THREAD_MAP.get(hashtag, thread_id)
        
        img_bytes = await generate_curator_image_async(final_title, body, topic_info["topic"])
        
        if img_bytes:
            fname = _FILENAME_CLEAN_RE.sub('_', (final_title or 'aura_apex'))[:60] + ".jpg"
            bio = io.BytesIO(img_bytes)
            try:
                bio.name = fname
            except Exception:
                pass
            try:
                await client.send_file(
                    channel_id,
                    file=bio,
                    caption=text,
                    reply_to=thread_id,
                    force_document=False
                )
            except Exception as e:
                logger.warning(f"Failed to send file, retrying: {e}")
                try:
                    tm = await load_forum_thread_map(client, channel_id)
                    if tm:
                        DYNAMIC_THREAD_MAP.update(tm)
                        thread_id = DYNAMIC_THREAD_MAP.get(hashtag, thread_id)
                    await client.send_file(
                        channel_id,
                        file=bio,
                        caption=text,
                        reply_to=thread_id,
                        force_document=False
                    )
                except Exception as e2:
                    logger.error(f"Failed to send file after retry: {e2}")
                    return False
        else:
            try:
                await client.send_message(channel_id, text, reply_to=thread_id)
            except Exception as e:
                logger.warning(f"Failed to send msg, retrying: {e}")
                try:
                    tm = await load_forum_thread_map(client, channel_id)
                    if tm:
                        DYNAMIC_THREAD_MAP.update(tm)
                        thread_id = DYNAMIC_THREAD_MAP.get(hashtag, thread_id)
                    await client.send_message(channel_id, text, reply_to=thread_id)
                except Exception as e2:
                    logger.error(f"Failed to send msg after retry: {e2}")
                    return False
        try:
            # Post notification to 'me' disabled
            # await client.send_message('me', f"[Curator] Posted: {link}")
            pass
        except Exception:
            pass
        await _save_dedup(title, link)
        return True
    except FloodWaitError as e:
        await asyncio.sleep(int(getattr(e, "seconds", 60)))
        return False
    except PeerFloodError:
        return False
    except Exception as e:
        logger.error(f"Post to channel failed: {e}")
        return False

async def count_today_posts(client: TelegramClient, channel_id: int) -> int:
    try:
        msgs = await client.get_messages(channel_id, limit=100)
        today = datetime.now(timezone.utc).date()
        c = 0
        for m in msgs:
            dt = getattr(m, "date", None)
            txt = (getattr(m, "text", "") or "")
            if dt and dt.date() == today and any(tag in txt for tag in ["#AuraApex", "#ApexSupreme"]):
                c += 1
        return c
    except Exception:
        return 0

async def total_curator_posts_logged(client: TelegramClient) -> int:
    try:
        msgs = await client.get_messages('me', limit=200)
        return sum(1 for m in msgs if (getattr(m, "text", "") or "").startswith("[Curator] Posted:"))
    except Exception:
        return 0

async def maybe_post_soft_sale(client: TelegramClient, channel_id: int, today_count: int, global_count: int) -> bool:
    try:
        # Limit to 1 post per day in production
        if today_count >= 1 and os.environ.get("AURA_MODE", "").lower() != "testing":
            return False
        # Post a soft sale on every 5th curator post
        if (global_count + 1) % 5 == 0:
            
            header = "**🛠️ #AuraFix | Skip Manual Fixes with Hardcoding**"
            contact_line = "📩 @AuraApexSupport_Bot"
            
            # Use AI if available to generate fresh copy
            if ai_client and _ai_allow_now():
                try:
                    sys_p = (
                        "Write a 2-sentence sales hook for 'Aura Apex' (a hardcoded IPTV app). "
                        "Pain point: Users tired of manually entering DNS/URLs. "
                        "Solution: We hardcode it so it never breaks. "
                        "Tone: Expert, slightly arrogant but helpful (Aiden persona). "
                        "No hashtags."
                    )
                    resp = await ai_client.chat.completions.create(
                        model="llama-3.1-8b-instant",
                        messages=[{"role":"system", "content": sys_p}],
                        temperature=0.8,
                        max_tokens=100
                    )
                    body = (resp.choices[0].message.content or "").strip()
                    body = _TME_SANITIZE_RE.sub('', body)
                except Exception:
                     body = (
                        "Just caught this—too many guides push DNS tweaks. Fix: hardcode DNS and portal so "
                        "users stop manual patches. Aiden: hardcoding kills leakage and handshake flaps."
                    )
            else:
                 body = (
                    "Just caught this—too many guides push DNS tweaks. Fix: hardcode DNS and portal so "
                    "users stop manual patches. Aiden: hardcoding kills leakage and handshake flaps."
                )

            # Use AI for pro-tip too
            pro_tip_block = await get_pro_tip_for_topic("fix")
            if not pro_tip_block.startswith(">"):
                pro_tip_block = f"> {pro_tip_block}"
            
            text = f"{header}\n\n{body}\n\n{pro_tip_block}\n\n{contact_line}\n#AuraFix"
            await client.send_message(channel_id, text)
            await _save_dedup(header, "")
            try:
                # Sale notification to 'me' disabled
                # await client.send_message('me', f"[Curator] Soft Sale Posted")
                pass
            except Exception:
                pass
            return True
        return False
    except Exception as e:
        logger.error(f"Soft sale post failed: {e}")
        return False

# Market Windows (Aiden Lifecycle 2026)
def is_within_market_window(dt: datetime.datetime) -> bool:
    """Checks if current time is within any defined market window."""
    return get_target_topic_for_time(dt) is not None

def get_target_topic_for_time(dt: datetime.datetime) -> Optional[str]:
    """Returns the expected topic for a given local time and day of week (Aiden Lifecycle)."""
    h = dt.hour
    m = dt.minute
    total_min = h * 60 + m
    day = dt.weekday() # 0=Mon, 6=Sun
    
    # Mon-Fri: #AuraNews (08:30 – 10:00)
    if 0 <= day <= 4:
        if 510 <= total_min < 600: # 8:30 is 510m, 10:00 is 600m
            return "news"
            
    # Tue/Thu: #AuraGuide (13:00 – 15:00)
    if day in (1, 3):
        if 780 <= total_min < 900: # 13:00 is 780m, 15:00 is 900m
            return "guide"
            
    # Wed/Sat: #AuraFix (19:00 – 21:00)
    if day in (2, 5):
        if 1140 <= total_min < 1260: # 19:00 is 1140m, 21:00 is 1260m
            return "fix"
            
    # Sun: #AuraUpdate (11:00 – 13:00)
    if day == 6:
        if 660 <= total_min < 780: # 11:00 is 660m, 13:00 is 780m
            return "update"
            
    return None

def _get_gaussian_jitter_interval() -> float:
    base = float(CHECK_INTERVAL_SECONDS)
    if os.environ.get("AURA_MODE", "").lower() == "production":
        base = max(base, 900.0) # 15 min minimum in prod
    # +/- 10% jitter
    import random
    jitter = base * 0.1 * random.gauss(0, 1)
    return max(60.0, base + jitter)

async def curator_loop():
    from config import TARGET_CHANNEL_ID, CHANNEL_INVITE_LINK
    chan_raw = (TARGET_CHANNEL_ID or CURATOR_CHANNEL_ID or "")
    try:
        channel_id = int(chan_raw)
    except Exception:
        channel_id = 0
    session = StringSession(SESSION_STRING) if SESSION_STRING else None
    client = TelegramClient(session or "aura_curator_session", int(API_ID), API_HASH)
    await client.start()
    
    # Permissions check
    try:
        if not channel_id and (CHANNEL_INVITE_LINK or "").strip():
            channel_id = await resolve_channel_id(client, CHANNEL_INVITE_LINK.strip())
            if not channel_id:
                logger.error("Error: Unable to resolve channel ID from invite link.")
                return
        
        # Ensure we have admin rights
        admin_ok = await has_admin_rights(client, channel_id)
        if not admin_ok:
            logger.warning("Warning: Bot may lack admin rights; pinning may fail.")
            
        # Load forum topics map
        try:
            tm = await load_forum_thread_map(client, channel_id)
            if tm:
                DYNAMIC_THREAD_MAP.update(tm)
        except Exception:
            pass
            
        # Initial load of dedup cache
        _load_dedup()
        
        last_hb = time.time()
        
        # Start background sentinel
        asyncio.create_task(dead_link_sentinel(client, channel_id))
        
        while True:
            try:
                today_count = await count_today_posts(client, channel_id)
                global_count = await total_curator_posts_logged(client)
                testing_mode = os.environ.get("AURA_MODE", "").lower() == "testing"
                
                # Determine current market window
                tz = _curator_tzinfo()
                now = datetime.now(tz)
                target_topic = get_target_topic_for_time(now)
                in_window = is_within_market_window(now)
                
                if testing_mode:
                    logger.info(f"TESTING MODE: Ignoring schedule. Target topic: {target_topic} (but ignored)")
                    target_topic = None 
                else:
                    logger.info(f"Curator status: {now.strftime('%H:%M')} {tz.key}. Market window: {in_window}. Target: {target_topic}")
                
                # If we are in production and OUTSIDE any window, we should skip posting unless it's a critical update
                if not testing_mode and not in_window:
                     # We still scrape because updates can happen anytime
                     pass

                if today_count < 1 or testing_mode:
                    sources = [
                        ("TROYPOINT", scrape_troypoint),
                        ("IPTVWire", scrape_iptvwire),
                        ("Guru99", scrape_guru99),
                        ("AFTVnews", scrape_aftvnews),
                        ("TorrentFreak", scrape_torrentfreak),
                        ("Reddit r/DetailedIPTV", scrape_reddit_detailediptv),
                        ("Reddit r/TiviMate", scrape_reddit_tivimate),
                        ("Reddit r/IPTV", scrape_reddit_iptv),
                    ]
                    enabled = set((CONFIG_RULES.get("ENABLED_SOURCES") or []))
                    if enabled:
                        sources = [s for s in sources if s[0] in enabled]
                        
                    tasks = [ _run_source(name, func) for name, func in sources ]
                    results = await asyncio.gather(*tasks, return_exceptions=False)
                    
                    results_flat = []
                    for arr in results:
                        if arr:
                            results_flat.extend(arr)
                    
                    # Dedup
                    filtered = []
                    for t, l, s in results_flat:
                        try:
                            if _is_duplicate(t, l, 0.90):
                                continue
                        except Exception:
                            pass
                        filtered.append((t, l, s))
                    
                    new_items = filtered
                    
                    # Sort high value first
                    hv = [it for it in new_items if is_high_value_topic(it[0])]
                    rest = [it for it in new_items if not is_high_value_topic(it[0])]
                    new_items = hv + rest
                    
                    for title, link, source in new_items:
                        # Only post once per day when in production mode
                        if today_count >= 1 and not testing_mode:
                            break
                            
                        if _is_duplicate(title, link, 0.90):
                            continue
                            
                        # Topic Classification
                        raw_topic_info = classify_topic(title)
                        is_update = "update" in raw_topic_info["topic"] or "release" in title.lower() or "patch" in title.lower()
                        
                        if not testing_mode:
                            if is_update:
                                pass 
                            elif target_topic:
                                if raw_topic_info["topic"] != target_topic:
                                    continue
                            else:
                                continue
                        
                        # Post it
                        ok = await post_to_channel(client, channel_id, source, title, link)
                        if ok:
                            today_count += 1
                            global_count += 1
                            append_local_link(link) # Add this to persist links for sentinel
                            
                # Soft Sale Logic
                # We also dedup soft sales by content type
                header = "**🛠️ #AuraFix | Skip Manual Fixes with Hardcoding**"
                if not _is_duplicate(header, "", 0.90):
                    await maybe_post_soft_sale(client, channel_id, today_count, global_count)
                            
                # Daily Audit
                await maybe_send_daily_audit(client)
                
                # Heartbeat
                if (time.time() - last_hb) >= 3600:
                    try:
                        logger.info(f"Curator heartbeat ok")
                        hb = (os.environ.get("CURATOR_HEARTBEAT_DM", "").strip().lower() in ("1", "true", "yes"))
                        if hb:
                            summary = "All Systems Nominal"
                            if _SOURCE_METRICS:
                                details = "; ".join([f"{n}: {m.get('success',0)}/{m.get('attempts',0)}" for n, m in _SOURCE_METRICS.items()])
                                summary = f"{summary}\n\n{details}"
                            # Summary to 'me' disabled
                            # await client.send_message('me', f"[Curator] {summary}")
                    except Exception as e:
                        logger.error(f"Heartbeat report error: {e}")
                    last_hb = time.time()
                
                # Sleep with Jitter
                sleep_sec = _get_gaussian_jitter_interval()
                await asyncio.sleep(sleep_sec)
                
            except Exception as e:
                logger.error(f"Curator Loop Error: {e}")
                await asyncio.sleep(300)
    except Exception as e:
        logger.critical(f"Curator Critical Failure: {e}")

async def dead_link_sentinel(client: TelegramClient, channel_id: int):
    while True:
        try:
            p = os.path.join("data", "posted_links.jsonl")
            if not os.path.exists(p):
                await asyncio.sleep(21600)
                continue
            cutoff = int(time.time()) - 30 * 86400
            to_check: List[str] = []
            seen = set()
            try:
                with open(p, "r", encoding="utf-8") as f:
                    for line in f:
                        try:
                            rec = json.loads(line)
                            ln = str(rec.get("link","")).strip()
                            ts = int(rec.get("ts", 0))
                            if ln and ts <= cutoff and ln not in seen:
                                to_check.append(ln)
                                seen.add(ln)
                        except Exception:
                            continue
            except Exception:
                to_check = []
            for ln in to_check[:10]:
                try:
                    ok = False
                    try:
                        session = await get_session()
                        async with session.head(ln, timeout=10) as r:
                            ok = (200 <= r.status < 400)
                    except Exception:
                        ok = False
                    if not ok:
                        msg = "Update: Source link is down, checking for mirrors."
                        try:
                            await client.send_message(channel_id, msg)
                        except Exception:
                            pass
                    await asyncio.sleep(1.0)
                except Exception:
                    continue
            await asyncio.sleep(21600)
        except Exception:
            await asyncio.sleep(21600)
def extract_invite_hash(invite_link: str) -> str:
    if not invite_link:
        return ""
    m = _INVITE_HASH_RE.search(invite_link)
    if m:
        return m.group(1)
    m = _JOINCHAT_HASH_RE.search(invite_link)
    if m:
        return m.group(1)
    return ""

async def resolve_channel_id(client: TelegramClient, invite_link: str) -> int:
    try:
        from telethon import utils
        # First try direct entity resolution (works if already a member)
        try:
            ent = await client.get_entity(invite_link)
            return utils.get_peer_id(ent)
        except Exception:
            pass
        # Fallback: use invite hash to join and read id
        h = extract_invite_hash(invite_link)
        if not h:
            return 0
        try:
            invite = await client(CheckChatInviteRequest(h))
            # ChatInviteAlready or ChatInvite
            if hasattr(invite, "chat"):
                return utils.get_peer_id(invite.chat)
        except Exception:
            pass
            
        try:
            res = await client(ImportChatInviteRequest(h))
            chats = getattr(res, "chats", []) or []
            if chats:
                return utils.get_peer_id(chats[0])
        except UserAlreadyParticipantError:
            # If we are already a participant, we can search the dialogs for this channel
            # but that's slow. Let's try to get entity from hash
            try:
                # Some invite links resolve directly to channel if we are members
                ent = await client.get_entity(invite_link)
                return utils.get_peer_id(ent)
            except Exception:
                pass
        except Exception:
            pass
        return 0
    except Exception:
        return 0

async def has_admin_rights(client: TelegramClient, channel_id: int) -> bool:
    try:
        test = await client.send_message(channel_id, "[Curator] Permission check")
        ok = True
        try:
            # We don't need to check pinning anymore as we've disabled it,
            # but we'll keep the check for generic admin rights without pinning.
            pass
        except Exception:
            ok = False
        try:
            await client.delete_messages(channel_id, [test.id])
        except Exception:
            pass
        return ok
    except (ChatWriteForbiddenError, ChannelPrivateError):
        return False
    except Exception:
        return False

def assign_group_hashtag(title: str) -> str:
    t = (title or "").lower()
    if any(k in t for k in ["buffering", "error", "401", "dns", "blocked", "portal", "login", "connection", "freeze", "stutter"]):
        return "#AuraFix"
    if any(k in t for k in ["update", "version", "release", "changelog", "new apk", "v5.0", "v6.0"]):
        return "#AuraUpdate"
    if any(k in t for k in ["setup", "tutorial", "how to", "install", "configure", "guide"]):
        return "#AuraGuide"
    if any(k in t for k in ["iptv", "industry", "news", "policy", "legal", "crackdown", "piracy"]):
        return "#AuraNews"
    return "#AuraNews"

if __name__ == "__main__":
    validate_curator_env()
    asyncio.run(curator_loop())
