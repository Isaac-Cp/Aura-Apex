from typing import List, Tuple, Dict, Optional, Any, Union
import asyncio
import os
import re
import time
import json
import datetime
import io
from urllib.parse import urljoin

import aiohttp
import ssl
import certifi
import logging
from aura_core import setup_logging
setup_logging()
logger = logging.getLogger(__name__)
from bs4 import BeautifulSoup
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError, PeerFloodError, ChatWriteForbiddenError, ChannelPrivateError
from telethon.tl.functions.messages import CheckChatInviteRequest, ImportChatInviteRequest
# Forum topic APIs vary across Telethon versions; dynamic mapping disabled for compatibility
from groq import AsyncGroq
from PIL import Image, ImageDraw, ImageFont

from config import (
    API_ID, API_HASH, SESSION_STRING, GROQ_API_KEY, CURATOR_CHANNEL_ID, JUNK_KEYWORDS,
    REQUEST_TIMEOUT, CHECK_INTERVAL_SECONDS, BRAND_COLORS, PLATFORM_SPECS, PRO_TIPS
)

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
HEADERS = {"User-Agent": USER_AGENT}

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

ai_client = AsyncGroq(api_key=GROQ_API_KEY) if GROQ_API_KEY else None

IPTV_FILTER_KEYWORDS = [
    "iptv", "tivimate", "smarters", "apk", "firestick",
    "buffering", "dns", "m3u", "rebrand", "player", "epg",
    "xtream codes", "hardcoded", "ott navigator", "implayer", "sparkle",
    "nvidia shield", "formuler", "mag box", "android tv",
    "reseller", "panel", "credits", "white label", "billing",
    "streaming", "update", "firmware", "setup", "tutorial"
]

STRICT_IPTV_KEYWORDS = ["iptv", "firestick", "tivimate", "smarters", "streaming", "buffering", "dns", "rebrand"]
COMPETITOR_TERMS = ["top 10 providers", "best sellers", "best providers", "top sellers", "alternative providers", "apollo", "xtream", "stb", "stbemu", "apollo group tv", "apollo group"]
THREAD_MAP = {"#AuraNews": 9, "#AuraGuide": 4, "#AuraFix": 2, "#AuraUpdate": 3}
DYNAMIC_THREAD_MAP = {}

GUIDE_KEYWORDS = ["tivimate", "smarters", "ibo", "ott", "m3u", "setup", "tutorial", "how to"]
FIX_KEYWORDS = ["isp", "throttling", "vpn", "handshake", "error", "fix", "buffering"]
NEWS_KEYWORDS = ["crackdown", "news", "update", "new channels", "broadcasting", "global"]
TOPIC_MAP = {
    "guide": {"hashtag": "#AuraGuide", "thread": 4, "tags": ["#AuraGuide", "#ApexTutorial"]},
    "fix": {"hashtag": "#AuraFix", "thread": 2, "tags": ["#AuraFix", "#ApexFix"]},
    "news": {"hashtag": "#AuraNews", "thread": 9, "tags": ["#AuraNews", "#ApexNews"]}
}

POST_IMAGE_CACHE = {}
IMAGE_GEN_ENDPOINT = os.environ.get("IMAGE_GEN_ENDPOINT", "").strip()
IMAGE_GEN_API_KEY = os.environ.get("IMAGE_GEN_API_KEY", "").strip()
IMAGE_GEN_MODEL = os.environ.get("IMAGE_GEN_MODEL", "sdxl").strip()
ai_client = None
if (os.environ.get("GROQ_API_KEY") or "").strip():
    try:
        ai_client = AsyncGroq(api_key=os.environ.get("GROQ_API_KEY").strip())
    except Exception as _e:
        ai_client = None

def classify_topic(title: str, description: str = "") -> Dict[str, Any]:
    s = ((title or "") + " " + (description or "")).lower()
    if any(k in s for k in GUIDE_KEYWORDS):
        m = TOPIC_MAP["guide"]
        return {"topic": "guide", "hashtag": m["hashtag"], "thread": m["thread"], "tags": m["tags"]}
    if any(k in s for k in FIX_KEYWORDS):
        m = TOPIC_MAP["fix"]
        return {"topic": "fix", "hashtag": m["hashtag"], "thread": m["thread"], "tags": m["tags"]}
    if any(k in s for k in NEWS_KEYWORDS):
        m = TOPIC_MAP["news"]
        return {"topic": "news", "hashtag": m["hashtag"], "thread": m["thread"], "tags": m["tags"]}
    h = assign_group_hashtag(title)
    th = THREAD_MAP.get(h) or THREAD_MAP.get("#AuraNews")
    return {"topic": "news", "hashtag": h, "thread": th, "tags": ["#AuraNews", "#ApexNews"]}

def is_high_value_topic(title: str) -> bool:
    t = (title or "").lower()
    hv = [
        "tivimate vs smarters pro",
        "isp throttling in uk/usa",
        "optimizing iptv buffer size for 4k playback",
        "setup guide: m3u vs stalker portal"
    ]
    return any(p in t for p in hv)

def get_pro_tip_for_topic(topic: str) -> str:
    import random
    if topic == "guide":
        c = [
            "Large buffer size reduces jitter on 4K streams.",
            "Favor Software decoder when hardware acceleration stutters.",
            "Keep EPG refresh at 24h for smoother navigation."
        ]
        return f"💡 Aiden’s Pro-Tip: {random.choice(c)}"
    if topic == "fix":
        c = [
            "Switch VPN to TCP on match days for stable handshakes.",
            "Pin DNS to 1.1.1.1 or 8.8.8.8 to stop portal flaps.",
            "Reduce reconnect loops by lowering retry aggressiveness."
        ]
        return f"💡 Aiden’s Pro-Tip: {random.choice(c)}"
    c = [
        "Stick with Aura Apex for secure, consistent streaming.",
        "Own hardware beats resellers when services flap.",
        "Hardened APKs avoid third‑party scanner noise."
    ]
    return f"💡 Aiden’s Pro-Tip: {random.choice(c)}"

def is_iptv_content(title: str, description: str = "") -> bool:
    combined = ((title or "") + " " + (description or "")).lower()
    return any(k in combined for k in IPTV_FILTER_KEYWORDS)

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
def _build_tags(title: str) -> List[str]:
    t = (title or "").lower()
    extra = []
    if "firestick" in t:
        extra.append("#Firestick")
    if "tivimate" in t:
        extra.append("#TiviMate")
    if "dns" in t:
        extra.append("#DNS")
    if "buffer" in t:
        extra.append("#NoBuffering")
    if "rebrand" in t or "white label" in t:
        extra.append("#WhiteLabelIPTV")
    if "stream" in t:
        extra.append("#Streaming")
    out = []
    for tag in extra:
        if tag not in out:
            out.append(tag)
        if len(out) >= 2:
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
        base = f"Create an informative, clean promo image related to '{title}'. Topic: {topic}. Style: modern tech, high contrast, legible typography. No faces."
        if not ai_client:
            return base
        sys_prompt = "You generate concise image briefs. Return one line describing subject, elements, colors, and mood."
        user = f"Title: {title}\nBody: {body[:240]}\nTopic: {topic}\nConstraints: No logos, no trademarks, no faces, tech‑centric."
        try:
            resp = await ai_client.chat.completions.create(
                model="llama-3.1-8b-instant",
                messages=[{"role": "system", "content": sys_prompt}, {"role": "user", "content": user}],
                temperature=0.5
            )
            txt = (resp.choices[0].message.content or "").strip()
            return txt or base
        except Exception:
            return base
    except Exception:
        return f"{title} — clean tech poster, minimal, dark background, neon accent"

async def _ai_generate_image(prompt: str, size: Tuple[int, int] = (1280, 720)) -> Optional[bytes]:
    try:
        if not IMAGE_GEN_ENDPOINT:
            return None
        payload = {"prompt": prompt, "model": IMAGE_GEN_MODEL, "width": size[0], "height": size[1]}
        headers = {"Content-Type": "application/json"}
        if IMAGE_GEN_API_KEY:
            headers["Authorization"] = f"Bearer {IMAGE_GEN_API_KEY}"
        ssl_ctx = ssl.create_default_context(cafile=certifi.where())
        async with aiohttp.ClientSession(headers=headers) as session:
            for _ in range(2):
                try:
                    async with session.post(IMAGE_GEN_ENDPOINT, json=payload, timeout=60, ssl=ssl_ctx) as r:
                        if r.status == 200:
                            ct = r.headers.get("Content-Type", "")
                            if "application/json" in ct:
                                data = await r.json()
                                b64 = (data.get("image_base64") or "").strip()
                                if not b64:
                                    return None
                                import base64
                                raw = base64.b64decode(b64)
                                return raw
                            else:
                                raw = await r.read()
                                return raw if raw else None
                except Exception:
                    await asyncio.sleep(1.0)
        return None
    except Exception:
        return None

async def generate_curator_image_async(title: str, body: str, topic: str) -> Optional[bytes]:
    try:
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
    return re.findall(r'https?://\S+', text)

_session: Optional[aiohttp.ClientSession] = None

async def get_session() -> aiohttp.ClientSession:
    global _session
    if _session is None or _session.closed:
        ssl_context = ssl.create_default_context(cafile=certifi.where())
        _session = aiohttp.ClientSession(headers=HEADERS, connector=aiohttp.TCPConnector(ssl=ssl_context))
    return _session

async def get_html(url: str) -> str:
    try:
        session = await get_session()
        async with session.get(url, timeout=REQUEST_TIMEOUT) as r:
            if r.status == 200:
                return await r.text()
        return ""
    except Exception as e:
        logger.error(f"Request failed for {url}: {e}")
        return ""

async def generate_4k_image_async(title: str, concept_text: str = "") -> Optional[bytes]:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, generate_4k_image, title, concept_text)


async def scrape_troypoint() -> List[Tuple[str, str, str]]:
    try:
        html = await get_html("https://troypoint.com/category/tutorials/")
        if not html:
            logger.error("Aura Radar: Source blocked (TroyPoint). Shifting to secondary nodes.")
            return []
        soup = BeautifulSoup(html, "html.parser")
        items = []
        
        # Generic scraper that looks for articles or posts
        articles = soup.select("article") or soup.select("div.post") or soup.select("div.entry") or soup.select("div.type-post") or soup.select("div.blog-post")
        
        for art in articles:
            # Try to find a link inside an h2 or h3 first (common for titles)
            a = art.select_one("h2 a") or art.select_one("h3 a")
            
            # Fallback: find the first link with an href
            if not a:
                a = art.find("a", href=True)
            
            if not a:
                continue
                
            title = (a.get_text() or "").strip()
            link = (a.get("href") or "").strip()
            
            if link and is_iptv_content(title):
                items.append((title, link, "TROYPOINT"))
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
        soup = BeautifulSoup(html, "html.parser")
        items = []
        articles = soup.find_all("article")
        if not articles:
             # Fallback: look for generic divs with class post or similar
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
             # Generic fallback
             a = art.find("a", href=True)
        if not a:
            continue
        title = (a.get_text() or "").strip()
        link = (a.get("href") or "").strip()
        if link and is_iptv_content(title):
            items.append((title, link, "AFTVnews"))
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
             # Generic fallback
             a = art.find("a", href=True)
        if not a:
            continue
        title = (a.get_text() or "").strip()
        link = (a.get("href") or "").strip()
        if link and is_iptv_content(title):
            items.append((title, link, "TorrentFreak"))
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
    import random
    t = (title or "").lower()
    opener = random.choice(["Quick alert—", "Heads up—", "Just caught this—"])
    problem = "Bitrate caps and ISP throttling"
    guidance = [
        "Switch decoder profiles when OS updates disrupt rendering.",
        "Pin DNS and reduce reconnect loops to stabilize the handshake."
    ]
    if any(k in t for k in ["buffer", "stutter", "frame drop"]):
        problem = "Hardware acceleration conflicting with OS video path"
        guidance = ["Use Software decoder for this device class.", "Clear cache partition beyond app-level cache."]
    elif any(k in t for k in ["dns", "blocked", "portal", "login", "handshake"]):
        problem = "DNS leakage and unstable portal authentication"
        guidance = ["Lock DNS to 1.1.1.1 or 8.8.8.8.", "Verify portal endpoint stability and token refresh."]
    elif any(k in t for k in ["m3u", "playlist", "epg"]):
        problem = "Aggressive EPG refresh and playlist endpoint throttling"
        guidance = ["Stretch EPG refresh to 24h.", "Confirm playlist token/session longevity."]
    elif any(k in t for k in ["xtream", "mac", "stalker"]):
        problem = "Account model mismatch and server-side rate limits"
        guidance = ["Validate credentials against the correct auth model.", "Reduce retries during peak hours."]
    p1 = f"{opener} regarding {title}. This piece is relevant to IPTV stability and user-side reliability."
    p2 = f"The core issue here is {problem}. Understanding this helps avoid the usual ‘clear cache’ advice that doesn’t fix the underlying transport and auth constraints."
    p3 = f"To address it, apply disciplined configuration: {guidance[0]} {guidance[1]}"
    p4 = "After adjustment, monitor for fewer reconnect flaps, smoother playback, and consistent portal logins across device restarts."
    text = f"{p1}\n\n{p2}\n\n{p3}\n\n{p4}"
    text = re.sub(r'(https?://\S+|t\.me/\S+)', '', text)
    if len(text) > 1200:
        text = text[:1200]
    return text

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
    l = (link or "").lower()
    return ("copyright" in l) or ("infring" in l)

async def maybe_send_daily_audit(client: TelegramClient) -> None:
    global last_audit_date, risky_skipped_today, educational_rephrased_today
    try:
        today = datetime.datetime.now(datetime.timezone.utc).date()
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
                    await client.send_message('me', msg)
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
    tw, th = draw.textsize(tt, font=font_main)
    draw.text((int((w - tw) / 2), int(h * 0.18)), tt, fill=(255, 255, 255), font=font_main)
    sub = "AURA APEX SUPREME"
    sw, sh = draw.textsize(sub, font=font_sub)
    draw.text((int((w - sw) / 2), int(h * 0.18) + th + 30), sub, fill=(255, 215, 0), font=font_sub)
    tag = "#AuraGuide" if topic == "guide" else "#AuraFix" if topic == "fix" else "#AuraNews"
    tw2, th2 = draw.textsize(tag, font=font_sub)
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

def format_instructional_body(title: str, steps: List[str]) -> str:
    if not steps:
        return ""
    parts = []
    parts.append(f"Just caught this—{title}. Actionable walkthrough:")
    for i, s in enumerate(steps, 1):
        parts.append(f"{i}. {s}")
    txt = "\n".join(parts)
    txt = re.sub(r'(https?://\S+|t\.me/\S+)', '', txt)
    if len(txt) > 1400:
        txt = txt[:1400]
    return txt

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
    p = os.path.join("data", "posted_links.json")
    try:
        with open(p, "a", encoding="utf-8") as f:
            f.write(link.strip() + "\n")
    except Exception:
        pass

WELCOME_TAG = "[Aura Welcome]"

async def ensure_welcome_message(client: TelegramClient, channel_id: int) -> None:
    try:
        msgs = await client.get_messages(channel_id, limit=10)
        if any(("Welcome to Aura Apex" in ((getattr(m, "text", "") or ""))) for m in msgs):
            return
        header = "**🖼️ #AuraDemo | Welcome to Aura Apex**"
        body = (
            "Heads up—this channel shares fixes that actually matter: decoder choice, "
            "DNS/portal handshake stability, and sane EPG refresh. "
            "Aiden: we hardcode what users keep breaking, so stability sticks."
        )
        pro_tip_block = "> 💡 **Aiden’s Quick Hit:** Pin DNS and cut reconnect loops to stop handshake flaps."
        contact_line = "📩 @AuraApexSupport_Bot"
        text = f"{header}\n\n{body}\n\n{pro_tip_block}\n\n{contact_line}\n#AuraDemo"
        msg = await client.send_message(channel_id, text)
        try:
            await client.pin_message(channel_id, msg)
        except Exception:
            pass
    except Exception:
        pass

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
    if tag == "#AuraDemo":
        return "🖼️"
    if tag == "#AuraNews":
        return "🛰️"
    return "📡"

async def post_to_channel(client: TelegramClient, channel_id: int, source: str, title: str, link: str):
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
        topic_info = classify_topic(title)
        if topic_info["topic"] == "guide":
            steps, metrics = await extract_instructional_content(link)
            body = format_instructional_body(title, steps) if steps else await humanize_post(source, title, educational=sensitive)
        else:
            body = await humanize_post(source, title, educational=sensitive)
        if sensitive:
            global educational_rephrased_today
            educational_rephrased_today += 1
        hashtag = topic_info["hashtag"]
        top_bar = "🌟━━━━━━━━━━━━━━━🌟"
        title_block = f"🔥**{(title or '').upper()}**🔥"
        pt = get_pro_tip_for_topic(topic_info["topic"])
        pro_tip_block = pt
        add_tags = _build_tags(title)
        base_tags = " ".join(topic_info["tags"])
        tags_block = "#AuraApex #ApexSupreme #IPTVTech " + base_tags + ((" " + " ".join(add_tags)) if add_tags else "")
        text = f"{top_bar}\n{title_block}\n{top_bar}\n\n{body}\n\n{pro_tip_block}\n{top_bar}\n{tags_block}"
        thread_id = topic_info["thread"] or THREAD_MAP.get(hashtag) or THREAD_MAP.get("#AuraNews")
        thread_id = DYNAMIC_THREAD_MAP.get(hashtag, thread_id)
        
        img_bytes = await generate_curator_image_async(title, body, topic_info["topic"])
        
        if img_bytes:
            fname = re.sub(r'[^A-Za-z0-9_\-]+', '_', (title or 'aura_apex'))[:60] + ".jpg"
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
            await client.send_message('me', f"[Curator] Posted: {link}")
        except Exception:
            pass
        append_local_link(link)
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
        today = datetime.datetime.now(datetime.timezone.utc).date()
        c = 0
        for m in msgs:
            dt = getattr(m, "date", None)
            txt = (getattr(m, "text", "") or "")
            if dt and dt.date() == today and ("Source:" in txt):
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
        if today_count >= 3:
            return False
        # Post a soft sale on every 5th curator post
        if (global_count + 1) % 5 == 0:
            header = "**🛠️ #AuraFix | Skip Manual Fixes with Hardcoding**"
            body = (
                "Just caught this—too many guides push DNS tweaks. Fix: hardcode DNS and portal so "
                "users stop manual patches. Aiden: hardcoding kills leakage and handshake flaps."
            )
            pro_tip_block = "> 💡 **Aiden’s Quick Hit:** Lock DNS in‑app; OS updates stop breaking the path."
            contact_line = "📩 @AuraApexSupport_Bot"
            text = f"{header}\n\n{body}\n\n{pro_tip_block}\n\n{contact_line}\n#AuraFix"
            await client.send_message(channel_id, text)
            try:
                await client.send_message('me', "[Curator] SoftSale")
            except Exception:
                pass
            return True
        return False
    except Exception:
        return False

async def curator_loop():
    from config import TARGET_CHANNEL_ID, CHANNEL_INVITE_LINK
    chan_raw = (TARGET_CHANNEL_ID or CURATOR_CHANNEL_ID or "")
    try:
        channel_id = int(chan_raw)
    except Exception:
        channel_id = 0
    session = StringSession(SESSION_STRING) if SESSION_STRING else None
    client = TelegramClient(session or "aura_curator_session", int(API_ID), API_HASH)
    async with client:
        try:
            client.session.save_entities = False
        except Exception:
            pass
        if not channel_id and (CHANNEL_INVITE_LINK or "").strip():
            channel_id = await resolve_channel_id(client, CHANNEL_INVITE_LINK.strip())
            if not channel_id:
                logger.error("Error: Unable to resolve channel ID from invite link.")
                return
        # Permissions check
        admin_ok = await has_admin_rights(client, channel_id)
        if not admin_ok:
            logger.warning("Warning: Bot may lack admin rights; pinning may fail.")
        await ensure_welcome_message(client, channel_id)
        try:
            tm = await load_forum_thread_map(client, channel_id)
            if tm:
                DYNAMIC_THREAD_MAP.update(tm)
        except Exception:
            pass
        seen = await load_recent_links(client, channel_id)
        seen |= load_local_links()
        while True:
            try:
                today_count = await count_today_posts(client, channel_id)
                global_count = await total_curator_posts_logged(client)
                if today_count < 3 or os.environ.get("AURA_MODE", "").lower() == "testing":
                    new_items = []
                    new_items.extend(await scrape_troypoint())
                    new_items.extend(await scrape_iptvwire())
                    new_items.extend(await scrape_guru99())
                    new_items.extend(await scrape_aftvnews())
                    new_items.extend(await scrape_torrentfreak())
                    new_items.extend(await scrape_reddit_detailediptv())
                    new_items.extend(await scrape_reddit_tivimate())
                    new_items.extend(await scrape_reddit_iptv())
                    hv = [it for it in new_items if is_high_value_topic(it[0])]
                    rest = [it for it in new_items if not is_high_value_topic(it[0])]
                    new_items = hv + rest
                    def target_topic_now() -> str | None:
                        d = datetime.datetime.now()
                        h = d.hour
                        wd = d.weekday()
                        if 6 <= h <= 11:
                            return "news"
                        if 12 <= h <= 15 and wd in (0, 2, 4):
                            return "guide"
                        if 18 <= h <= 22:
                            return "fix"
                        return None
                    target = target_topic_now()
                    for title, link, source in new_items:
                        if today_count >= 3:
                            break
                        if link in seen and os.environ.get("AURA_MODE", "").lower() != "testing":
                            continue
                        if target and os.environ.get("AURA_MODE", "").lower() != "testing":
                            ci = classify_topic(title)
                            if ci["topic"] != target:
                                continue
                        ok = await post_to_channel(client, channel_id, source, title, link)
                        if ok:
                            seen.add(link)
                            today_count += 1
                            global_count += 1
                # Attempt soft-sale if eligible and under daily cap
                await maybe_post_soft_sale(client, channel_id, today_count, global_count)
                await maybe_send_daily_audit(client)
                await asyncio.sleep(CHECK_INTERVAL_SECONDS)
            except Exception:
                await asyncio.sleep(300)

def extract_invite_hash(invite_link: str) -> str:
    if not invite_link:
        return ""
    m = re.search(r't\.me/\+([A-Za-z0-9_\-]+)', invite_link)
    if m:
        return m.group(1)
    m = re.search(r'joinchat/([A-Za-z0-9_\-]+)', invite_link)
    if m:
        return m.group(1)
    return ""

async def resolve_channel_id(client: TelegramClient, invite_link: str) -> int:
    try:
        # First try direct entity resolution (works if already a member)
        try:
            ent = await client.get_entity(invite_link)
            cid = int(getattr(ent, "id", 0) or 0)
            if cid:
                return cid
        except Exception:
            pass
        # Fallback: use invite hash to join and read id
        h = extract_invite_hash(invite_link)
        if not h:
            return 0
        try:
            await client(CheckChatInviteRequest(h))
        except Exception:
            pass
        res = await client(ImportChatInviteRequest(h))
        chats = getattr(res, "chats", []) or []
        if chats:
            chat = chats[0]
            cid = int(getattr(chat, "id", 0) or 0)
            if cid:
                return cid
        return 0
    except Exception:
        return 0

async def has_admin_rights(client: TelegramClient, channel_id: int) -> bool:
    try:
        test = await client.send_message(channel_id, "[Curator] Permission check")
        ok = True
        try:
            await client.pin_message(channel_id, test)
            await client.unpin_message(channel_id, test)
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
    if any(k in t for k in ["rebrand", "white label", "hardcoded dns", "showcase", "branding", "demo"]):
        return "#AuraDemo"
    if any(k in t for k in ["iptv", "industry", "news", "policy", "legal", "crackdown", "piracy"]):
        return "#AuraNews"
    return "#AuraNews"

if __name__ == "__main__":
    asyncio.run(curator_loop())
