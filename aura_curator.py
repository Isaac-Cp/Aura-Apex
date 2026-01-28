import asyncio
import os
import re
import time
import datetime
from typing import List, Tuple

import requests
from bs4 import BeautifulSoup
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError, PeerFloodError
from groq import AsyncGroq

from config import API_ID, API_HASH, SESSION_STRING, GROQ_API_KEY, CURATOR_CHANNEL_ID

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
HEADERS = {"User-Agent": USER_AGENT}
REQUEST_TIMEOUT = 30
CHECK_INTERVAL_SECONDS = 6 * 3600

SYSTEM_PROMPT = (
    "Act as Aiden, an expert in IPTV player optimization.\n"
    "When you share a link:\n"
    "- Use a casual opener like 'Quick one —' or 'Just saw this —'.\n"
    "- Keep it under 50 words in 1–2 short sentences.\n"
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
    "reseller", "panel", "credits", "white label", "billing"
]

def is_iptv_content(title: str, description: str = "") -> bool:
    combined = ((title or "") + " " + (description or "")).lower()
    return any(k in combined for k in IPTV_FILTER_KEYWORDS)

def extract_links(text: str) -> List[str]:
    if not text:
        return []
    return re.findall(r'https?://\S+', text)

def get_html(url: str) -> str:
    try:
        r = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        if r.status_code == 200:
            return r.text
        return ""
    except Exception:
        return ""

def scrape_troypoint() -> List[Tuple[str, str, str]]:
    html = get_html("https://troypoint.com/category/tutorials/")
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")
    items = []
    for art in soup.select("article.type-post"):
        a = art.select_one("h2.entry-title a")
        if not a:
            continue
        title = (a.get_text() or "").strip()
        link = (a.get("href") or "").strip()
        if link and is_iptv_content(title):
            items.append((title, link, "TROYPOINT"))
    return items

def scrape_aftvnews() -> List[Tuple[str, str, str]]:
    html = get_html("https://www.aftvnews.com/")
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")
    items = []
    for art in soup.select("div.post-listing article"):
        a = art.select_one("h2.post-title a[rel='bookmark']")
        if not a:
            a = art.select_one("h2.post-title a")
        if not a:
            continue
        title = (a.get_text() or "").strip()
        link = (a.get("href") or "").strip()
        if link and is_iptv_content(title):
            items.append((title, link, "AFTVnews"))
    return items

def scrape_torrentfreak() -> List[Tuple[str, str, str]]:
    html = get_html("https://torrentfreak.com/")
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")
    items = []
    for art in soup.select("article.post"):
        a = art.select_one("h2.entry-title a")
        if not a:
            continue
        title = (a.get_text() or "").strip()
        link = (a.get("href") or "").strip()
        if link and is_iptv_content(title):
            items.append((title, link, "TorrentFreak"))
    return items

def scrape_reddit_detailediptv() -> List[Tuple[str, str, str]]:
    html = get_html("https://old.reddit.com/r/DetailedIPTV/")
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")
    items = []
    for a in soup.select("div#siteTable .thing a.title"):
        title = (a.get_text() or "").strip()
        link = (a.get("href") or "").strip()
        if not link or not title:
            continue
        if is_iptv_content(title):
            # Prefer external links; if relative, build full URL
            if link.startswith("/"):
                link = "https://old.reddit.com" + link
            items.append((title, link, "Reddit r/DetailedIPTV"))
    return items

async def humanize_post(source: str, title: str) -> str:
    base = f"Quick one — {title} ({source}). "
    if not ai_client:
        return base + "pro tip: check player EPG refresh before any cache wipes."
    user_msg = (
        f"Rewrite this for IPTV specialists: {title}. "
        f"Focus on server-side realities like bitrate caps, ISP throttling, and handshake behavior versus router rebooting. "
        f"Explain why this fix beats clearing cache. Add one concrete Pro Tip. Mention {source}."
    )
    try:
        resp = await ai_client.chat.completions.create(
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_msg},
            ],
            model="llama-3.3-70b-versatile",
            temperature=0.7,
            max_tokens=80,
        )
        text = (resp.choices[0].message.content or "").strip()
        text = re.sub(r'(https?://\S+|t\.me/\S+)', '', text)
        if len(text) > 280:
            text = text[:280]
        return text
    except Exception:
        return base + "pro tip: check player EPG refresh before any cache wipes."

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

PRO_TIPS = [
    "Tip: Switch to hardware decoder in player settings to stop 4K stuttering.",
    "Tip: If ISP blocks the portal, try a Netherlands VPN exit for stability.",
    "Tip: Keep EPG refresh at 24h; over-refreshing can trigger provider rate-limits.",
    "Tip: Use wired Ethernet for 4K; Wi‑Fi spikes add jitter even at 5GHz."
]

def get_random_pro_tip() -> str:
    import random
    return "\n\n💡 Aiden's Pro-Tip: " + random.choice(PRO_TIPS)

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
WELCOME_MESSAGE = (
    WELCOME_TAG + " Quick one — this channel shares IPTV fixes that actually matter "
    "(player decoder, DNS/portal handshake, EPG sync). "
    "Tired of patching? Aura Apex rebranding hardcodes stability into your APK, "
    "so users stop fiddling with settings. DM for a demo and pricing."
)

async def ensure_welcome_message(client: TelegramClient, channel_id: int) -> None:
    try:
        msgs = await client.get_messages(channel_id, limit=10)
        if any(((getattr(m, "text", "") or "").startswith(WELCOME_TAG)) for m in msgs):
            return
        msg = await client.send_message(channel_id, WELCOME_MESSAGE)
        try:
            await client.pin_message(channel_id, msg)
        except Exception:
            pass
    except Exception:
        pass

async def post_to_channel(client: TelegramClient, channel_id: int, source: str, title: str, link: str):
    try:
        body = await humanize_post(source, title)
        tip = get_random_pro_tip()
        suffix = f"\n\n🔗 Source: {source} | {link}"
        text = f"{body}{tip}{suffix}"
        await client.send_message(channel_id, text)
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
    except Exception:
        return False

async def count_today_posts(client: TelegramClient, channel_id: int) -> int:
    try:
        msgs = await client.get_messages(channel_id, limit=100)
        today = datetime.datetime.utcnow().date()
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
            msg = (
                "Just saw a flood of DNS workaround guides — annoying, right? "
                "Pro Tip: Aura Apex rebranding hardcodes DNS and portal tweaks into the APK, "
                "so users skip fixes entirely. DM for a demo."
            )
            await client.send_message(channel_id, msg)
            try:
                await client.send_message('me', "[Curator] SoftSale")
            except Exception:
                pass
            return True
        return False
    except Exception:
        return False

async def curator_loop():
    chan_raw = CURATOR_CHANNEL_ID or ""
    try:
        channel_id = int(chan_raw)
    except Exception:
        channel_id = 0
    if not channel_id:
        return
    session = StringSession(SESSION_STRING) if SESSION_STRING else None
    client = TelegramClient(session or "aura_curator_session", int(API_ID), API_HASH)
    async with client:
        await ensure_welcome_message(client, channel_id)
        seen = await load_recent_links(client, channel_id)
        seen |= load_local_links()
        while True:
            try:
                today_count = await count_today_posts(client, channel_id)
                global_count = await total_curator_posts_logged(client)
                if today_count < 3:
                    new_items = []
                    new_items.extend(scrape_troypoint())
                    new_items.extend(scrape_aftvnews())
                    new_items.extend(scrape_torrentfreak())
                    new_items.extend(scrape_reddit_detailediptv())
                    new_items.extend(scrape_reddit_tivimate())
                    new_items.extend(scrape_reddit_iptv())
                    for title, link, source in new_items:
                        if today_count >= 3:
                            break
                        if link in seen:
                            continue
                        ok = await post_to_channel(client, channel_id, source, title, link)
                        if ok:
                            seen.add(link)
                            today_count += 1
                            global_count += 1
                # Attempt soft-sale if eligible and under daily cap
                await maybe_post_soft_sale(client, channel_id, today_count, global_count)
                await asyncio.sleep(CHECK_INTERVAL_SECONDS)
            except Exception:
                await asyncio.sleep(300)

if __name__ == "__main__":
    asyncio.run(curator_loop())
