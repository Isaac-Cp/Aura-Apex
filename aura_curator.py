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
    "buffering", "dns", "m3u", "rebrand", "player", "epg"
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
        f"Rewrite this headline for IPTV specialists: {title}. "
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

async def post_to_channel(client: TelegramClient, channel_id: int, source: str, title: str, link: str):
    try:
        body = await humanize_post(source, title)
        suffix = f"\nSource: {source} | {link}"
        text = f"{body}{suffix}"
        await client.send_message(channel_id, text)
        try:
            await client.send_message('me', f"[Curator] Posted: {link}")
        except Exception:
            pass
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
        seen = await load_recent_links(client, channel_id)
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
