import asyncio
import os
import re
import time
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
    "You are 'Aiden', a helpful friend sharing a useful find with an IPTV community.\n"
    "Rules:\n"
    "1. Use casual openers like 'Quick one —' or 'Just saw this —'.\n"
    "2. No sales jargon. Keep human and practical.\n"
    "3. Max 50 words. One or two short sentences.\n"
    "4. Reference the source name.\n"
    "5. End with a low-pressure note.\n"
)

ai_client = AsyncGroq(api_key=GROQ_API_KEY) if GROQ_API_KEY else None

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
        if link and "/category/tutorials/" in "https://troypoint.com/category/tutorials/":
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
        if link:
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
        if link:
            items.append((title, link, "TorrentFreak"))
    return items

async def humanize_post(source: str, title: str) -> str:
    base = f"Quick one — {title} ({source}). "
    if not ai_client:
        return base + "worth a look if you’ve hit recent app/login snags."
    user_msg = f"Turn this headline into a helpful, casual tip for IPTV users: {title}. Mention {source}."
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
        if len(text) > 300:
            text = text[:300]
        return text
    except Exception:
        return base + "worth a look if you’ve hit recent app/login snags."

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
                new_items = []
                new_items.extend(scrape_troypoint())
                new_items.extend(scrape_aftvnews())
                new_items.extend(scrape_torrentfreak())
                for title, link, source in new_items:
                    if link in seen:
                        continue
                    ok = await post_to_channel(client, channel_id, source, title, link)
                    if ok:
                        seen.add(link)
                await asyncio.sleep(CHECK_INTERVAL_SECONDS)
            except Exception:
                await asyncio.sleep(300)

if __name__ == "__main__":
    asyncio.run(curator_loop())
