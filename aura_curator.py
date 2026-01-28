import asyncio
import os
import re
import time
import datetime
from typing import List, Tuple
import io
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError, PeerFloodError, ChatWriteForbiddenError, ChannelPrivateError
from telethon.tl.functions.messages import CheckChatInviteRequest, ImportChatInviteRequest
from groq import AsyncGroq
from PIL import Image

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

async def humanize_post(source: str, title: str, educational: bool = False) -> str:
    import random
    t = (title or "").lower()
    opener = random.choice(["Just caught this—", "Heads up—"])
    reason = "bitrate caps and ISP throttling"
    fix = "adjust player decoder and stabilize the portal handshake"
    insight = "lock DNS, refresh EPG sanely, and avoid cache nukes"
    if any(k in t for k in ["buffer", "stutter", "frame drop"]):
        reason = "hardware acceleration fighting the OS handshake"
        fix = "switch decoder to Software for this device profile"
        insight = "hardware modes vary by OS updates; test decoder on 4K"
    elif any(k in t for k in ["dns", "blocked", "portal", "login", "handshake"]):
        reason = "DNS leakage and a flaky portal handshake"
        fix = "pin DNS and confirm portal endpoint stability"
        insight = "avoid public DNS; pick a consistent resolver close to origin"
    elif any(k in t for k in ["m3u", "playlist", "epg"]):
        reason = "playlist endpoint auth and aggressive EPG refresh"
        fix = "reduce EPG refresh and validate token/session longevity"
        insight = "24h EPG refresh avoids provider rate-limit bursts"
    elif any(k in t for k in ["xtream", "mac", "stalker"]):
        reason = "auth model mismatch and server-side throttling"
        fix = "verify account type and cut retries during peak hours"
        insight = "limit reconnect loops; they trigger back-end protections"
    intro = f"{opener}{title}. Most IPTV apps fail here because of {reason}."
    fix_line = f"Fix: {fix}."
    aiden_line = "Aiden: " + (("focus strictly on lawful player/network optimization." if educational else insight) + ".")
    text = f"{intro} {fix_line} {aiden_line}"
    text = re.sub(r'(https?://\S+|t\.me/\S+)', '', text)
    if len(text) > 500:
        text = text[:500]
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

PRO_TIPS = [
    "Switch decoder to Software when OS updates cause stutter.",
    "Use a stable DNS; avoid public resolvers that leak and flap.",
    "Keep EPG refresh at 24h to prevent provider rate-limit spikes.",
    "Prefer wired Ethernet for 4K; Wi‑Fi jitter ruins handshake stability."
]

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

def extract_feature_image_url(article_url: str) -> str:
    try:
        r = requests.get(article_url, headers=HEADERS, timeout=5)
        if r.status_code != 200:
            return ""
        html = r.text
    except Exception:
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
    except Exception:
        return ""
    return ""

def download_image(url: str) -> Image.Image | None:
    try:
        r = requests.get(url, headers=HEADERS, timeout=5)
        if r.status_code != 200:
            return None
        b = io.BytesIO(r.content)
        b.seek(0)
        img = Image.open(b)
        return img
    except Exception:
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

def prepare_photo(article_url: str) -> bytes | None:
    try:
        iu = extract_feature_image_url(article_url)
        if not iu:
            return None
        img = download_image(iu)
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
        sensitive = is_sensitive_url(link)
        if sensitive and not ai_client:
            global risky_skipped_today
            risky_skipped_today += 1
            return False
        body = await humanize_post(source, title, educational=sensitive)
        if sensitive:
            global educational_rephrased_today
            educational_rephrased_today += 1
        hashtag = assign_group_hashtag(title)
        emoji = emoji_for_hashtag(hashtag)
        header = f"**{emoji} {hashtag} | {title}**"
        pt = get_random_pro_tip()
        pro_tip_block = f"> 💡 **Aiden’s Quick Hit:** {pt}"
        link_line = f"🔗 Full Walkthrough: {link}"
        text = f"{header}\n\n{body}\n\n{pro_tip_block}\n\n{link_line}\n{hashtag}"
        photo_bytes = prepare_photo(link)
        if photo_bytes:
            await client.send_file(channel_id, file=io.BytesIO(photo_bytes), caption=text)
        else:
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
        if not channel_id and (CHANNEL_INVITE_LINK or "").strip():
            channel_id = await resolve_channel_id(client, CHANNEL_INVITE_LINK.strip())
            if not channel_id:
                print("Error: Unable to resolve channel ID from invite link.")
                return
        # Permissions check
        admin_ok = await has_admin_rights(client, channel_id)
        if not admin_ok:
            print("Warning: Bot may lack admin rights; pinning may fail.")
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
