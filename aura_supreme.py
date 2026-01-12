
import asyncio
import logging
import logging.handlers
import random
import sys
import datetime
import json
import os
import re
from telethon import TelegramClient, events, functions, errors
from telethon.tl.types import User, Channel, Chat, InputPeerChannel
from telethon.tl.functions.messages import CheckChatInviteRequest, GetHistoryRequest, GetFullChatRequest
from telethon.tl.functions.channels import GetFullChannelRequest, JoinChannelRequest, LeaveChannelRequest
from telethon.tl.functions.account import UpdateNotifySettingsRequest
from telethon.tl.types import InputNotifyPeer, InputPeerNotifySettings, ReactionEmoji
from deep_translator import GoogleTranslator
try:
    from google import genai as genai_new
    GENAI_PROVIDER = 'new'
except Exception:
    GENAI_PROVIDER = 'old'
    import google.generativeai as genai_old

# Configuration
try:
    from config import API_ID, API_HASH, PHONE_NUMBER, GEMINI_API_KEY
except ImportError:
    print("Error: Could not import config.")
    sys.exit(1)

# Logging
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)
try:
    _fh = logging.handlers.RotatingFileHandler('bot_error.log', maxBytes=1000000, backupCount=3, encoding='utf-8')
    _fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(_fh)
except Exception:
    pass

# Constants
SESSION_NAME = 'aura_supreme_session'
STATS_FILE = 'supreme_stats.json'
PROCESSED_GROUPS = 'supreme_groups.json'
PROCESSED_LEADS = 'supreme_leads.json'

SCOUT_BOTS = ['@SearchXBot', '@NiceHubBot', '@enSearcBot', '@TgFinderBot', '@Ossint_group_searh']

# Negative Keyword Shield (Junk Filter)
NEGATIVE_KEYWORDS = [
    'binning', 'carding', 'cc', 'clone', 'logs', 'refund', 'cashout', 'fraud',
    'crypto', 'bitcoin', 'forex', 'trading', 'casino', 'gambling', 'nude', 'adult', 'dating',
    'free iptv', 'm3u daily', 'vlc playlist', 'cracked', 'hacked',
    'test group', 'channel for sale', 'promotion only', 'spam here'
]

# Urgency Keywords (Golden Lead)
URGENCY_KEYWORDS = ['asap', 'help now', 'match starting', 'provider gone', 'urgent', 'now']

# Aura Supreme Keyword Matrix
KEYWORD_MATRIX = {
    "Layer A: Server & Panel Specifics": [
        "Strong IPTV panel", "B1G IPTV server", "GAY IPTV reseller", 
        "Xtreme HD panel help", "Treaxit server status", "Dino IPTV support"
    ],
    "Layer B: Regional Branding": [
        "Apollo Group TV UK", "Crystal OTT USA", "Magnum OTT Canada", 
        "XCodes IPTV review", "JBNOTT setup", "SmartiFlix community"
    ],
    "Layer C: Technical Buy-Ready": [
        "Looking for Xtream Codes login", "M3U playlist UK 2026", 
        "EPG source fix", "IPTV Antifreeze tech", "Private server invite IPTV"
    ]
}

# Stats Template
supreme_stats = {
    "bots_queried": 0,
    "keywords_used": [],
    "rich_joined": 0,
    "trash_rejected": 0,
    "dms_sent": 0,
    "golden_leads": 0,
    "top_keyword": "None",
    "last_report": str(datetime.datetime.now())
}

client = TelegramClient(SESSION_NAME, API_ID, API_HASH)

if GEMINI_API_KEY and "your_gemini_api_key_here" not in GEMINI_API_KEY:
    if GENAI_PROVIDER == 'new':
        try:
            ai_client = genai_new.Client(api_key=GEMINI_API_KEY)
            aura_model = None
        except Exception:
            ai_client = None
            aura_model = None
    else:
        try:
            genai_old.configure(api_key=GEMINI_API_KEY)
            aura_model = genai_old.GenerativeModel('gemini-pro')
            ai_client = None
        except Exception:
            ai_client = None
            aura_model = None
else:
    aura_model = None
    ai_client = None

# --- Helpers ---

def load_json(path, default):
    if os.path.exists(path):
        try:
            with open(path, 'r', encoding='utf-8') as f: return json.load(f)
        except: return default
    return default

def save_json(path, data):
    with open(path, 'w', encoding='utf-8') as f: json.dump(data, f)

def is_prime_time():
    """Check if current local time is between 18:00 and 23:00."""
    now = datetime.datetime.now()
    return 18 <= now.hour <= 23

# --- Supreme Gatekeeper Logic ---

async def supreme_gatekeeper(chat_link):
    """Enhanced 5-Bot Wealth Gatekeeper with Negative Keyword Shield."""
    try:
        # Step 1: Metadata & Negative Keyword Shield
        invite_hash = chat_link.split('/')[-1]
        try:
            invite = await client(CheckChatInviteRequest(invite_hash))
            chat_obj = getattr(invite, 'chat', invite)
            
            title = getattr(chat_obj, 'title', '').lower()
            about = getattr(chat_obj, 'about', '').lower() if hasattr(chat_obj, 'about') else ''
            
            # Junk Filter
            if any(k in title or k in about for k in NEGATIVE_KEYWORDS):
                logger.info(f"Rejected: Negative Keyword found in Title/Bio for {chat_link}")
                return False, "Negative Keyword Shield (Junk Filter)"

            members = getattr(chat_obj, 'participants_count', 0)
            if members < 1200:
                logger.info(f"Rejected: Small Group ({members} members)")
                return False, f"Low Members ({members})"
        except:
             pass # Continue if invite check fails (might be private or restricted)

        # Step 2: Join & Technical Analysis
        try:
            join_result = await client(JoinChannelRequest(chat_link))
            chat_id = join_result.chats[0].id
        except Exception as e:
            logger.error(f"Join failed: {e}")
            return False, "Join Error"

        # Step 3: Infrastructure Scan (Technical Heatmap)
        messages = await client.get_messages(chat_id, limit=50)
        tech_keywords = ['mac address', 'dns', 'portal', 'xtream', 'm3u', 'mag', 'stalker', 'url', 'host']
        tech_hits = 0
        ad_count = 0
        consecutive_ads = 0
        
        for msg in messages:
            if not msg.text: continue
            text_lower = msg.text.lower()
            
            # Count tech keywords
            if any(k in text_lower for k in tech_keywords):
                tech_hits += 1
            
            # Ad Bot Filter
            is_ad = any(k in text_lower for k in ['buy now', 'cheap', 'best price', 'trial link', 'support 24/7'])
            if is_ad:
                ad_count += 1
                consecutive_ads += 1
                if consecutive_ads >= 5:
                    await client(LeaveChannelRequest(chat_id))
                    return False, "Spam Bot Group (Consecutive Ads)"
            else:
                consecutive_ads = 0

        logger.info(f"Supreme Scan: {tech_hits} tech hits in {len(messages)} msgs")
        
        if tech_hits < 5:
            await client(LeaveChannelRequest(chat_id))
            return False, "Non-Technical Group (Low Heatmap)"
            
        # Mute Immediately
        try:
            await client(UpdateNotifySettingsRequest(
                peer=InputNotifyPeer(peer=await client.get_input_entity(chat_id)),
                settings=InputPeerNotifySettings(mute_until=2147483647)
            ))
        except: pass
        
        # Smart-React Stealth Protocol (Initial)
        await smart_react(chat_id)
        
        return True, "Supreme Rich Group"

    except Exception as e:
        logger.error(f"Supreme Gatekeeper failed: {e}")
        return False, "Analysis Error"

async def smart_react(chat_id):
    """React to messages to build account trust score."""
    try:
        messages = await client.get_messages(chat_id, limit=10)
        valid_msgs = [m for m in messages if not m.out and m.text]
        if valid_msgs:
            target = random.choice(valid_msgs)
            reaction = random.choice(["\U0001F44D", "\U0001F525"]) 
            await client(functions.messages.SendReactionRequest(
                peer=chat_id,
                msg_id=target.id,
                reaction=[ReactionEmoji(emoticon=reaction)]
            ))
            logger.info(f"Smart-React: Interaction recorded in {chat_id}")
    except: pass

# --- Scouting System ---

@client.on(events.NewMessage(from_users=SCOUT_BOTS))
async def handle_scout_reply(event):
    text = event.raw_text
    links = re.findall(r't\.me/(?:joinchat/|)\S+', text)
    if not links: return

    stats = load_json(STATS_FILE, supreme_stats)
    groups = load_json(PROCESSED_GROUPS, [])

    for link in links:
        link = "https://" + link if not link.startswith('http') else link
        if link in groups: continue
        
        groups.append(link)
        save_json(PROCESSED_GROUPS, groups)
        
        is_supreme, reason = await supreme_gatekeeper(link)
        if is_supreme:
            stats["rich_joined"] += 1
            await client.send_message('me', f"💎 **Aura Supreme: Rich Infra Group**\nGroup: {link}\nStatus: Verified Infrastructure Hub")
        else:
            stats["trash_rejected"] += 1
            
    save_json(STATS_FILE, stats)

async def supreme_rotator():
    """Rotate queries every 6 hours."""
    print("Supreme Rotator: Active")
    while True:
        try:
            stats = load_json(STATS_FILE, supreme_stats)
            
            # Selection
            layer_keys = list(KEYWORD_MATRIX.keys())
            layer = random.choice(layer_keys)
            keyword = random.choice(KEYWORD_MATRIX[layer])
            
            logger.info(f"Supreme Scouting: [{layer}] query: '{keyword}'")
            
            bot = random.choice(SCOUT_BOTS)
            try:
                await client.send_message(bot, keyword)
                stats["bots_queried"] += 1
                stats["keywords_used"].append(keyword)
                save_json(STATS_FILE, stats)
            except: pass

            await asyncio.sleep(21600) # 6 hours
            
        except Exception as e:
            logger.error(f"Rotator Error: {e}")
            await asyncio.sleep(3600)

async def supreme_dashboard():
    """12-hour Status Dashboard."""
    while True:
        await asyncio.sleep(43200) # 12h
        try:
            stats = load_json(STATS_FILE, supreme_stats)
            # Find top keyword today
            from collections import Counter
            top_kw = Counter(stats["keywords_used"]).most_common(1)[0][0] if stats["keywords_used"] else "None"
            
            report = (
                f"🛰️ **Aura Supreme: Infrastructure Stats**\n"
                f"📡 **Bots Queried:** {stats['bots_queried']}\n"
                f"💎 **Rich Infrastructure Joined:** {stats['rich_joined']}\n"
                f"🗑️ **Trash Rejected:** {stats['trash_rejected']}\n"
                f"🔍 **Top Keyword Today:** {top_kw}\n"
                f"📩 **DMs Sent:** {stats['dms_sent']}\n"
                f"🔴 **Golden Leads Escalated:** {stats['golden_leads']}\n"
                f"🕒 **Report Time:** {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}"
            )
            await client.send_message('me', report)
        except: pass

# --- Aiden Supreme Persona Logic ---

@client.on(events.NewMessage(incoming=True))
async def handle_new_lead(event):
    """Handle incoming messages in Rich Groups."""
    if event.is_private: return

    stats = load_json(STATS_FILE, supreme_stats)
    text = event.raw_text
    if not text: return
    text_lower = text.lower()

    # Step 1: Golden Lead Escalation (ASAP/NOW)
    if any(k in text_lower for k in URGENCY_KEYWORDS):
        sender = await event.get_sender()
        chat = await event.get_chat()
        stats["golden_leads"] += 1
        save_json(STATS_FILE, stats)
        
        msg_id = event.id
        if hasattr(chat, 'username') and chat.username:
            link = f"https://t.me/{chat.username}/{msg_id}"
        else:
            link = f"https://t.me/c/{str(chat.id).replace('-100', '')}/{msg_id}"

        alert = (
            f"🔴 **GOLDEN LEAD ESCALATION**\n"
            f"User: `@{sender.username if hasattr(sender, 'username') else sender.id}`\n"
            f"Group: `{chat.title}`\n"
            f"Urgency detected: `{text[:100]}`\n\n"
            f"🔗 **Action:** [REPLY NOW]({link})"
        )
        await client.send_message('me', alert)
        logger.info(f"Golden Lead Escalated: {sender.id}")
        return # Priority alert sent, bypass normal delay

    # Step 2: Time-Zone Synchronization (18:00 - 23:00)
    if not is_prime_time():
        return # Skip outreach during non-buying hours

    # Step 3: Normal Aiden Outreach Logic
    # (Scoring, AI generation, and 24h wait would occur here in a full deployment)
    pass

async def main():
    print("Initializing AURA SUPREME Infrastructure Engine (Phase 15)...")
    async def _start_with_retry():
        retries = 3
        delay = 5
        for _ in range(retries):
            try:
                await client.start(phone=PHONE_NUMBER)
                return
            except Exception as _e:
                if 'database is locked' in str(_e).lower():
                    await asyncio.sleep(delay)
                    continue
                raise
        await asyncio.sleep(delay)
        await client.start(phone=PHONE_NUMBER)
    await _start_with_retry()
    print("💎 Aura Supreme Online. Refined Logic: Junk Shield & Prime-Time Sync Active.")
    
    client.loop.create_task(supreme_rotator())
    client.loop.create_task(supreme_dashboard())
    
    await client.run_until_disconnected()

if __name__ == '__main__':
    try:
        client.loop.run_until_complete(main())
    except KeyboardInterrupt:
        print("\nSupreme Bot Stopped.")
    finally:
        try:
            client.loop.run_until_complete(client.disconnect())
        except Exception:
            pass
