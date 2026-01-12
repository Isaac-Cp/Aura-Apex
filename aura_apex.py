
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
SESSION_NAME = 'aura_apex_session'
STATS_FILE = 'apex_stats.json'
PROCESSED_GROUPS = 'apex_groups.json'
PROCESSED_LEADS = 'apex_leads.json'

SCOUT_BOTS = ['@SearchXBot', '@NiceHubBot', '@enSearcBot', '@TgFinderBot', '@Ossint_group_searh']

KEYWORD_MATRIX = {
    "Layer A (Hardware)": ["Firestick 4K", "Nvidia Shield TV", "Formuler Z11", "Android Box Setup", "Mag Box Help"],
    "Layer B (Apps)": ["Tivimate Premium", "IPTV Smarters Pro", "IBO Player", "OttNavigator", "Perfect Player"],
    "Layer C (Intent)": ["IPTV buffering fix", "Looking for UK Sports", "M3U Playlist 2026", "Test line request", "Service down help"]
}

# Stats Template
apex_stats = {
    "bots_queried": 0,
    "keywords_used": [],
    "rich_joined": 0,
    "trash_rejected": 0,
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

# --- Analysis Logic ---

async def perform_wealth_analysis(chat_link):
    """3-Step Quality Check."""
    try:
        # Step 1: Metadata Peek (Pre-Join)
        # Handle t.me links
        invite_hash = chat_link.split('/')[-1]
        try:
            invite = await client(CheckChatInviteRequest(invite_hash))
            members = getattr(invite, 'chat', invite).participants_count if hasattr(invite, 'chat') else 0
            if members < 800:
                logger.info(f"Rejected: Low membership ({members})")
                return False, "Low Membership"
        except:
             pass # Private link/failed check, try joining anyway if it's a link we want

        # Step 2: Activity Heatmap & Bot Ratio (Post-Join)
        # We need to join to check messages properly in 2026
        try:
            chat = await client(JoinChannelRequest(chat_link))
            if isinstance(chat, functions.messages.Chat):
                chat_id = chat.chats[0]
            else:
                chat_id = chat.chats[0]
        except Exception as e:
            logger.error(f"Join failed: {e}")
            return False, "Join Error"

        # Check last 50 messages
        messages = await client.get_messages(chat_id, limit=50)
        unique_users = set()
        bot_msgs = 0
        ad_keywords = ['offer', 'price', 'buy', 'discount', 'reseller', 'cheap', 'best iptv']
        
        one_day_ago = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=1)
        
        for msg in messages:
            if not msg.text: continue
            if msg.date > one_day_ago:
                unique_users.add(msg.sender_id)
            
            # Simple Bot/Ad Detection
            if any(k in msg.text.lower() for k in ad_keywords):
                bot_msgs += 1
            if getattr(msg.sender, 'bot', False):
                bot_msgs += 1

        # Criteria
        unique_user_count = len(unique_users)
        bot_ratio = bot_msgs / len(messages) if messages else 1
        
        logger.info(f"Analysis: {unique_user_count} unique users, {bot_ratio:.2f} bot ratio")
        
        if unique_user_count < 15:
            await client(LeaveChannelRequest(chat_id))
            return False, "Dead Group"
            
        if bot_ratio > 0.8:
            await client(LeaveChannelRequest(chat_id))
            return False, "Trash/Ad Group"
            
        # Mute Immediately
        try:
            await client(UpdateNotifySettingsRequest(
                peer=InputNotifyPeer(peer=await client.get_input_entity(chat_id)),
                settings=InputPeerNotifySettings(mute_until=2147483647)
            ))
        except: pass
        
        return True, "Rich Group"

    except Exception as e:
        logger.error(f"Wealth analysis failed: {e}")
        return False, "Analysis Error"

# --- Scouting Logic ---

@client.on(events.NewMessage(from_users=SCOUT_BOTS))
async def handle_scout_reply(event):
    """Capture links from scouting bots."""
    text = event.raw_text
    links = re.findall(r't\.me/(?:joinchat/|)\S+', text)
    if not links: return

    logger.info(f"Scout {event.sender_id} found {len(links)} links.")
    
    stats = load_json(STATS_FILE, apex_stats)
    groups = load_json(PROCESSED_GROUPS, [])

    for link in links:
        link = "https://" + link if not link.startswith('http') else link
        if link in groups: continue
        
        groups.append(link)
        save_json(PROCESSED_GROUPS, groups)
        
        is_rich, reason = await perform_wealth_analysis(link)
        if is_rich:
            stats["rich_joined"] += 1
            await client.send_message('me', f"💎 **Aura Apex: Rich Group Found**\nGroup: {link}\nReason: {reason}")
        else:
            stats["trash_rejected"] += 1
            logger.info(f"Rejected group {link}: {reason}")
            
    save_json(STATS_FILE, stats)

async def scouting_rotator():
    """Rotate queries every 8 hours."""
    print("Scouting Rotator: Active")
    while True:
        try:
            stats = load_json(STATS_FILE, apex_stats)
            
            # Select Layer
            layers = list(KEYWORD_MATRIX.keys())
            layer = random.choice(layers)
            keyword = random.choice(KEYWORD_MATRIX[layer])
            
            logger.info(f"Scouting Layer: {layer} | Query: {keyword}")
            
            bot = random.choice(SCOUT_BOTS)
            try:
                await client.send_message(bot, keyword)
                stats["bots_queried"] += 1
                if keyword not in stats["keywords_used"]:
                    stats["keywords_used"].append(keyword)
                save_json(STATS_FILE, stats)
            except Exception as e:
                logger.error(f"Failed to query {bot}: {e}")

            # Wait 8 hours
            await asyncio.sleep(28800)
            
        except Exception as e:
            logger.error(f"Rotator Error: {e}")
            await asyncio.sleep(3600)

async def richness_report():
    """Generate status report every 12h."""
    while True:
        await asyncio.sleep(43200) # 12h
        try:
            stats = load_json(STATS_FILE, apex_stats)
            report = (
                f"📊 **Aura Apex: Richness Report**\n"
                f"📡 **Bots Queried:** {stats['bots_queried']}\n"
                f"🔍 **Keywords Sample:** {', '.join(stats['keywords_used'][-3:])}\n"
                f"💎 **Rich Joined:** {stats['rich_joined']}\n"
                f"🗑️ **Trash Rejected:** {stats['trash_rejected']}\n"
                f"🕒 **Report Time:** {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}"
            )
            await client.send_message('me', report)
        except: pass

async def aiden_apex_miner():
    """Historical scraper for Rich groups (24h delay)."""
    print("Aiden Apex Miner: Active")
    while True:
        try:
            dialogs = await client.get_dialogs(limit=50)
            rich_groups = [d for d in dialogs if d.is_group]
            
            for group in rich_groups:
                # 24h wait: check join date (Telethon doesn't give join date easily, we use apex_groups entry)
                # For now, we allow scanning if we haven't scanned recently.
                
                logger.info(f"Mining Rich Group: {group.title}")
                date_90d = datetime.datetime.now() - datetime.timedelta(days=90)
                
                async for msg in client.iter_messages(group, limit=50, offset_date=date_90d):
                    if not msg.text: continue
                    text_lower = msg.text.lower()
                    
                    if any(k in text_lower for k in ['trial', 'dm me', 'pricing', 'buffering', 'stable']):
                        sender = await msg.get_sender()
                        if not sender or not isinstance(sender, User) or sender.bot: continue
                        
                        # AI Engagement
                        prompt = f"19yo Aiden persona. peer-to-peer. saw user in {group.title} talking about '{msg.text[:50]}'. casual hook about switching to 4k private line. end with question."
                        # (Aura logic from previous versions...)
                        logger.info(f"Apex Lead: {sender.id} in {group.title}")
                
                await asyncio.sleep(300) # per group
            
            await asyncio.sleep(86400) # daily cycle
        except Exception as e:
             logger.error(f"Miner Error: {e}")
             await asyncio.sleep(3600)

async def main():
    print("Initializing AURA APEX Scouting Network...")
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
    print("🔮 Aiden Apex Online. Scouting active.")
    
    client.loop.create_task(scouting_rotator())
    client.loop.create_task(richness_report())
    client.loop.create_task(aiden_apex_miner())
    
    await client.run_until_disconnected()

if __name__ == '__main__':
    try:
        client.loop.run_until_complete(main())
    except KeyboardInterrupt:
        print("\nApex Bot Stopped.")
    finally:
        try:
            client.loop.run_until_complete(client.disconnect())
        except Exception:
            pass
