
import asyncio
import logging
import logging.handlers
import random
import sys
import datetime
import json
import os
import csv
from telethon import TelegramClient, events, functions, errors
from telethon.tl.types import User, Channel, Chat, InputPeerChannel
from telethon.tl.functions.account import UpdateNotifySettingsRequest
from telethon.tl.types import InputNotifyPeer, InputPeerNotifySettings, ReactionEmoji
from deep_translator import GoogleTranslator
try:
    from google import genai as genai_new
    GENAI_PROVIDER = 'new'
except Exception:
    GENAI_PROVIDER = 'old'
    import google.generativeai as genai_old

# Import configuration
try:
    from config import API_ID, API_HASH, PHONE_NUMBER, GEMINI_API_KEY
except ImportError:
    print("Error: Could not import config. Make sure config.py and .env represent a valid configuration.")
    sys.exit(1)

# Logging Setup
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)
try:
    _fh = logging.handlers.RotatingFileHandler('bot_error.log', maxBytes=1000000, backupCount=3, encoding='utf-8')
    _fh.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(_fh)
except Exception:
    pass

# Constants
SESSION_NAME = 'aura_pro_elite'
STATS_FILE = 'bot_stats.json'
PROCESSED_LEADS_FILE = 'processed_leads.json'
CRM_FILE = 'crm_leads.csv'

# AI Persona Settings
PERSONA_NAME = "Aiden"
PERSONA_AGE = 19
PERSONA_STYLE = "19yo tech enthusiast, casual, lowercase, empathetic, peer-to-peer."

# --- 1. Intent-Scoring Engine ---
HIGH_INTENT = ['looking for', 'buying', 'trial', 'test line', 'recommend', 'subscription', 'provider link', 'service down', '24h trial', 'asap', 'today', 'right now']
CONTEXT_KEYWORDS = ['iptv', 'firestick', 'tivimate', 'smarters', 'freeze', 'buffering', 'm3u', 'xtream', 'mag box', 'nvidia shield', 'formuler', 'uk streaming', 'usa channels', '4k sports', 'ppv']
BLACKLIST = ['sell', 'restock', 'cheap price', 'dm for panel', 'reseller account', 'no buffering guarantee', '24/7 support', 'free giveaway', 'join my channel', 'discount code', 'best price', 'unlimited connections', 'instantly', 'automated', 'shop link']

# Historical Keywords
HISTORICAL_KEYWORDS = ['freezing', 'down', 'expired', 'help', 'recommend', 'iptv']

# Search Terms
SEARCH_TERMS = ["TiviMate Premium", "Smarters Pro Support", "IBO Player", "OttNavigator", "Firestick 4K Max", "Nvidia Shield TV", "UK Streaming Support", "USA Cord Cutters", "Premier League Live", "UFC PPV", "M3U Troubleshooting"]

# Initialize Client
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

# --- Persistence & Stats ---

def load_json(filename, default):
    if os.path.exists(filename):
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            return default
    return default

def save_json(filename, data):
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f)

def get_limits():
    """Warm-up Protocol: Week 1 vs Week 2+"""
    stats = load_json(STATS_FILE, {"start_date": str(datetime.date.today()), "joins": 0})
    start_date = datetime.datetime.strptime(stats.get("start_date", str(datetime.date.today())), "%Y-%m-%d").date()
    days_active = (datetime.date.today() - start_date).days
    
    if days_active < 7:
        return {"max_joins": 3, "max_dms": 5, "interval": (5400, 7200)} # Week 1
    else:
        return {"max_joins": 12, "max_dms": 25, "interval": (5400, 6300)} # Week 2+ (Elite)

def record_dm_sent():
    stats = load_json(STATS_FILE, {"joins": 0, "dms_today": 0, "date": str(datetime.date.today())})
    if stats.get("date") != str(datetime.date.today()):
        stats["dms_today"] = 0
        stats["date"] = str(datetime.date.today())
    stats["dms_today"] = stats.get("dms_today", 0) + 1
    save_json(STATS_FILE, stats)

def is_processed(user_id):
    processed = load_json(PROCESSED_LEADS_FILE, {})
    return str(user_id) in processed

def mark_processed(user_id, reason="dm_sent"):
    processed = load_json(PROCESSED_LEADS_FILE, {})
    processed[str(user_id)] = {"date": str(datetime.date.today()), "reason": reason}
    save_json(PROCESSED_LEADS_FILE, processed)

def log_to_crm(user_info):
    file_exists = os.path.isfile(CRM_FILE)
    with open(CRM_FILE, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=["username", "user_id", "group", "problem", "date"])
        if not file_exists:
            writer.writeheader()
        writer.writerow(user_info)

# --- Core Logic Functions ---

async def generate_aura_pro_msg(group_name, problem, original_text):
    if not aura_model and not ai_client: return None
    
    prompt = f"""
    You are {PERSONA_NAME}, a {PERSONA_AGE}yo tech enthusiast. Style: {PERSONA_STYLE}
    Context: You saw a post in the group '{group_name}' about: "{problem}".
    
    Task: Write a short, casual outreach DM.
    Rules:
    - Reference that you saw their post "a while back" (since this is historical mining).
    - mention the group name.
    - lowercase sentence starts, casual punctuation.
    - BE EMPATHETIC.
    - HOOK: "found a crazy stable 4k setup recently and remembered ur post."
    - END with a question: "did u ever fix that?" or "lmk if u still need help."
    - NO LINKS.
    """
    try:
        if ai_client:
            def _work():
                try:
                    r = ai_client.models.generate_content(model="gemini-1.5-flash", contents=prompt)
                    return getattr(r, 'text', None)
                except Exception:
                    return None
            t = await asyncio.to_thread(_work)
            return t.strip() if t else None
        else:
            response = await aura_model.generate_content_async(prompt)
            return response.text.strip()
    except Exception as e:
        logger.error(f"AI Error: {e}")
        return None

async def safety_typing(peer):
    try:
        async with client.action(peer, 'typing'):
            await asyncio.sleep(random.randint(5, 8))
    except: pass

async def send_elite_dm(user_id, message, lead_info):
    limits = get_limits()
    stats = load_json(STATS_FILE, {"dms_today": 0, "date": str(datetime.date.today())})
    
    if stats.get("dms_today", 0) >= limits["max_dms"]:
        logger.warning(f"Daily DM Limit reached ({limits['max_dms']}). Skipping {user_id}")
        return False
        
    try:
        await safety_typing(user_id)
        await client.send_message(user_id, message)
        record_dm_sent()
        mark_processed(user_id)
        log_to_crm(lead_info)
        logger.info(f"✅ Aiden DM sent to {user_id}")
        return True
    except errors.PeerIdInvalidError:
        logger.error(f"Cannot DM {user_id}: Invalid Peer")
    except errors.UserPrivacyRestrictedError:
        logger.warning(f"Cannot DM {user_id}: Privacy Restricted")
    except Exception as e:
        logger.error(f"DM Failed: {e}")
    return False

# --- Background Tasks ---

async def time_traveler():
    """Historical Scraper: Scans for leads 3, 6, 12 months ago."""
    print("Time Traveler Engine: Active")
    while True:
        try:
            dialogs = await client.get_dialogs(limit=50)
            groups = [d for d in dialogs if d.is_group]
            
            for group in groups:
                # Target dates: approx 3, 6, 12 months ago
                offsets = [90, 180, 365]
                for days in offsets:
                    date_target = datetime.datetime.now() - datetime.timedelta(days=days)
                    logger.info(f"Scanning {group.title} for posts near {date_target.date()}")
                    
                    async for msg in client.iter_messages(group, limit=20, offset_date=date_target):
                        if not msg.text: continue
                        
                        text_lower = msg.text.lower()
                        if any(k in text_lower for k in HISTORICAL_KEYWORDS):
                            sender = await msg.get_sender()
                            if not sender or not isinstance(sender, User) or sender.bot: continue
                            
                            if is_processed(sender.id): continue
                            
                            logger.info(f"📍 Found Historical Lead: {sender.id} in {group.title}")
                            
                            # Generate & Alert
                            problem = msg.text[:50]
                            draft = await generate_aura_pro_msg(group.title, problem, msg.text)
                            
                            if draft:
                                # Alert Admin
                                alert = (
                                    f"🕰️ **TIME TRAVELER LEAD**\n"
                                    f"User: `{sender.id}` in `{group.title}`\n"
                                    f"Date: {msg.date.date()}\n"
                                    f"Problem: {problem}...\n\n"
                                    f"🤖 **Aiden Choice:**\n{draft}"
                                )
                                await client.send_message('me', alert)
                                
                                # Soft Outreach Delay
                                wait = random.randint(3600, 10800) # 1-3 hours
                                logger.info(f"Sleeping {wait}s before Aiden outreach...")
                                await asyncio.sleep(wait)
                                
                                lead_info = {
                                    "username": getattr(sender, 'username', 'N/A'),
                                    "user_id": sender.id,
                                    "group": group.title,
                                    "problem": problem,
                                    "date": str(msg.date.date())
                                }
                                await send_elite_dm(sender.id, draft, lead_info)
                        
                        await asyncio.sleep(2) # Per-message safety
                    
                    await asyncio.sleep(30) # Per-offset safety
                
                await asyncio.sleep(300) # Per-group safety
            
            await asyncio.sleep(86400) # Full cycle daily
            
        except Exception as e:
            logger.error(f"Time Traveler Error: {e}")
            await asyncio.sleep(3600)

async def auto_joiner():
    """Scale-up Joiner logic."""
    print("Aura Pro Joiner: Active")
    while True:
        try:
            limits = get_limits()
            stats = load_json(STATS_FILE, {"joins": 0, "date": str(datetime.date.today())})
            
            if stats.get("date") != str(datetime.date.today()):
                stats["joins"] = 0
                stats["date"] = str(datetime.date.today())
                save_json(STATS_FILE, stats)

            if stats["joins"] >= limits["max_joins"]:
                logger.info(f"Daily Join Limit reached ({limits['max_joins']}).")
                await asyncio.sleep(3600)
                continue
                
            query = random.choice(SEARCH_TERMS)
            result = await client(functions.contacts.SearchRequest(q=query, limit=10))
            
            for chat in result.chats:
                if getattr(chat, 'broadcast', False): continue
                
                try:
                    await client(functions.channels.JoinChannelRequest(chat))
                    stats["joins"] += 1
                    save_json(STATS_FILE, stats)
                    
                    await client(UpdateNotifySettingsRequest(
                        peer=InputNotifyPeer(peer=await client.get_input_entity(chat)),
                        settings=InputPeerNotifySettings(mute_until=2147483647)
                    ))
                    
                    logger.info(f"Joined {chat.title}. Total: {stats['joins']}/{limits['max_joins']}")
                    await client.send_message('me', f"🚀 **Aura Pro Join**\nJoined: {chat.title}\nLimit: {stats['joins']}/{limits['max_joins']}")
                    
                    # Interval: 105 +/- 15 mins (approx)
                    delay = random.randint(5400, 7200) # 90-120 mins
                    await asyncio.sleep(delay)
                    
                    if stats["joins"] >= limits["max_joins"]: break
                except: continue

            await asyncio.sleep(600)
        except Exception as e:
            logger.error(f"Joiner Error: {e}")
            await asyncio.sleep(60)

async def health_check():
    """ Heartbeat every 6 hours """
    while True:
        try:
            await client.send_message('me', f"💓 **HEALTH CHECK**\nAccount Status: Active\nDate: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}")
            await asyncio.sleep(21600)
        except: await asyncio.sleep(3600)

async def main():
    print("Initializing AURA PRO Elite...")
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
    print("Aiden & The Time Traveler are Online.")
    
    # Ensure Stats file has start_date
    stats = load_json(STATS_FILE, {})
    if "start_date" not in stats:
        stats["start_date"] = str(datetime.date.today())
        save_json(STATS_FILE, stats)
    
    # Start tasks
    client.loop.create_task(time_traveler())
    client.loop.create_task(auto_joiner())
    client.loop.create_task(health_check())
    
    await client.run_until_disconnected()

if __name__ == '__main__':
    try:
        client.loop.run_until_complete(main())
    except KeyboardInterrupt:
        print("\nElite Bot Stopped.")
    finally:
        try:
            client.loop.run_until_complete(client.disconnect())
        except Exception:
            pass
