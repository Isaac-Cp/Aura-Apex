
import asyncio
import logging
import logging.handlers
import random
import sys
import datetime
import json
import os
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
SESSION_NAME = 'iptv_pro_session'
STATS_FILE = 'bot_stats.json'

# --- 1. Intent-Scoring Engine ---
# High-Intent Words (+2 Points)
HIGH_INTENT = [
    'looking for', 'buying', 'trial', 'test line', 'recommend', 
    'subscription', 'provider link', 'service down', '24h trial',
    'asap', 'today', 'right now'
]

# Context Words (+1 Point)
CONTEXT_KEYWORDS = [
    'iptv', 'firestick', 'tivimate', 'smarters', 'freeze', 'buffering',
    'm3u', 'xtream', 'mag box', 'nvidia shield', 'formuler',
    'uk streaming', 'usa channels', '4k sports', 'ppv'
]

# Blacklist (Score = 0)
BLACKLIST = [
    'sell', 'restock', 'cheap price', 'dm for panel', 'reseller account', 
    'no buffering guarantee', '24/7 support', 'free giveaway', 
    'join my channel', 'discount code', 'best price', 'unlimited connections',
    'instantly', 'automated', 'shop link'
]

# --- 2. Expanded Discovery Categories ---
SEARCH_TERMS = [
    # High-End
    "TiviMate Premium", "Smarters Pro Support", "IBO Player", "OttNavigator", "Sparkle TV",
    # Hardware
    "Firestick 4K Max", "Nvidia Shield TV", "Formuler Z11", "Buzztv", "Android TV Box Help",
    # Regional
    "UK Streaming Support", "USA Cord Cutters", "Canada IPTV", "Australia Live TV", "Europe Streaming",
    # Sports
    "Premier League Live", "NFL Sunday Ticket", "UFC PPV", "F1 Streams UK",
    # Technical
    "M3U Troubleshooting", "Xtream Codes API", "EPG Updates", "Cut The Cord"
]

# JOINER SETTINGS & SAFETY (AURA EDITION)
MAX_JOINS_PER_DAY = 12 # Reduced for stealth
MIN_DELAY = 5400   # 90 minutes
MAX_DELAY = 7200   # 120 minutes
FLOOD_SLEEP_THRESHOLD = 3600

# Statistics & Dashboard
daily_stats = {
    "scanned": 0,
    "leads": 0,
    "urgent": 0,
    "dms_sent": 0
}

# Initialize Client
client = TelegramClient(SESSION_NAME, API_ID, API_HASH)

if GEMINI_API_KEY and "your_gemini_api_key_here" not in GEMINI_API_KEY:
    if GENAI_PROVIDER == 'new':
        try:
            ai_client = genai_new.Client(api_key=GEMINI_API_KEY)
            aura_model = None
            logging.info("Aura AI Engine Activated (genai).")
        except Exception as _e:
            aura_model = None
            ai_client = None
            logger.warning(f"Aura AI init failed (genai): {_e}")
    else:
        try:
            genai_old.configure(api_key=GEMINI_API_KEY)
            aura_model = genai_old.GenerativeModel('gemini-pro')
            ai_client = None
            logging.info("Aura AI Engine Activated (generativeai).")
        except Exception as _e:
            aura_model = None
            ai_client = None
            logger.warning(f"Aura AI init failed (generativeai): {_e}")
else:
    aura_model = None
    ai_client = None
    logging.warning("Aura AI: No valid GEMINI_API_KEY found. Using fallback text.")

def load_stats():
    """Loads daily stats from JSON to prevent restart loopholes."""
    today_str = str(datetime.date.today())
    default_stats = {"date": today_str, "joins": 0}
    
    if os.path.exists(STATS_FILE):
        try:
            with open(STATS_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if data.get("date") != today_str:
                    return default_stats
                return data
        except Exception:
            return default_stats
    return default_stats

def save_stats(joins):
    """Saves current joins to JSON."""
    stats = {
        "date": str(datetime.date.today()),
        "joins": joins
    }
    try:
        with open(STATS_FILE, 'w', encoding='utf-8') as f:
            json.dump(stats, f)
    except Exception as e:
        logger.error(f"Failed to save stats: {e}")

def get_lead_status():
    """Determines lead priority based on time and day."""
    now = datetime.datetime.now()
    is_prime_time = 18 <= now.hour <= 21
    is_weekend = now.weekday() >= 5
    
    if is_prime_time or is_weekend:
        return "⚡", "High-Traffic"
    return "", "Normal"

def calculate_score(text):
    """Calculates lead score based on keywords."""
    text_lower = text.lower()
    score = 0
    
    for bad_word in BLACKLIST:
        if bad_word in text_lower:
            return 0
            
    for word in HIGH_INTENT:
        if word in text_lower:
            score += 2
            
    for word in CONTEXT_KEYWORDS:
        if word in text_lower:
            score += 1
            
    return score

# CRM Hook Placeholder
async def send_to_crm(lead_data):
    """Placeholder for future CRM integration."""
    pass

# Aura AI Generator
async def generate_aura_message(group_name, user_problem):
    """Generates a specialized 'Human Boy' persona reply."""
    if not aura_model and not ai_client:
        return None # Fallback to manual response
    prompt = f"""
    You are a helpful, tech-savvy user on Telegram. You are NOT a seller. You are just a guy who solved a similar problem.
    Context: A user in the group '{group_name}' posted about: "{user_problem}".
    
    Task: Write a short, casual DM to them.
    Rules:
    1. Use lowercase mostly, casual punctuation.
    2. Empathize with the problem ("man i hate when that happens").
    3. Say you found a fix/provider that works for you.
    4. END WITH A QUESTION ("want the link?" or "should i send info?").
    5. NO LINKS in this message.
    """
    try:
        if ai_client:
            def _work():
                try:
                    r = ai_client.models.generate_content(model="gemini-1.5-flash", contents=prompt)
                    return getattr(r, 'text', None)
                except Exception as _e:
                    logger.error(f"Aura Gen Error (genai): {_e}")
                    return None
            t = await asyncio.to_thread(_work)
            if t:
                return t.lower().strip()
            return None
        else:
            response = await aura_model.generate_content_async(prompt)
            return response.text.lower().strip()
    except Exception as e:
        logger.error(f"Aura Gen Error: {e}")
        return None

# Auto-Greeting (Business Mode Emulation) - Now "Hot Lead" Detector
@client.on(events.NewMessage(incoming=True, func=lambda e: e.is_private))
async def handle_private_reply(event):
    """Detects replies to our outreach."""
    sender = await event.get_sender()
    if sender and not sender.bot and not event.out:
        logger.info(f"⚡ HOT LEAD REPLY: {sender.id} replied: {event.raw_text}")
        await client.send_message('me', f"⚡ **HOT LEAD DETECTED**\nUser `{sender.id}` replied to DM!\n\n> {event.raw_text}")

@client.on(events.NewMessage(incoming=True))
async def handle_new_message(event):
    global daily_stats
    
    try:
        if event.is_private: return

        if not event.raw_text: return

        # Maturity Filter: Skip if text is too short/bot-like? (Simplified: Just count scans)
        daily_stats['scanned'] += 1

        sender = await event.get_sender()
        chat = await event.get_chat()
        
        text = event.raw_text
        
        if event.out or (isinstance(sender, User) and sender.bot):
            return
        
        # Translation Logic
        original_text = text
        lang_detected = 'en'
        try:
             if 'iptv' in text.lower() and calculate_score(text) < 1:
                 translated = GoogleTranslator(source='auto', target='en').translate(text)
                 if translated != text:
                     text = translated
                     lang_detected = 'foreign'
        except Exception as e: pass

        # Scoring
        score = calculate_score(text)
        if score < 2:
            return

        daily_stats['leads'] += 1
        
        # Time-of-Day
        priority_icon, priority_label = get_lead_status()
        
        # Urgency
        urgency_icon = "💎"
        if any(w in text.lower() for w in ['asap', 'today', 'right now']):
            urgency_icon = "🔥"
            daily_stats['urgent'] += 1

        # Link Construction
        chat_id = chat.id
        msg_id = event.id
        if hasattr(chat, 'username') and chat.username:
            msg_link = f"https://t.me/{chat.username}/{msg_id}"
        else:
            msg_link = f"https://t.me/c/{str(chat_id).replace('-100', '')}/{msg_id}"

        username = f"@{sender.username}" if (sender and sender.username) else "No Username"
        group_name = chat.title if hasattr(chat, 'title') else "Unknown"

        trans_note = f"\n🌐 **Translated:** `{original_text}`" if lang_detected == 'foreign' else ""
        
        # Aura Generation
        aura_suggestion = await generate_aura_message(group_name, text)
        ai_block = f"\n🤖 **Aura Draft:**\n> {aura_suggestion}\n" if aura_suggestion else ""

        lead_report = (
            f"⚡ **AURA LEAD ALERT** {priority_icon}\n"
            f"**Score:** {score}/5 {urgency_icon}\n"
            f"**Traffic:** {priority_label}\n\n"
            f"👤 **User:** {username} (`{sender.id if sender else 'N/A'}`)\n"
            f"📍 **Group:** {group_name}\n"
            f"💬 **Message:**\n"
            f"> {text[:200]}...{trans_note}\n"
            f"{ai_block}\n"
            f"🔗 **Action:** [CLICK TO REPLY]({msg_link})"
        )

        await client.send_message('me', lead_report, link_preview=False)
        logger.info(f"Lead captured: {username}")
        
        # Update Dashboard
        await update_dashboard()

    except Exception as e:
        logger.error(f"Error processing message: {e}")

async def update_dashboard():
    """Updates the Hot Lead Dashboard in Saved Messages."""
    # Logic to find the last dashboard message could be complex, for now we log it periodically
    # or just rely on the alerts.
    pass 

async def auto_joiner():
    """Background task to search and join new groups safely."""
    print("Auto-Joiner started (Aura Stealth Mode).")
    
    stats = load_stats()
    joins_today = stats['joins']
    
    while True:
        try:
            stats = load_stats()
            joins_today = stats['joins']
            
            if joins_today >= MAX_JOINS_PER_DAY:
                logger.info("Daily join limit reached. Sleeping for 1 hour.")
                await asyncio.sleep(3600)
                continue
            
            query = random.choice(SEARCH_TERMS)
            
            try:
                result = await client(functions.contacts.SearchRequest(q=query, limit=20))
            except Exception:
                await asyncio.sleep(60)
                continue

            for chat in result.chats:
                try:
                    # BLOCKER 1: Broadcast Channel
                    if getattr(chat, 'broadcast', False): continue
                    
                    # BLOCKER 2: Maturity Filter (Msg Count Check) -> Requires joining to see count reliably?
                    # Telethon Check: chat.participants_count. If too low, skip.
                    if hasattr(chat, 'participants_count') and chat.participants_count and chat.participants_count < 50:
                        logger.info(f"Skipping {chat.title}: Too small ({chat.participants_count})")
                        continue

                    logger.info(f"Attempting to join: {chat.title}")
                    await client(functions.channels.JoinChannelRequest(chat))
                    
                    joins_today += 1
                    save_stats(joins_today)
                    
                    try:
                        await client(UpdateNotifySettingsRequest(
                            peer=InputNotifyPeer(peer=await client.get_input_entity(chat)),
                            settings=InputPeerNotifySettings(mute_until=2147483647)
                        ))
                    except Exception: pass

                    await client.send_message('me', f"🔮 **Aura Joiner**\nJoined: {chat.title}\nStats: {joins_today}/{MAX_JOINS_PER_DAY}")
                    
                    delay = random.randint(MIN_DELAY, MAX_DELAY)
                    logger.info(f"Sleeping for {delay}s...")
                    await asyncio.sleep(delay)
                    
                    if joins_today >= MAX_JOINS_PER_DAY: break
                        
                except errors.FloodWaitError as e:
                    logger.warning(f"FloodWait: Sleeping {e.seconds + 60}s")
                    await asyncio.sleep(e.seconds + 60)
                    break 
                except Exception:
                    await asyncio.sleep(60)

            await asyncio.sleep(600)

        except Exception as e:
            logger.error(f"Auto-Joiner Error: {e}")
            await asyncio.sleep(60)

async def conversational_ghost():
    """Reacts to messages to build reputation."""
    print("Conversational Ghost started.")
    while True:
        try:
            wait_time = random.randint(14400, 21600)
            await asyncio.sleep(wait_time)
            
            dialogs = await client.get_dialogs(limit=50)
            targets = [d for d in dialogs if d.is_group]
            
            if not targets: continue

            target = random.choice(targets)
            messages = await client.get_messages(target, limit=1)
            
            if messages:
                reaction = random.choice(["\U0001F44D", "\U0001F525"]) 
                try:
                    await client(functions.messages.SendReactionRequest(
                        peer=target,
                        msg_id=messages[0].id,
                        reaction=[ReactionEmoji(emoticon=reaction)]
                    ))
                    logger.info(f"Ghost reacted in {target.title}")
                except Exception: pass

        except Exception:
            await asyncio.sleep(3600)

async def main():
    print("Initializing Aura Client...")
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
    print("🔮 Aura AI Lead Specialist Online.")
    print(f"Features: AI Persona, Smart Delay, Stealth Joiner ({MAX_JOINS_PER_DAY}/day).")
    
    client.loop.create_task(auto_joiner())
    client.loop.create_task(conversational_ghost())
    
    await client.run_until_disconnected()

if __name__ == '__main__':
    try:
        client.loop.run_until_complete(main())
    except KeyboardInterrupt:
        print("\nBot stopped by user.")
    finally:
        try:
            client.loop.run_until_complete(client.disconnect())
        except Exception:
            pass
