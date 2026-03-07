from typing import Optional, Any, Dict, List
import asyncio
import json
import logging
import logging.handlers
import queue
import atexit
import os
import random
import sys
import time
import re
import aiosqlite

logger = logging.getLogger(__name__)

def setup_logging(log_file: str = "bot.log", level: int = logging.INFO) -> Optional[logging.Logger]:
    """
    Configures the root logger with non-blocking QueueHandler and QueueListener.
    """
    try:
        # Create a shared queue
        log_queue = queue.Queue(-1)
        
        # Create handlers
        file_handler = logging.handlers.RotatingFileHandler(
            log_file, maxBytes=5*1024*1024, backupCount=5, encoding='utf-8'
        )
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)

        # Create QueueListener with the blocking handlers
        listener = logging.handlers.QueueListener(log_queue, file_handler, console_handler)
        listener.start()
        
        # Register cleanup to stop listener on exit
        atexit.register(listener.stop)

        # Configure root logger to use QueueHandler
        root_logger = logging.getLogger()
        root_logger.setLevel(level)
        
        # Remove existing handlers to avoid duplicates
        if root_logger.handlers:
            root_logger.handlers.clear()
            
        # Add the non-blocking QueueHandler
        queue_handler = logging.handlers.QueueHandler(log_queue)
        root_logger.addHandler(queue_handler)
        
        logging.getLogger("aiosqlite").setLevel(logging.WARNING)
        logging.getLogger("aiohttp").setLevel(logging.WARNING)
        
        return root_logger
    except Exception as e:
        sys.stderr.write(f"Failed to setup logging: {e}\n")
        return None


def load_json(path: str, default: Any) -> Any:
    # Legacy sync support if needed, but prefer async
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"JSON Load Error ({path}): {e}")
            return default
    return default


async def load_json_async(path: str, default: Any) -> Any:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, load_json, path, default)


def save_json(path: str, data: Any) -> None:
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f)
    except Exception as e:
        logger.error(f"JSON Save Error: {e}")


async def save_json_async(path: str, data: Any) -> None:
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, save_json, path, data)


def should_outreach(probability: float = 0.6) -> bool:
    try:
        return random.random() < float(probability)
    except Exception as e:
        logger.warning(f"Outreach Check Error: {e}")
        return False


async def proxy_health_monitor(client: Any, proxy_file: str, interval: int = 3600) -> None:
    while True:
        try:
            if not os.path.exists(proxy_file):
                await asyncio.sleep(interval)
                continue
            await client.get_me()
        except Exception as e:
            logger.error(f"Proxy health check failed: {e}")
        await asyncio.sleep(interval)


def keep_alive() -> None:
    try:
        _ = time.time()
    except Exception:
        pass

async def clean_old_logs_async(db_path: str, days: int = 7) -> None:
    try:
        async with aiosqlite.connect(db_path) as conn:
            try:
                await conn.execute("PRAGMA journal_mode=WAL;")
                await conn.execute("PRAGMA synchronous=NORMAL;")
            except Exception as e:
                logger.debug(f"DB Pragma Error: {e}")
            try:
                await conn.execute("DELETE FROM prospects WHERE datetime(message_ts) < datetime('now', ?)", (f'-{int(days)} days',))
            except Exception as e:
                logger.debug(f"Prospects Cleanup Error: {e}")
            try:
                await conn.execute("DELETE FROM activity_log WHERE datetime(ts) < datetime('now', ?)", (f'-{int(days)} days',))
            except Exception as e:
                logger.debug(f"Activity Log Cleanup Error: {e}")
            await conn.commit()
    except Exception as e:
        logger.error(f"Cleanup Error: {e}")

def detect_language_from_text(text: str) -> Optional[str]:
    try:
        if not text:
            return None
        if re.search(r'[Ð-Ð¯Ð°-ÑÐÑ‘]', text):
            return "ru"
        return None
    except Exception as e:
        logger.debug(f"Language detection error: {e}")
        return None

# --- Advanced Lead Scoring Engine (2026 Update) ---

# Refined Keyword Structure
from config import (
    REBRAND_KEYWORDS, URGENCY_KEYWORDS, COMMERCIAL_KEYWORDS, 
    COMPETITOR_KEYWORDS, NEGATIVE_TRIGGERS
)

def calculate_lead_score(message_text: Optional[str], user_data: Any) -> int:
    """
    Calculates a lead score based on keywords, user quality, and sentiment.
    Returns an integer score.
    """
    score = 0
    if not message_text:
        return 0
    text = message_text.lower()

    # 1. HIGH INTENT KEYWORDS (+7 points)
    if any(word in text for word in REBRAND_KEYWORDS):
        score += 7

    # 2. PAIN POINT KEYWORDS (+3 points)
    if any(word in text for word in URGENCY_KEYWORDS):
        score += 3

    # 3. COMPETITOR MENTIONS (+2 points)
    if any(word in text for word in COMPETITOR_KEYWORDS):
        score += 2
        
    # 4. COMMERCIAL INTENT (+4 points)
    if any(word in text for word in COMMERCIAL_KEYWORDS):
        score += 4

    # 5. NEGATIVE SENTIMENT (+2 points)
    if any(word in text for word in NEGATIVE_TRIGGERS):
        score += 2

    # 6. USER QUALITY BONUSES (Multipliers)
    if user_data:
        # Give points for having a username (shows an active/real user)
        if hasattr(user_data, 'username') and user_data.username:
            score += 2
        
        # Give points for being Premium (shows they have money/intent)
        if hasattr(user_data, 'premium') and user_data.premium:
            score += 3

    return score
