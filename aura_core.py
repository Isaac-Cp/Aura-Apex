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
import sqlite3
from config import DB_FILE

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
    try:
        name = os.path.basename(path or "")
        if name == "supreme_stats.json":
            try:
                con = sqlite3.connect(DB_FILE)
                cur = con.cursor()
                cur.execute("SELECT data FROM supreme_stats WHERE id=1")
                row = cur.fetchone()
                con.close()
                if row and row[0]:
                    return json.loads(row[0])
                return default
            except Exception:
                return default
        if name == "supreme_groups.json":
            try:
                con = sqlite3.connect(DB_FILE)
                cur = con.cursor()
                cur.execute("SELECT link FROM processed_groups")
                rows = cur.fetchall()
                con.close()
                return [str(r[0]) for r in rows]
            except Exception:
                return default
        if name == "prospect_catalog.json":
            try:
                con = sqlite3.connect(DB_FILE)
                cur = con.cursor()
                cur.execute("SELECT data FROM prospect_catalog")
                rows = cur.fetchall()
                con.close()
                out = []
                for (data_json,) in rows:
                    try:
                        out.append(json.loads(data_json))
                    except Exception:
                        continue
                return out
            except Exception:
                return default
        if name == "source_kpis.json":
            try:
                con = sqlite3.connect(DB_FILE)
                cur = con.cursor()
                cur.execute("SELECT term, attempts, successes, errors FROM source_kpis")
                rows = cur.fetchall()
                con.close()
                out = {}
                for term, att, suc, err in rows:
                    out[str(term or "")] = {"attempts": int(att or 0), "successes": int(suc or 0), "errors": int(err or 0)}
                return out
            except Exception:
                return default
        if name == "join_attempts.json":
            try:
                con = sqlite3.connect(DB_FILE)
                cur = con.cursor()
                cur.execute("SELECT id, title, status, reason, ts FROM join_attempts ORDER BY ts DESC LIMIT 1000")
                rows = cur.fetchall()
                con.close()
                out = []
                for i, t, s, r, ts in rows:
                    out.append({"id": i or "", "title": t or "", "status": s or "", "reason": r or "", "ts": ts})
                return out
            except Exception:
                return default
        if name == "potential_targets.json":
            try:
                con = sqlite3.connect(DB_FILE)
                cur = con.cursor()
                cur.execute("SELECT link, title, members, source_group_id, discovered_at FROM potential_targets ORDER BY discovered_at DESC")
                rows = cur.fetchall()
                con.close()
                out = []
                for ln, tt, mm, sg, dt in rows:
                    out.append({"link": ln or "", "title": tt or "", "members": int(mm or 0), "source_group_id": sg or "", "discovered_at": dt or ""})
                return out
            except Exception:
                return default
        if name == "cached_invites.json":
            try:
                con = sqlite3.connect(DB_FILE)
                cur = con.cursor()
                cur.execute("SELECT link, title, ts FROM cached_invites ORDER BY ts DESC")
                rows = cur.fetchall()
                con.close()
                out = []
                for ln, tt, ts in rows:
                    out.append({"link": ln or "", "title": tt or "", "ts": ts})
                return out
            except Exception:
                return default
        if name == "entity_cache.json":
            try:
                con = sqlite3.connect(DB_FILE)
                cur = con.cursor()
                cur.execute("SELECT value FROM entity_cache")
                rows = cur.fetchall()
                con.close()
                return [str(v[0]) for v in rows]
            except Exception:
                return default
        if name == "resolve_cooldowns.json":
            try:
                con = sqlite3.connect(DB_FILE)
                cur = con.cursor()
                cur.execute("SELECT key, until_ts FROM resolve_cooldowns")
                rows = cur.fetchall()
                con.close()
                out = {}
                for k, u in rows:
                    out[str(k or "")] = float(u or 0.0)
                return out
            except Exception:
                return default
        if name == "supreme_groups.json":
            try:
                con = sqlite3.connect(DB_FILE)
                cur = con.cursor()
                cur.execute("CREATE TABLE IF NOT EXISTS processed_groups (link TEXT PRIMARY KEY)")
                cur.execute("SELECT link FROM processed_groups")
                rows = cur.fetchall()
                con.close()
                return [str(v[0]) for v in rows]
            except Exception:
                return default
        if name == "supreme_stats.json":
            try:
                con = sqlite3.connect(DB_FILE)
                cur = con.cursor()
                cur.execute("CREATE TABLE IF NOT EXISTS kv_store (key TEXT PRIMARY KEY, value TEXT)")
                cur.execute("SELECT value FROM kv_store WHERE key='supreme_stats'")
                row = cur.fetchone()
                con.close()
                if row and row[0]:
                    return json.loads(row[0])
                return default
            except Exception:
                return default
        if name == "prospect_catalog.json":
            try:
                con = sqlite3.connect(DB_FILE)
                cur = con.cursor()
                cur.execute("CREATE TABLE IF NOT EXISTS prospect_catalog (url TEXT PRIMARY KEY, json TEXT)")
                cur.execute("SELECT json FROM prospect_catalog")
                rows = cur.fetchall()
                con.close()
                out = []
                for (blob,) in rows:
                    try:
                        out.append(json.loads(blob))
                    except Exception:
                        continue
                return out
            except Exception:
                return default
    except Exception:
        pass
    # Skip legacy JSON file reads for migrated stores
    name = os.path.basename(path or "")
    MIGRATED = {
        "supreme_stats.json",
        "supreme_groups.json",
        "prospect_catalog.json",
        "source_kpis.json",
        "join_attempts.json",
        "potential_targets.json",
        "cached_invites.json",
        "entity_cache.json",
        "resolve_cooldowns.json",
    }
    if name in MIGRATED:
        return default
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
        name = os.path.basename(path or "")
        if name == "supreme_stats.json" and isinstance(data, dict):
            try:
                con = sqlite3.connect(DB_FILE)
                cur = con.cursor()
                cur.execute("INSERT OR REPLACE INTO supreme_stats(id, data) VALUES(1, ?)", (json.dumps(data),))
                con.commit()
                con.close()
                return
            except Exception:
                pass
        if name == "supreme_groups.json" and isinstance(data, list):
            try:
                con = sqlite3.connect(DB_FILE)
                cur = con.cursor()
                cur.execute("DELETE FROM processed_groups")
                for ln in data:
                    if ln:
                        cur.execute("INSERT OR IGNORE INTO processed_groups(link) VALUES(?)", (str(ln),))
                con.commit()
                con.close()
                return
            except Exception:
                pass
        if name == "prospect_catalog.json" and isinstance(data, list):
            try:
                con = sqlite3.connect(DB_FILE)
                cur = con.cursor()
                cur.execute("DELETE FROM prospect_catalog")
                for rec in data:
                    cur.execute("INSERT OR REPLACE INTO prospect_catalog(url, data) VALUES(?, ?)", (str(rec.get("url","")), json.dumps(rec)))
                con.commit()
                con.close()
                return
            except Exception:
                pass
        if name == "source_kpis.json" and isinstance(data, dict):
            try:
                con = sqlite3.connect(DB_FILE)
                cur = con.cursor()
                for term, rec in data.items():
                    att = int((rec or {}).get("attempts", 0) or 0)
                    suc = int((rec or {}).get("successes", 0) or 0)
                    err = int((rec or {}).get("errors", 0) or 0)
                    cur.execute("INSERT INTO source_kpis(term, attempts, successes, errors, updated_at) VALUES(?,?,?,?,CURRENT_TIMESTAMP) ON CONFLICT(term) DO UPDATE SET attempts=?, successes=?, errors=?, updated_at=CURRENT_TIMESTAMP", (term, att, suc, err, att, suc, err))
                con.commit()
                con.close()
                return
            except Exception:
                pass
        if name == "join_attempts.json" and isinstance(data, list):
            try:
                con = sqlite3.connect(DB_FILE)
                cur = con.cursor()
                for it in data:
                    cur.execute("INSERT INTO join_attempts(id, title, status, reason, ts) VALUES(?,?,?,?,?)", (str(it.get("id","")), str(it.get("title","")), str(it.get("status","")), str(it.get("reason","")), float(it.get("ts",0.0))))
                con.commit()
                con.close()
                return
            except Exception:
                pass
        if name == "potential_targets.json" and isinstance(data, list):
            try:
                con = sqlite3.connect(DB_FILE)
                cur = con.cursor()
                cur.execute("DELETE FROM potential_targets")
                for it in data:
                    cur.execute("INSERT OR REPLACE INTO potential_targets(link, title, members, source_group_id, discovered_at) VALUES(?,?,?,?,?)", (str(it.get("link","")), str(it.get("title","")), int(it.get("members",0) or 0), str(it.get("source_group_id","")), str(it.get("discovered_at",""))))
                con.commit()
                con.close()
                return
            except Exception:
                pass
        if name == "cached_invites.json" and isinstance(data, list):
            try:
                con = sqlite3.connect(DB_FILE)
                cur = con.cursor()
                for it in data:
                    cur.execute("INSERT OR REPLACE INTO cached_invites(link, title, ts) VALUES(?,?,?)", (str(it.get("link","")), str(it.get("title","")), float(it.get("ts",0.0))))
                con.commit()
                con.close()
                return
            except Exception:
                pass
        if name == "entity_cache.json" and isinstance(data, list):
            try:
                con = sqlite3.connect(DB_FILE)
                cur = con.cursor()
                cur.execute("DELETE FROM entity_cache")
                for v in data:
                    cur.execute("INSERT OR IGNORE INTO entity_cache(value) VALUES(?)", (str(v),))
                con.commit()
                con.close()
                return
            except Exception:
                pass
        if name == "resolve_cooldowns.json" and isinstance(data, dict):
            try:
                con = sqlite3.connect(DB_FILE)
                cur = con.cursor()
                for k, u in data.items():
                    cur.execute("INSERT OR REPLACE INTO resolve_cooldowns(key, until_ts) VALUES(?,?)", (str(k or ""), float(u or 0.0)))
                con.commit()
                con.close()
                return
            except Exception:
                pass
        if name == "supreme_groups.json" and isinstance(data, list):
            try:
                con = sqlite3.connect(DB_FILE)
                cur = con.cursor()
                cur.execute("CREATE TABLE IF NOT EXISTS processed_groups (link TEXT PRIMARY KEY)")
                cur.execute("DELETE FROM processed_groups")
                for ln in data:
                    if ln:
                        cur.execute("INSERT OR IGNORE INTO processed_groups(link) VALUES(?)", (str(ln),))
                con.commit()
                con.close()
                return
            except Exception:
                pass
        if name == "supreme_stats.json" and isinstance(data, dict):
            try:
                con = sqlite3.connect(DB_FILE)
                cur = con.cursor()
                cur.execute("CREATE TABLE IF NOT EXISTS kv_store (key TEXT PRIMARY KEY, value TEXT)")
                cur.execute("INSERT OR REPLACE INTO kv_store(key, value) VALUES('supreme_stats', ?)", (json.dumps(data),))
                con.commit()
                con.close()
                return
            except Exception:
                pass
        if name == "prospect_catalog.json" and isinstance(data, list):
            try:
                con = sqlite3.connect(DB_FILE)
                cur = con.cursor()
                cur.execute("CREATE TABLE IF NOT EXISTS prospect_catalog (url TEXT PRIMARY KEY, json TEXT)")
                cur.execute("DELETE FROM prospect_catalog")
                for rec in data:
                    url = str((rec or {}).get("url", ""))
                    if url:
                        cur.execute("INSERT OR REPLACE INTO prospect_catalog(url, json) VALUES(?, ?)", (url, json.dumps(rec)))
                con.commit()
                con.close()
                return
            except Exception:
                pass
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

# Compiled Regex for performance
_REBRAND_RE = re.compile(rf"\b({'|'.join(re.escape(w) for w in REBRAND_KEYWORDS)})\b", re.I) if REBRAND_KEYWORDS else None
_URGENCY_RE = re.compile(rf"\b({'|'.join(re.escape(w) for w in URGENCY_KEYWORDS)})\b", re.I) if URGENCY_KEYWORDS else None
_COMPETITOR_RE = re.compile(rf"\b({'|'.join(re.escape(w) for w in COMPETITOR_KEYWORDS)})\b", re.I) if COMPETITOR_KEYWORDS else None
_COMMERCIAL_RE = re.compile(rf"\b({'|'.join(re.escape(w) for w in COMMERCIAL_KEYWORDS)})\b", re.I) if COMMERCIAL_KEYWORDS else None
_NEGATIVE_RE = re.compile(rf"\b({'|'.join(re.escape(w) for w in NEGATIVE_TRIGGERS)})\b", re.I) if NEGATIVE_TRIGGERS else None

_SELLER_TERMS = [
    "dm for", "dm me", "contact me", "reseller", "panel", "credits", "wholesale", 
    "supplier", "official", "whatsapp", "join my", "test available", "free trial",
    "all channels", "premium iptv", "stable service", "bouquet", "m3u list",
    "price list", "pricing", "subscription offer"
]
_SELLER_RE = re.compile(rf"({'|'.join(re.escape(w) for w in _SELLER_TERMS)})", re.I)

def calculate_lead_score(message_text: Optional[str], user_data: Any) -> int:
    """
    Calculates a lead score based on keywords, user quality, and sentiment.
    Returns an integer score. Optimized with pre-compiled regex.
    """
    score = 0
    if not message_text:
        return 0
    text = message_text.lower()

    # 1. HIGH INTENT KEYWORDS (+7 points)
    if _REBRAND_RE and _REBRAND_RE.search(text):
        score += 7

    # 2. PAIN POINT KEYWORDS (+3 points)
    if _URGENCY_RE and _URGENCY_RE.search(text):
        score += 3

    # 3. COMPETITOR MENTIONS (+2 points)
    if _COMPETITOR_RE and _COMPETITOR_RE.search(text):
        score += 2
        
    # 4. COMMERCIAL INTENT (+4 points)
    if _COMMERCIAL_RE and _COMMERCIAL_RE.search(text):
        score += 4

    # 5. NEGATIVE SENTIMENT (+2 points)
    if _NEGATIVE_RE and _NEGATIVE_RE.search(text):
        score += 2

    # 6. SELLER/PROVIDER SIGNALS (CRITICAL DEDUCTION)
    if _SELLER_RE.search(text):
        score -= 20 # Decimate the score for sellers

    # 7. USER QUALITY BONUSES (Multipliers)
    if user_data:
        if hasattr(user_data, 'username') and user_data.username:
            score += 2
        if hasattr(user_data, 'premium') and user_data.premium:
            score += 3

    return score
