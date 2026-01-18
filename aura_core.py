import asyncio
import json
import logging
import os
import random
import time
import sqlite3
import sqlite3

logger = logging.getLogger(__name__)


def load_json(path, default):
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return default
    return default


def save_json(path, data):
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f)
    except Exception as e:
        logger.error(f"JSON Save Error: {e}")


def should_outreach(probability=0.6):
    try:
        return random.random() < float(probability)
    except Exception:
        return False


async def proxy_health_monitor(client, proxy_file, interval=3600):
    while True:
        try:
            if not os.path.exists(proxy_file):
                await asyncio.sleep(interval)
                continue
            await client.get_me()
        except Exception as e:
            logger.error(f"Proxy health check failed: {e}")
        await asyncio.sleep(interval)


def keep_alive():
    try:
        _ = time.time()
    except Exception:
        pass

def clean_old_logs(db_path, days=7):
    try:
        conn = sqlite3.connect(db_path)
        c = conn.cursor()
        try:
            c.execute("DELETE FROM prospects WHERE datetime(message_ts) < datetime('now', ?)", (f'-{int(days)} days',))
        except Exception:
            pass
        try:
            c.execute("DELETE FROM activity_log WHERE datetime(ts) < datetime('now', ?)", (f'-{int(days)} days',))
        except Exception:
            pass
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Cleanup Error: {e}")
