import asyncio
import json
import logging
import os
import random
import time

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

