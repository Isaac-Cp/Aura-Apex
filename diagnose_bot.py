import os
import asyncio
import aiosqlite
import datetime
from dotenv import load_dotenv
from aura_core import setup_logging
from config import DB_FILE

# Configure logging
setup_logging()
import logging
logger = logging.getLogger("DiagnoseBot")

load_dotenv()

logger.info("--- ENV VAR CHECK ---")
market = os.getenv("MARKET")
logger.info(f"MARKET: {market}")
api_id = os.getenv("API_ID")
logger.info(f"API_ID set: {bool(api_id)}")

logger.info("\n--- MARKET HOUR CHECK ---")
def market_hour_ok():
    mk = (market or "").lower()
    offsets = {
        "en-uk": 0, "en-us": -5, "es-es": 1, "it-it": 1, "de-de": 1, "fr-fr": 1
    }
    off = offsets.get(mk, 0)
    now_utc = datetime.datetime.utcnow()
    h = (now_utc + datetime.timedelta(hours=off)).hour
    logger.info(f"Market: {mk}, Offset: {off}, UTC Hour: {now_utc.hour}, Local Hour: {h}")
    return 9 <= h <= 21

is_ok = market_hour_ok()
logger.info(f"Market Open: {is_ok}")

async def check_db():
    logger.info("\n--- DB LOCK CHECK ---")
    try:
        async with aiosqlite.connect(DB_FILE, timeout=5) as conn:
            async with conn.execute("PRAGMA journal_mode;") as cursor:
                mode = await cursor.fetchone()
                logger.info(f"Journal Mode: {mode}")
            
            async with conn.execute("SELECT name FROM sqlite_master WHERE type='table';") as cursor:
                tables = await cursor.fetchall()
                logger.info(f"Tables: {tables}")
            
            for table in tables:
                async with conn.execute(f"SELECT count(*) FROM {table[0]};") as cursor:
                    count = (await cursor.fetchone())[0]
                    logger.info(f"Table {table[0]} count: {count}")
        logger.info("DB Check: SUCCESS")
    except Exception as e:
        logger.error(f"DB Check: FAILED - {e}")

if __name__ == "__main__":
    try:
        asyncio.run(check_db())
    except Exception as e:
        logger.error(f"Fatal Error: {e}")
