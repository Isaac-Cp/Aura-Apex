import os
import sqlite3
import datetime
from dotenv import load_dotenv

load_dotenv()

print("--- ENV VAR CHECK ---")
market = os.getenv("MARKET")
print(f"MARKET: {market}")
api_id = os.getenv("API_ID")
print(f"API_ID set: {bool(api_id)}")

print("\n--- MARKET HOUR CHECK ---")
def market_hour_ok():
    mk = (market or "").lower()
    offsets = {
        "en-uk": 0, "en-us": -5, "es-es": 1, "it-it": 1, "de-de": 1, "fr-fr": 1
    }
    off = offsets.get(mk, 0)
    now_utc = datetime.datetime.utcnow()
    h = (now_utc + datetime.timedelta(hours=off)).hour
    print(f"Market: {mk}, Offset: {off}, UTC Hour: {now_utc.hour}, Local Hour: {h}")
    return 9 <= h <= 21

is_ok = market_hour_ok()
print(f"Market Open: {is_ok}")

print("\n--- DB LOCK CHECK ---")
try:
    conn = sqlite3.connect("gold_leads.db", timeout=5)
    cursor = conn.cursor()
    cursor.execute("PRAGMA journal_mode;")
    mode = cursor.fetchone()
    print(f"Journal Mode: {mode}")
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    print(f"Tables: {tables}")
    for table in tables:
        cursor.execute(f"SELECT count(*) FROM {table[0]};")
        count = cursor.fetchone()[0]
        print(f"Table {table[0]} count: {count}")
    conn.close()
    print("DB Check: SUCCESS")
except Exception as e:
    print(f"DB Check: FAILED - {e}")
