
import sqlite3
import json
import os

DB_FILE = "gold_leads.db"

def check_db():
    if not os.path.exists(DB_FILE):
        print(f"Database {DB_FILE} not found.")
        return

    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    print("--- Stats ---")
    cursor.execute("SELECT value FROM kv_store WHERE key='dm_initiated_today'")
    row = cursor.fetchone()
    print(f"DM Initiated Today: {row[0] if row else '0'}")

    cursor.execute("SELECT data FROM supreme_stats WHERE id=1")
    row = cursor.fetchone()
    if row:
        stats = json.loads(row[0])
        print(f"Stats Data: {json.dumps(stats, indent=2)}")

    print("\n--- Recent Activity Log ---")
    cursor.execute("SELECT ts, type, details FROM activity_log ORDER BY ts DESC LIMIT 5")
    for row in cursor.fetchall():
        print(f"{row[0]} | {row[1]} | {row[2]}")

    print("\n--- Prospects Summary ---")
    cursor.execute("SELECT status, COUNT(*) FROM prospects GROUP BY status")
    for row in cursor.fetchall():
        print(f"Status: {row[0]} | Count: {row[1]}")

    print("\n--- Last 5 Prospects ---")
    cursor.execute("SELECT user_id, username, group_title, status, message_ts FROM prospects ORDER BY message_ts DESC LIMIT 5")
    for row in cursor.fetchall():
        print(f"User: {row[0]} (@{row[1]}) | Group: {row[2]} | Status: {row[3]} | TS: {row[4]}")

    conn.close()

if __name__ == "__main__":
    check_db()
