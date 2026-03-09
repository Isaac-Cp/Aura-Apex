
import sqlite3
import os

DB_FILE = "gold_leads.db"

def check_dm_sent():
    if not os.path.exists(DB_FILE):
        print(f"Database {DB_FILE} not found.")
        return

    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    print("--- DM Sent Log ---")
    cursor.execute("SELECT ts, type, details FROM activity_log WHERE type='dm_sent' ORDER BY ts DESC LIMIT 10")
    rows = cursor.fetchall()
    if not rows:
        print("No DM sent yet.")
    for row in rows:
        print(f"{row[0]} | {row[1]} | {row[2]}")

    print("\n--- Prospects Status ---")
    cursor.execute("SELECT status, COUNT(*) FROM prospects GROUP BY status")
    for row in cursor.fetchall():
        print(f"Status: {row[0]} | Count: {row[1]}")

    conn.close()

if __name__ == "__main__":
    check_dm_sent()
