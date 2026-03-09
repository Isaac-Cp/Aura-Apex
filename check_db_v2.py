
import sqlite3
import os

DB_FILE = "gold_leads.db"

def check_groups():
    if not os.path.exists(DB_FILE):
        print(f"Database {DB_FILE} not found.")
        return

    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    print("--- Recently Joined Groups ---")
    cursor.execute("SELECT id, group_id, title, last_scanned_id, banned, archived FROM joined_groups ORDER BY id DESC LIMIT 10")
    for row in cursor.fetchall():
        print(row)

    print("\n--- Prospects Count ---")
    cursor.execute("SELECT status, COUNT(*) FROM prospects GROUP BY status")
    for row in cursor.fetchall():
        print(row)

    conn.close()

if __name__ == "__main__":
    check_groups()
