
import sqlite3
import json

def check_stats():
    try:
        conn = sqlite3.connect("gold_leads.db")
        cur = conn.cursor()
        cur.execute("SELECT data FROM supreme_stats WHERE id=1")
        row = cur.fetchone()
        if row:
            print(f"Stats Data: {row[0]}")
        else:
            print("No stats data found in DB.")
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_stats()
