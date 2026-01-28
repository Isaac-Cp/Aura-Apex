
import json
import time
import os
import datetime
import sys

# Define constants from config/code logic
STATS_FILE = 'supreme_stats.json'
LOG_FILE = 'bot_error.log'

def read_logs_safely():
    print("\n--- Log Analysis (Last 50 lines) ---")
    if not os.path.exists(LOG_FILE):
        print(f"{LOG_FILE} not found.")
        return

    try:
        mtime = os.path.getmtime(LOG_FILE)
        print(f"Log file last modified: {datetime.datetime.fromtimestamp(mtime)}")
        
        size = os.path.getsize(LOG_FILE)
        read_size = min(size, 8000)
        with open(LOG_FILE, 'rb') as f:
            f.seek(-read_size, os.SEEK_END)
            data = f.read()
            
        # Try decoding
        content = ""
        try:
            content = data.decode('utf-16le')
        except:
            try:
                content = data.decode('utf-8', errors='ignore')
            except:
                content = str(data)
        
        lines = content.splitlines()
        for line in lines[-50:]:
            # Encode to ascii to prevent console crashing
            safe_line = line.encode('ascii', 'backslashreplace').decode('ascii')
            print(safe_line)
            
    except Exception as e:
        print(f"Error reading logs: {e}")

if __name__ == "__main__":
    read_logs_safely()
