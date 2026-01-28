
import os

LOG_FILE = 'bot_error.log'
DUMP_FILE = 'log_dump.txt'

try:
    if not os.path.exists(LOG_FILE):
        print("Log file not found.")
        with open(DUMP_FILE, 'w') as f:
            f.write("Log file not found.")
    else:
        with open(LOG_FILE, 'rb') as f:
             # Read all because we can't easily seek from end in variable-width/utf-16 encoding safely without complex logic
             # but given 18KB size, it's small.
             data = f.read()
        
        content = ""
        try:
            content = data.decode('utf-16le')
        except:
            try:
                content = data.decode('utf-8', errors='ignore')
            except:
                content = "Decoding failed."

        lines = content.splitlines()
        last_lines = lines[-100:]
        
        with open(DUMP_FILE, 'w', encoding='utf-8') as f:
            for line in last_lines:
                f.write(line + "\n")
        print("Done.")

except Exception as e:
    with open(DUMP_FILE, 'w') as f:
        f.write(f"Error: {e}")
