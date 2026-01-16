from flask import Flask
from threading import Thread
import os
import logging

# Disable Flask startup logs to keep console clean
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

app = Flask('')

@app.route('/')
def home():
    return "Aura Apex Supreme is alive!"

@app.route('/health')
def health():
    return "OK", 200

def run():
    try:
        # Use PORT env var if available, default to 8080
        port = int(os.environ.get("PORT", 8080))
        app.run(host='0.0.0.0', port=port)
    except Exception as e:
        print(f"Web server failed to start: {e}")

def keep_alive():
    t = Thread(target=run)
    t.daemon = True
    t.start()
