from flask import Flask, request
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

@app.route('/code', methods=['POST'])
def set_code():
    try:
        code = None
        if request.is_json:
            data = request.get_json(silent=True) or {}
            code = (data.get("code") or "").strip()
        else:
            code = (request.form.get("code") or "").strip()
        if not code:
            return ("Missing code", 400)
        path = os.path.join(os.getcwd(), "WAITING_FOR_CODE")
        with open(path, "w", encoding="utf-8") as f:
            f.write(code)
        return ("OK", 200)
    except Exception as e:
        return (f"Error: {e}", 500)

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
