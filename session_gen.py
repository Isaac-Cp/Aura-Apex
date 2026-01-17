import os
import asyncio
import time
from telethon import TelegramClient
from telethon.sessions import StringSession
from dotenv import load_dotenv
from keep_alive import keep_alive

def _code_callback():
    env_code = (os.environ.get("TELEGRAM_CODE") or "").strip()
    if env_code:
        return env_code
    path = os.path.join(os.getcwd(), "WAITING_FOR_CODE")
    deadline = time.time() + 300
    while time.time() < deadline:
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    code = f.read().strip()
                os.remove(path)
                if code:
                    return code
            except Exception:
                pass
        time.sleep(2)
    raise RuntimeError("No code received within 5 minutes. POST the code to /code and rerun.")

async def main_async():
    load_dotenv()
    api_id = os.environ.get("API_ID")
    api_hash = os.environ.get("API_HASH")
    phone = os.environ.get("PHONE_NUMBER")
    password = os.environ.get("TELEGRAM_PASSWORD")  # optional 2FA
    if not api_id or not api_hash or not phone:
        raise SystemExit("Missing API_ID, API_HASH, or PHONE_NUMBER in environment/.env")
    try:
        api_id_int = int(api_id)
    except Exception:
        raise SystemExit("API_ID must be an integer.")
    keep_alive()
    loop = asyncio.get_event_loop()
    client = TelegramClient(StringSession(), api_id_int, api_hash, loop=loop)
    await client.start(phone=phone, code_callback=_code_callback, password=password)
    session_string = client.session.save()
    print("\nYour Session String is:\n")
    print(session_string)
    print("\nCopy this entire string and set it as SESSION_STRING in your server environment.")
    await client.disconnect()

if __name__ == "__main__":
    try:
        asyncio.set_event_loop(asyncio.new_event_loop())
    except Exception:
        pass
    asyncio.run(main_async())
