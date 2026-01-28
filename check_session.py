
import os
import asyncio
from telethon import TelegramClient
from telethon.sessions import StringSession
from dotenv import load_dotenv

load_dotenv()

API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
SESSION_STRING = os.getenv("SESSION_STRING")

print(f"API_ID: {API_ID}")
print(f"API_HASH: {API_HASH}")
print(f"SESSION_STRING Length: {len(SESSION_STRING) if SESSION_STRING else 'None'}")

if not SESSION_STRING:
    print("Error: SESSION_STRING not found in .env")
    exit(1)

async def check():
    client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
    try:
        print("Connecting...")
        await client.connect()
        if await client.is_user_authorized():
            me = await client.get_me()
            print(f"Success! Logged in as: {me.username or me.first_name} ({me.id})")
        else:
            print("Failed: Session string exists but user is not authorized.")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        await client.disconnect()

asyncio.run(check())
