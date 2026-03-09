
import asyncio
from telethon import TelegramClient
import os
from dotenv import load_dotenv

load_dotenv()
API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
SESSION_STRING = os.getenv("SESSION_STRING")

async def check_msgs():
    from telethon.sessions import StringSession
    client = TelegramClient(StringSession(SESSION_STRING), int(API_ID), API_HASH)
    await client.connect()
    
    group_id = 1517346868 # IPTV HELP GROUP
    print(f"--- Messages from {group_id} ---")
    async for m in client.iter_messages(group_id, limit=20):
        print(f"ID: {m.id} | User: {m.sender_id} | Text: {m.text[:50] if m.text else 'No text'}")
    
    await client.disconnect()

if __name__ == "__main__":
    asyncio.run(check_msgs())
