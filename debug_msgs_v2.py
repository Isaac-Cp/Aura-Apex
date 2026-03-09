
import asyncio
from telethon import TelegramClient
import os
from dotenv import load_dotenv

load_dotenv()
API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
SESSION_STRING = os.getenv("SESSION_STRING")

async def check_msgs():
    try:
        from telethon.sessions import StringSession
        client = TelegramClient(StringSession(SESSION_STRING), int(API_ID), API_HASH)
        print("Connecting...")
        await client.connect()
        print("Connected.")
        
        print("Dialogs:")
        async for d in client.iter_dialogs(limit=10):
            print(f"ID: {d.id} | Title: {d.title}")
            
        await client.disconnect()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(check_msgs())
