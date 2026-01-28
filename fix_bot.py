import asyncio
import os
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.functions.contacts import UnblockRequest
from dotenv import load_dotenv

load_dotenv()

API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
SESSION_NAME = 'aura_apex_supreme_session'

# Bots to unblock
SCOUT_BOTS = [
    "@GroupFinderBot", 
    "@GroupSearchBot", 
    "@TgFinderBot", 
    "@GroupHelpBot", 
    "@ChannelRadarBot"
]

async def main():
    print("--- FIX BOT: Unblocking Scouters ---")
    client = TelegramClient(SESSION_NAME, int(API_ID), API_HASH)
    
    try:
        await client.connect()
        if not await client.is_user_authorized():
            print("Client not authorized! Session might be invalid.")
            return

        print("Client connected successfully.")
        
        me = await client.get_me()
        print(f"Logged in as: {me.username} ({me.id})")

        for bot_username in SCOUT_BOTS:
            try:
                print(f"Unblocking {bot_username}...")
                entity = await client.get_input_entity(bot_username)
                await client(UnblockRequest(id=entity))
                print(f"Unblocked {bot_username} SUCCESS")
                
                # Optional: Send a test /start command to verify
                # print(f"Sending /start to {bot_username}...")
                # await client.send_message(entity, "/start")
                # print(f"Sent /start to {bot_username}")
                
            except Exception as e:
                print(f"Failed to unblock {bot_username}: {e}")

        print("\n--- FIX COMPLETE ---")
        
    except Exception as e:
        print(f"Critical Error: {e}")
    finally:
        await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
