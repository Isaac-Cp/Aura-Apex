import os
from dotenv import load_dotenv
from telethon.sync import TelegramClient
from telethon.sessions import StringSession

# Load environment variables
load_dotenv()

API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
PHONE_NUMBER = os.getenv("PHONE_NUMBER")

if not API_ID or not API_HASH:
    print("Error: API_ID or API_HASH not found in .env")
    exit(1)

print(f"Using API_ID: {API_ID}")
if PHONE_NUMBER:
    print(f"Using Phone: {PHONE_NUMBER}")

print("Initializing Telegram Client...")
print("Check your Telegram app for the login code.")

# Initialize client
client = TelegramClient(StringSession(), int(API_ID), API_HASH)

# Connect and start login flow
client.connect()

if not client.is_user_authorized():
    # This will trigger interactive login
    # If phone is provided, it skips asking for phone and goes straight to code
    client.start(phone=PHONE_NUMBER)

print("\n" + "="*50)
print("SUCCESS! Your session string has been generated.")
print("="*50 + "\n")

session_string = client.session.save()
print(session_string)

print("\n" + "="*50)
print("INSTRUCTIONS:")
print("1. Copy the long string above.")
print("2. Open your .env file.")
print("3. Replace the value of SESSION_STRING with this new string.")
print("4. Save the .env file.")
print("="*50 + "\n")
