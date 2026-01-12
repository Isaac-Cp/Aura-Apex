
import os
from dotenv import load_dotenv

load_dotenv()

API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
PHONE_NUMBER = os.getenv("PHONE_NUMBER")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

# Validate that we have what we need
if not API_ID or not API_HASH or not PHONE_NUMBER:
    print("Error: Missing API_ID, API_HASH, or PHONE_NUMBER in .env file.")
    # We don't exit here to allow main.py to handle it or prompt cleanly
