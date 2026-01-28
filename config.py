
import os
from dotenv import load_dotenv

load_dotenv()

API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
PHONE_NUMBER = os.getenv("PHONE_NUMBER")
SESSION_STRING = os.getenv("SESSION_STRING")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
CURATOR_CHANNEL_ID = os.getenv("CURATOR_CHANNEL_ID")

BANNED_ZONES = [
    "AF", "AL", "DZ", "AO", "BY", "BO", "VG", "BF", "BI", "CM", "CF", "TD", "CU", "CD",
    "ER", "GN", "GW", "HT", "IR", "IQ", "KP", "LA", "LB", "LY", "ML", "MD", "MC", "MZ",
    "MM", "NA", "NP", "NI", "NE", "NG", "PK", "PS", "RU", "SO", "SS", "SD", "SY", "TJ",
    "TT", "TN", "UA", "UZ", "VU", "VE", "VN", "YE", "BD", "TR", "SD", "ZW"
]

BANNED_CURRENCIES = [
    "AFN", "AOA", "ARS", "BYN", "BOB", "CDF", "CUP", "ERN", "ETB", "GHS", "GNF", "HTG",
    "IRR", "IQD", "KPW", "LAK", "LBP", "LRD", "LYD", "MGA", "MWK", "MMK", "MZN", "NGN",
    "PKR", "PYG", "SLL", "SOS", "SSP", "SDG", "SYP", "TJS", "TRY", "TMT", "UGX", "UZS",
    "VES", "VND", "YER", "ZMW", "ZWL", "KHR", "LKR", "RSD", "GEL", "AMD", "AZN", "KGS"
]

JUNK_KEYWORDS = [
    "100% free", "cracked", "bin", "carding", "hack", "giveaway", "follow4follow",
    "sub4sub", "megalink", "cheap vcc", "premium apk free", "unlimited credits",
    "no payment needed", "leak", "dump", "tutorial free", "bypass paywall",
    "get rich fast", "instant cash", "verified cc", "fullz", "scampage", "phishing",
    "spammer", "promo code 100", "free trial no card", "unlimited trial", "bot for hire",
    "dm for link", "visit my channel", "cheap followers", "crypto signal free", "airdrop",
    "gift card generator", "cracked firestick", "unlocked box", "free m3u list",
    "daily m3u", "iptv link free", "xtream codes free", "login free", "password list",
    "stbemu free", "mac portal free", "free activation", "no cost", "complimentary",
    "gifted", "hidden link", "earn money"
]

TIER_3_CODES = [
    "PH", "IN", "BR", "MX", "DZ", "MA", "EG", "ZA", "CO", "MY", "TH", "PE", "KE", "BD",
    "AR", "CL", "EC", "GT", "HN", "ID", "JO", "KZ", "KW", "KG", "LY", "MU", "MN", "ME",
    "OM", "PA", "QA", "SA", "RS", "LK", "TW", "TJ", "UA", "AE", "UY", "UZ", "VN", "GH",
    "CI", "SN", "TZ", "UG", "ZM", "ZW", "NP"
]

TIER_1_INDICATORS = [
    "UK", "USA", "CA", "DE", "FR", "AU", "CH", "NL", "NO", "SE", "JP", "IE", "NZ", "AT",
    "BE", "DK", "FI", "IS", "IT", "LU", "SG", "ES", "KR", "HK", "IL", "PT", "GR", "CZ",
    "HU", "PL", "RO", "SK", "SI", "EE", "LV", "LT", "MT", "CY", "US", "GB", "Great Britain",
    "America", "Canada", "Australia", "Germany", "France", "Switzerland", "Netherlands", "Sweden"
]

URGENCY_KEYWORDS = [
    "immediately", "now", "trial link", "ready to pay", "kickoff", "help urgent",
    "expiring", "renewal", "black screen", "looping", "buffering during", "admin help",
    "buy credits now", "setup guide", "asap", "starting in 5", "can't login",
    "payment portal", "buy now", "active subscription", "urgent fix", "match live",
    "ppv link", "pay via crypto", "paypal ready", "stripe link", "instant delivery",
    "activate now", "working link", "stable server", "no lag", "high speed", "8k stream",
    "4k stable", "reliable", "best provider", "recommend me", "looking for", "need new",
    "switch provider", "too much buffering", "service down", "offline", "not responding",
    "portal error", "m3u fix", "playlist error", "dns change", "server move"
]

SENTIMENT_BLACKLIST = [
    "scam", "fraud", "stole", "avoid", "fake", "bad service", "liar", "thief", "terrible",
    "don't buy", "garbage", "waste of money", "stay away", "blocked me", "refused refund",
    "admin is scammer", "fake proof", "stolen", "ripped off", "cheated", "unreliable",
    "slow support", "never reply", "don't trust", "warning", "be careful", "danger",
    "malware", "virus", "hacked", "identity theft", "police", "legal action", "sue",
    "complaint", "worst", "horrible", "awful", "disaster", "ruined", "disappointed",
    "poor quality", "pixelated", "freezing constantly", "total scam", "do not use",
    "don't subscribe", "stolen mac", "portal dead"
]

if not API_ID or not API_HASH or not PHONE_NUMBER:
    print("Error: Missing API_ID, API_HASH, or PHONE_NUMBER in .env file.")
