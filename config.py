
from typing import Optional
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    API_ID: str
    API_HASH: str
    PHONE_NUMBER: str
    GEMINI_API_KEY: Optional[str] = None
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", case_sensitive=False)

try:
    _settings = Settings()
    API_ID = _settings.API_ID
    API_HASH = _settings.API_HASH
    PHONE_NUMBER = _settings.PHONE_NUMBER
    GEMINI_API_KEY = _settings.GEMINI_API_KEY
except Exception as _e:
    print("Error: Configuration invalid or missing keys in environment/.env.")
    API_ID = None
    API_HASH = None
    PHONE_NUMBER = None
    GEMINI_API_KEY = None
