from pydantic_settings import BaseSettings
from functools import lru_cache
import os
from dotenv import load_dotenv

class Settings(BaseSettings):
    # Database settings
    DATABASE_HOST: str
    DATABASE_USER: str
    DATABASE_PASSWORD: str
    DATABASE_NAME: str
    
    # Environment
    ENV: str = "development"
    
    # Schema prefix (empty for production, "_stg" for staging)
    SCHEMA_SUFFIX: str = ""
    
    class Config:
        env_file = ".env"

@lru_cache()
def get_settings():
    # Load environment variables from .env file
    load_dotenv()
    
    # Determine environment
    env = os.getenv("ENV", "development")
    
    # Set schema suffix based on environment
    schema_suffix = "_stg" if env == "development" else ""
    
    # Create settings with environment-specific values
    return Settings(
        ENV=env,
        SCHEMA_SUFFIX=schema_suffix,
        DATABASE_HOST=os.getenv("DATABASE_HOST"),
        DATABASE_USER=os.getenv("DATABASE_USER"),
        DATABASE_PASSWORD=os.getenv("DATABASE_PASSWORD"),
        DATABASE_NAME=os.getenv("DATABASE_NAME")
    ) 