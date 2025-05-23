from pydantic_settings import BaseSettings
from functools import lru_cache
import os
from dotenv import load_dotenv

# Database settings class definition
class Settings(BaseSettings):
    # Database settings
    DATABASE_HOST: str
    DATABASE_USER: str
    DATABASE_PASSWORD: str
    DATABASE_NAME: str
    
    # Environment
    ENV: str = "production"
    
    # Schema prefix (empty for production, "_stg" for staging)
    SCHEMA_SUFFIX: str = ""
    
    # API Key
    API_KEY: str
    
    class Config:
        env_file = ".env"

# get settings function 
# making comment for testing
@lru_cache()
def get_settings():
    # Load environment variables from .env file
    load_dotenv()
    
    # Determine environment
    # default to production
    env = os.getenv("ENV", "production")

    if not os.getenv('ENV'):
        print("Warning: cannot find ENV variable")
        print("Default setting is production")

    print(f"Running in {env} environment")
    
    # Set schema suffix based on environment
    schema_suffix = "_stg" if env == "staging" else ""
    
    # Create settings with environment-specific values
    return Settings(
        ENV=env,
        SCHEMA_SUFFIX=schema_suffix,
        DATABASE_HOST=os.getenv("DATABASE_HOST"),
        DATABASE_USER=os.getenv("DATABASE_USER"),
        DATABASE_PASSWORD=os.getenv("DATABASE_PASSWORD"),
        DATABASE_NAME=os.getenv("DATABASE_NAME"),
        API_KEY=os.getenv("API_KEY")
    ) 