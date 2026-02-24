from typing import List
from pydantic_settings import BaseSettings
from pydantic_settings import SettingsConfigDict
from pydantic import AnyHttpUrl
import os
from dotenv import load_dotenv

load_dotenv()

class Settings(BaseSettings):
    # Ignore unrelated environment variables (e.g. OPENAI_API_KEY) so the app
    # can run in shared environments without crashing.
    model_config = SettingsConfigDict(
        case_sensitive=True,
        env_file=".env",
        extra="ignore",
    )

    PROJECT_NAME: str = "Stock FastAPI"
    VERSION: str = "1.0.0"
    DESCRIPTION: str = "A FastAPI application for stock management"
    API_V1_STR: str = "/api/v1"
    
    # CORS
    ALLOWED_ORIGINS: List[str] = ["*"]
    
    # Database
    DATABASE_URL: str = os.getenv("DATABASE_URL", "postgresql://user:password@localhost/stockdb")
    
    # Security
    SECRET_KEY: str = os.getenv("SECRET_KEY", "your-secret-key-change-in-production")
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30
    
    # Environment
    ENVIRONMENT: str = os.getenv("ENVIRONMENT", "development")
    DEBUG: bool = os.getenv("DEBUG", "True").lower() in ("true", "1", "yes")
    RELOAD: bool = os.getenv("RELOAD", "True").lower() in ("true", "1", "yes")
    PORT: int = int(os.getenv("PORT", "8000"))

settings = Settings()