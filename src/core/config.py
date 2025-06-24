from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    """
    Application settings.
    """
    # API Configuration
    API_HOST: str = "0.0.0.0"
    API_PORT: int = 8000
    DEBUG: bool = False

    # Redis Configuration
    REDIS_HOST: str = "127.0.0.1"
    REDIS_PORT: int = 6379
    REDIS_DB: int = 2  # Changed to use database 2 by default

    # Google Cloud Configuration
    GCP_PROJECT: str = "production-400914"
    

    # BigQuery Configuration
    BIGQUERY_DATASET: str 
    BIGQUERY_TABLE: str 

    # Pub/Sub Configuration
    PUBSUB_TOPIC: str = "nose"
    PUBSUB_SUBSCRIPTION: str = "duplicates-detector"

    # Duplicates Configuration
    DUPLICATE_EXACT_TTL: int = 1296000  # 15 días en segundos
    DUPLICATE_PATTERN_TTL: int = 31536000  # 1 año en segundos
    PATTERN_MONTHS_LOOKBACK: list[int] = [1, 2, 3, 4, 5, 6]  # Meses a revisar para patrones

    class Config:
        env_file = ".env"
        case_sensitive = True


@lru_cache()
def get_settings() -> Settings:
    """
    Get cached settings instance.
    """
    return Settings() 