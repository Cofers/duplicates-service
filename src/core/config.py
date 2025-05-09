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
    REDIS_HOST: str 
    REDIS_PORT: int 
    REDIS_DB: int 

    # Google Cloud Configuration
    GCP_PROJECT: str
    

    # BigQuery Configuration
    BIGQUERY_DATASET: str 
    BIGQUERY_TABLE: str 

    # Pub/Sub Configuration
    PUBSUB_TOPIC: str = "nose"
    PUBSUB_SUBSCRIPTION: str = "duplicates-detector"

    class Config:
        env_file = ".env"
        case_sensitive = True


@lru_cache()
def get_settings() -> Settings:
    """
    Get cached settings instance.
    """
    return Settings() 