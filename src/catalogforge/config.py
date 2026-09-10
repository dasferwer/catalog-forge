from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")
    database_url: str = "postgresql+asyncpg://catalogforge:catalogforge@database:5432/catalogforge"
    jwt_secret: str = Field(
        default="local-demo-catalogforge-replace-before-deployment", min_length=32
    )
    token_minutes: int = Field(default=60, ge=1, le=1440)
    testing: bool = False
    worker_interval: float = Field(default=0.5, ge=0.05, le=30)
    lease_seconds: float = Field(default=15, ge=1, le=300)
    upload_lease_seconds: int = Field(default=300, ge=30, le=1800)
    batch_size: int = Field(default=2000, ge=1, le=10000)
    max_upload_bytes: int = Field(default=268435456, ge=1024, le=1073741824)
    error_sample_limit: int = Field(default=1000, ge=1, le=10000)
    upload_dir: Path = Path("/data/imports")


settings = Settings()
