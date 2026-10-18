"""Process-wide settings, read from environment variables and `.env` (prefix `TOEVAL_`)."""

from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

REPO_ROOT = Path(__file__).resolve().parents[2]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="TOEVAL_",
        env_file=REPO_ROOT / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    data_dir: Path = Field(default=Path("data"), description="Warehouse root.")
    log_level: str = "INFO"
    sources_file: Path = Field(
        default=Path("sources.toml"),
        description="Optional per-source/stream overrides of schedule, windows and rate limits.",
    )
    user_agent: str = "toeval/0.1 (+https://github.com/AlexanderJVP/toeval)"

    def _resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else REPO_ROOT / path

    @property
    def data_path(self) -> Path:
        return self._resolve(self.data_dir)

    @property
    def raw_path(self) -> Path:
        return self.data_path / "raw"

    @property
    def quarantine_path(self) -> Path:
        """Batches that failed hard quality checks; deliberately outside raw_path."""
        return self.data_path / "quarantine"

    @property
    def log_path(self) -> Path:
        return self.data_path / "logs"

    @property
    def state_db_path(self) -> Path:
        return self.data_path / "state" / "state.duckdb"

    @property
    def lock_path(self) -> Path:
        return self.data_path / "state" / "ingest.lock"

    @property
    def sources_path(self) -> Path:
        return self._resolve(self.sources_file)


@lru_cache
def get_settings() -> Settings:
    return Settings()
