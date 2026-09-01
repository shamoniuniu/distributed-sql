"""Environment-backed service configuration."""

from functools import lru_cache
from pathlib import Path

from pydantic import AliasChoices, Field, PositiveFloat, PositiveInt
from pydantic_settings import BaseSettings, SettingsConfigDict


class CoordinatorSettings(BaseSettings):
    """Coordinator process configuration."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="DISTRIBUTED_SQL_COORDINATOR_",
        extra="ignore",
    )

    host: str = "127.0.0.1"
    port: int = Field(default=8080, ge=1, le=65535)
    lease_ttl_seconds: PositiveFloat = 6.0
    lease_check_interval_seconds: PositiveFloat = 1.0
    catalog_path: Path = Path("data/catalog.db")
    object_store_endpoint: str | None = None
    object_store_access_key: str | None = None
    object_store_secret_key: str | None = None
    object_store_bucket: str | None = None
    object_store_region: str = "us-east-1"
    object_store_secure: bool = True
    object_store_create_bucket: bool = False
    object_store_runtime_prefix: str = "runtime"
    remote_task_auth_token: str | None = None
    cancellation_timeout_seconds: PositiveFloat = 10.0
    log_level: str = "info"

    @property
    def runtime_root(self) -> str:
        if self.object_store_bucket:
            prefix = self.object_store_runtime_prefix.strip("/")
            return f"s3://{self.object_store_bucket}/{prefix}"
        return str((self.catalog_path.parent / "runtime").resolve())


class WorkerSettings(BaseSettings):
    """Worker process and registration configuration."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="DISTRIBUTED_SQL_WORKER_",
        extra="ignore",
    )

    worker_id: str | None = Field(
        default=None,
        validation_alias=AliasChoices("worker_id", "DISTRIBUTED_SQL_WORKER_ID"),
    )
    host: str = "127.0.0.1"
    advertised_host: str | None = None
    port: int = Field(default=8091, ge=1, le=65535)
    coordinator_url: str = "http://127.0.0.1:8080"
    heartbeat_interval_seconds: PositiveFloat = 2.0
    registration_retry_seconds: PositiveFloat = 1.0
    slots: PositiveInt = 1
    memory_limit_bytes: PositiveInt = 64 * 1024 * 1024
    temp_directory: Path = Path("data/tmp")
    object_store_endpoint: str | None = None
    object_store_access_key: str | None = None
    object_store_secret_key: str | None = None
    object_store_bucket: str | None = None
    object_store_region: str = "us-east-1"
    object_store_secure: bool = True
    remote_task_auth_token: str | None = None
    cancellation_timeout_seconds: PositiveFloat = 5.0
    task_start_delay_seconds: float = Field(default=0.0, ge=0)
    log_level: str = "info"

    @property
    def endpoint(self) -> str:
        return f"http://{self.advertised_host or self.host}:{self.port}"


class LocalClusterSettings(BaseSettings):
    """Defaults used by the local multi-process launcher."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="DISTRIBUTED_SQL_LOCAL_",
        extra="ignore",
    )

    worker_count: PositiveInt = 2
    worker_start_port: int = Field(default=8091, ge=1, le=65535)
    startup_timeout_seconds: PositiveFloat = 20.0


@lru_cache
def get_coordinator_settings() -> CoordinatorSettings:
    return CoordinatorSettings()


@lru_cache
def get_worker_settings() -> WorkerSettings:
    return WorkerSettings()
