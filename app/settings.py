from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    app_name: str = "mini-failover-demo"

    db_driver: str = "postgresql+asyncpg"
    postgres_user: str = "postgres"
    postgres_password: SecretStr = SecretStr("postgres")
    postgres_db: str = "postgres"

    db_primary_host: SecretStr = SecretStr("127.0.0.1")
    db_primary_port: int = 5432

    db_secondary_host: SecretStr = SecretStr("127.0.0.1")
    db_secondary_port: int = 5433

    db_echo: bool = False

    db_pool_size: int = 2
    db_max_overflow: int = 2
    db_pool_timeout_in_seconds: int = 10
    db_pool_recycle_in_seconds: int = 1800
    tcp_connect_timeout_in_seconds: int = 5

    postgres_statement_timeout_str: str = "5000"  # ms

    db_host_health_check_interval_in_seconds: int = 2
    db_host_cooldown_interval_in_seconds: int = 2
    db_host_writable_timeout_in_seconds: int = 10


settings = Settings()
