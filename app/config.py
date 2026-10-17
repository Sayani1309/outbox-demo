from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import URL


class Settings(BaseSettings):
    postgres_db: str
    postgres_app_user: str
    postgres_app_password: str
    postgres_port: int = 5432
    postgres_host: str = "localhost"

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    @property
    def database_url(self) -> URL:
        return URL.create(
            drivername="postgresql+asyncpg",
            username=self.postgres_app_user.strip(),
            password=self.postgres_app_password.strip(),
            host=self.postgres_host.strip(),
            port=self.postgres_port,
            database=self.postgres_db.strip(),
        )


settings = Settings()