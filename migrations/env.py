import asyncio
from logging.config import fileConfig

from sqlalchemy import pool
from sqlalchemy.ext.asyncio import async_engine_from_config

from alembic import context

from app.config import settings
from app.db import Base
from app.models import Order, OutboxEvent  # noqa: F401 — import so Alembic sees them

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# This tells Alembic what your models SHOULD look like — it compares this
# against the actual database schema to figure out what changed.
target_metadata = Base.metadata


def get_url() -> str:
    return settings.database_url.render_as_string(hide_password=False)


def run_migrations_offline() -> None:
    """Generate SQL scripts without connecting to a live database."""
    url = get_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection) -> None:
    context.configure(connection=connection, target_metadata=target_metadata)
    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_online() -> None:
    """Connect to the real database and apply migrations."""
    configuration = config.get_section(config.config_ini_section) or {}
    configuration["sqlalchemy.url"] = get_url()

    connectable = async_engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)

    await connectable.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_migrations_online())