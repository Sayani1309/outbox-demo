import pytest_asyncio
import redis.asyncio as redis
from sqlalchemy import text

from app.config import settings
from app.db import engine, async_session


async def _clean_tables():
    async with engine.begin() as conn:
        await conn.execute(text("DELETE FROM processed_events"))
        await conn.execute(text("DELETE FROM outbox_events"))
        await conn.execute(text("DELETE FROM orders"))


@pytest_asyncio.fixture
async def db_session():
    """
    Gives each test a clean slate before it runs, and cleans up again
    after, so tests never see stale data from manual testing or from
    each other.
    """
    await _clean_tables()

    async with async_session() as session:
        yield session
        # Reset the session to a clean state before the fixture closes it,
        # in case a test deliberately left it mid-transaction (chaos tests do).
        await session.rollback()

    await _clean_tables()


@pytest_asyncio.fixture
async def redis_client():
    client = redis.from_url(settings.redis_url, decode_responses=True)
    yield client

    try:
        await client.delete("outbox-events")
    except Exception:
        pass
    await client.aclose()