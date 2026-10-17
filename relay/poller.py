import asyncio
import json
import logging
from datetime import datetime, timezone

import redis.asyncio as redis
from sqlalchemy import select

from app.config import settings
from app.db import async_session
from app.models import OutboxEvent, OutboxStatus

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("poller")

POLL_INTERVAL_SECONDS = 2
BATCH_SIZE = 10
MAX_ATTEMPTS = 5
STREAM_NAME = "outbox-events"


async def fetch_pending_batch(session):
    result = await session.execute(
        select(OutboxEvent)
        .where(OutboxEvent.status == OutboxStatus.PENDING)
        .order_by(OutboxEvent.created_at)
        .limit(BATCH_SIZE)
        .with_for_update(skip_locked=True)
    )
    return result.scalars().all()


async def publish_event(redis_client: redis.Redis, event: OutboxEvent):
    await redis_client.xadd(
        STREAM_NAME,
        {
            "event_id": str(event.id),
            "event_type": event.event_type,
            "aggregate_type": event.aggregate_type,
            "aggregate_id": event.aggregate_id,
            "payload": json.dumps(event.payload),
        },
    )


async def process_batch(redis_client: redis.Redis) -> int:
    async with async_session() as session:
        events = await fetch_pending_batch(session)

        if not events:
            return 0

        for event in events:
            try:
                await publish_event(redis_client, event)
                event.status = OutboxStatus.PUBLISHED
                event.published_at = datetime.now(timezone.utc)
                logger.info(f"Published event {event.id} ({event.event_type})")
            except Exception as exc:
                event.attempts += 1
                logger.error(f"Failed to publish event {event.id}: {exc}")
                if event.attempts >= MAX_ATTEMPTS:
                    event.status = OutboxStatus.FAILED
                    logger.error(
                        f"Event {event.id} marked FAILED after {MAX_ATTEMPTS} attempts"
                    )

        await session.commit()
        return len(events)


async def run_poller():
    redis_client = redis.from_url(settings.redis_url, decode_responses=True)
    logger.info("Poller started. Polling for outbox events...")

    try:
        while True:
            count = await process_batch(redis_client)
            if count == 0:
                await asyncio.sleep(POLL_INTERVAL_SECONDS)
    finally:
        await redis_client.aclose()


if __name__ == "__main__":
    asyncio.run(run_poller())