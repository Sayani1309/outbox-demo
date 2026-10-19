import asyncio
import json
import logging
import uuid

import redis.asyncio as redis
from sqlalchemy.exc import IntegrityError

from app.config import settings
from app.db import async_session
from app.models import ProcessedEvent

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("consumer")

STREAM_NAME = "outbox-events"
GROUP_NAME = "order-processors"
CONSUMER_NAME = "worker-1"
BLOCK_MS = 5000
BATCH_SIZE = 10


async def ensure_group_exists(redis_client: redis.Redis):
    """Creates the consumer group if it doesn't already exist. Safe to call
    every time the worker starts."""
    try:
        await redis_client.xgroup_create(
            name=STREAM_NAME, groupname=GROUP_NAME, id="0", mkstream=True
        )
        logger.info(f"Created consumer group '{GROUP_NAME}'")
    except redis.ResponseError as e:
        if "BUSYGROUP" in str(e):
            logger.info(f"Consumer group '{GROUP_NAME}' already exists")
        else:
            raise


async def already_processed(event_id: uuid.UUID) -> bool:
    """Checks our own idempotency ledger — have we handled this event before?"""
    async with async_session() as session:
        existing = await session.get(ProcessedEvent, event_id)
        return existing is not None


async def mark_processed(event_id: uuid.UUID) -> bool:
    """
    Records that we've processed this event. Returns True if this is the
    first time (we actually inserted a new row), False if a concurrent
    worker already beat us to it (caught via the primary key constraint).
    """
    async with async_session() as session:
        session.add(ProcessedEvent(event_id=event_id))
        try:
            await session.commit()
            return True
        except IntegrityError:
            await session.rollback()
            return False


async def handle_event(fields: dict) -> None:
    """
    The actual 'business logic' for an event. In a real system this might
    send an email, update a search index, charge a card, etc. Here we just
    simulate the work with a log line.
    """
    event_id = uuid.UUID(fields["event_id"])
    event_type = fields["event_type"]
    payload = json.loads(fields["payload"])

    if await already_processed(event_id):
        logger.info(f"Event {event_id} already processed — skipping (duplicate).")
        return

    # Simulate doing real work, e.g. sending a confirmation email.
    logger.info(
        f"Processing {event_type} for order {payload.get('order_id')} "
        f"(customer: {payload.get('customer_email')})"
    )
    await asyncio.sleep(0.1)  # pretend this takes a moment

    was_new = await mark_processed(event_id)
    if was_new:
        logger.info(f"Marked event {event_id} as processed.")
    else:
        logger.info(f"Event {event_id} was processed by another worker concurrently.")


async def run_consumer():
    redis_client = redis.from_url( settings.redis_url,
        decode_responses=True,
        socket_timeout=BLOCK_MS / 1000 + 5,  
        socket_connect_timeout=5,)
    await ensure_group_exists(redis_client)
    logger.info(f"Consumer '{CONSUMER_NAME}' started, listening on '{STREAM_NAME}'...")

    try:
        while True:
            response = await redis_client.xreadgroup(
                groupname=GROUP_NAME,
                consumername=CONSUMER_NAME,
                streams={STREAM_NAME: ">"},
                count=BATCH_SIZE,
                block=BLOCK_MS,
            )

            if not response:
                continue  # timed out waiting, loop again

            for stream_name, entries in response:
                for entry_id, fields in entries:
                    try:
                        await handle_event(fields)
                        await redis_client.xack(STREAM_NAME, GROUP_NAME, entry_id)
                    except Exception:
                        logger.exception(
                            f"Failed to process entry {entry_id} — leaving unacked "
                            f"for redelivery."
                        )
    finally:
        await redis_client.aclose()


if __name__ == "__main__":
    asyncio.run(run_consumer())