from sqlalchemy import select, func

from app.db import async_session
from app.models import Order, OutboxEvent, OutboxStatus, ProcessedEvent


async def get_outbox_counts() -> dict:
    """Counts outbox events grouped by status."""
    async with async_session() as session:
        result = await session.execute(
            select(OutboxEvent.status, func.count(OutboxEvent.id))
            .group_by(OutboxEvent.status)
        )
        counts = {status.name: count for status, count in result.all()}

    return {
        "pending": counts.get("PENDING", 0),
        "published": counts.get("PUBLISHED", 0),
        "failed": counts.get("FAILED", 0),
    }


async def get_avg_publish_latency_seconds() -> float | None:
    """
    Average time between an outbox event's created_at and published_at,
    for events that have actually been published. Returns None if there's
    no published data yet (avoids a misleading 0.0).
    """
    async with async_session() as session:
        result = await session.execute(
            select(
                func.avg(
                    func.extract(
                        "epoch", OutboxEvent.published_at - OutboxEvent.created_at
                    )
                )
            ).where(OutboxEvent.status == OutboxStatus.PUBLISHED)
        )
        avg_seconds = result.scalar_one_or_none()

    return float(avg_seconds) if avg_seconds is not None else None


async def get_processed_events_count() -> int:
    async with async_session() as session:
        result = await session.execute(select(func.count(ProcessedEvent.event_id)))
        return result.scalar_one()


async def get_orders_count() -> int:
    async with async_session() as session:
        result = await session.execute(select(func.count(Order.id)))
        return result.scalar_one()