import uuid
from sqlalchemy import select

from app.models import Order, OutboxEvent


async def test_dual_write_is_atomic_on_success(db_session):
    """A successful order creation writes BOTH the order and its outbox
    event together, in one transaction."""
    order = Order(
        customer_email="alice@example.com",
        item="Keyboard",
        amount_cents=8999,
        idempotency_key=uuid.uuid4(),
    )
    db_session.add(order)
    await db_session.flush()

    outbox = OutboxEvent(
        aggregate_type="order",
        aggregate_id=str(order.id),
        event_type="order.created",
        payload={"order_id": str(order.id)},
    )
    db_session.add(outbox)
    await db_session.commit()

    orders = (await db_session.execute(select(Order))).scalars().all()
    events = (await db_session.execute(select(OutboxEvent))).scalars().all()

    assert len(orders) == 1
    assert len(events) == 1
    assert events[0].status.value == "PENDING" or events[0].status.value == "pending"


async def test_dual_write_rolls_back_completely_on_failure(db_session):
    """If something fails mid-transaction, NEITHER the order nor the
    outbox event should persist — proving atomicity, not just 'mostly works'."""
    order = Order(
        customer_email="bob@example.com",
        item="Mouse",
        amount_cents=2999,
        idempotency_key=uuid.uuid4(),
    )
    db_session.add(order)
    await db_session.flush()

    outbox = OutboxEvent(
        aggregate_type="order",
        aggregate_id=str(order.id),
        event_type="order.created",
        payload={"order_id": str(order.id)},
    )
    db_session.add(outbox)
    await db_session.flush()

    # Simulate a crash before commit.
    await db_session.rollback()

    orders = (await db_session.execute(select(Order))).scalars().all()
    events = (await db_session.execute(select(OutboxEvent))).scalars().all()

    assert len(orders) == 0
    assert len(events) == 0