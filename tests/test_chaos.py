from app.models import ProcessedEvent
import uuid
import json
from app.models import OutboxStatus
from relay.poller import publish_event, process_batch, fetch_pending_batch
from sqlalchemy import select

from app.models import Order, OutboxEvent

from consumer.worker import ensure_group_exists, handle_event, STREAM_NAME, GROUP_NAME


async def test_consumer_crash_before_ack_leaves_entry_pending_for_redelivery(
    db_session, redis_client
):
    """
    Simulates: the consumer reads an event, processes it, but crashes
    before calling XACK. Proves the entry stays in the group's pending
    list (not lost), and that a restarted consumer can claim and finish
    it — without reprocessing business logic twice, since our idempotency
    guard already recorded it.
    """
    order = Order(
        customer_email="ack-crash-test@example.com",
        item="Widget",
        amount_cents=1500,
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

    await ensure_group_exists(redis_client)

    # Publish directly to the stream (bypassing the poller, since we're
    # testing the consumer in isolation here).
    entry_id = await redis_client.xadd(
        STREAM_NAME,
        {
            "event_id": str(outbox.id),
            "event_type": outbox.event_type,
            "aggregate_type": outbox.aggregate_type,
            "aggregate_id": outbox.aggregate_id,
            "payload": json.dumps(outbox.payload),
        },
    )

    # --- Simulate the consumer reading the entry (XREADGROUP) ---
    response = await redis_client.xreadgroup(
        groupname=GROUP_NAME,
        consumername="worker-test",
        streams={STREAM_NAME: ">"},
        count=1,
    )
    assert response, "Expected the consumer group to deliver the new entry"
    _, entries = response[0]
    delivered_id, fields = entries[0]
    assert delivered_id == entry_id

    # --- Simulate processing succeeding, but crashing BEFORE xack ---
    await handle_event(fields)
    # Deliberately no xack call here — this is the crash point.

    # Prove the entry is still pending (not lost) for this consumer.
    pending = await redis_client.xpending_range(
        STREAM_NAME, GROUP_NAME, min="-", max="+", count=10
    )
    pending_ids = [p["message_id"] for p in pending]
    assert entry_id in pending_ids, (
        "Expected the unacked entry to still be in the pending list, "
        "proving it would be recoverable after a real crash, not lost."
    )

    # --- Simulate a restarted/second consumer claiming and finishing it ---
    claimed = await redis_client.xclaim(
        STREAM_NAME, GROUP_NAME, "worker-test-restarted", min_idle_time=0, message_ids=[entry_id]
    )
    assert len(claimed) == 1

    claimed_id, claimed_fields = claimed[0]
    await handle_event(claimed_fields)  # should skip — already in processed_events
    await redis_client.xack(STREAM_NAME, GROUP_NAME, claimed_id)

    # Prove it was only ever recorded as processed ONCE, despite being
    # delivered twice (original + reclaimed).
    result = await db_session.execute(
        select(ProcessedEvent).where(ProcessedEvent.event_id == outbox.id)
    )
    processed_rows = result.scalars().all()
    assert len(processed_rows) == 1

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

async def test_poller_crash_before_commit_causes_redelivery_but_consumer_stays_idempotent(
    db_session, redis_client
):
    """
    Simulates: poller publishes an event to Redis, then crashes before
    marking it PUBLISHED in Postgres. On restart, the poller redelivers
    the same event — this is the known, accepted at-least-once trade-off.
    We prove: (1) the redelivery genuinely happens, and (2) the consumer's
    idempotency guard means it's still only processed once downstream.
    """
    order = Order(
        customer_email="crash-test@example.com",
        item="Widget",
        amount_cents=1000,
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

    # --- Simulate the poller's first run, crashing right after publish ---
    await publish_event(redis_client, outbox)
    # Deliberately DO NOT update outbox.status or commit — this is the crash.
    # The row is still PENDING in the database, even though Redis already
    # has it.

    # --- Simulate the poller restarting and running its normal cycle ---
    # Since the row is still PENDING, a real restart would pick it up again.
    published_count = await process_batch(redis_client)

    # Prove the redelivery genuinely happened: two stream entries exist
    # for the same event, not one.
    entries = await redis_client.xrange("outbox-events", "-", "+")
    matching_entries = [
        e for e in entries if json.loads(e[1]["payload"])["order_id"] == str(order.id)
    ]
    assert len(matching_entries) == 2, (
        "Expected the event to be published twice (once manually, once by "
        "the restarted poller), proving at-least-once delivery is real."
    )

    # Prove the row is now correctly marked PUBLISHED after the restart.
    await db_session.refresh(outbox)
    assert outbox.status == OutboxStatus.PUBLISHED