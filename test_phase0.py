import asyncio
from sqlalchemy import select, text

from app.db import engine, async_session, Base
from app.models import Order, OutboxEvent


async def create_tables():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    print("Tables created (or already existed).\n")


async def reset_tables():
    async with engine.begin() as conn:
        await conn.execute(text("DELETE FROM outbox_events"))
        await conn.execute(text("DELETE FROM orders"))
    print(" Tables cleared.\n")


async def test_successful_dual_write():
    print("=== TEST 1: Successful dual-write ===")
    async with async_session() as session:
        async with session.begin():
            order = Order(
                customer_email="alice@example.com",
                item="Mechanical Keyboard",
                amount_cents=8999,
            )
            session.add(order)
            await session.flush()

            outbox = OutboxEvent(
                aggregate_type="order",
                aggregate_id=str(order.id),
                event_type="order.created",
                payload={
                    "order_id": str(order.id),
                    "customer_email": order.customer_email,
                    "amount_cents": order.amount_cents,
                },
            )
            session.add(outbox)

    async with async_session() as session:
        orders = (await session.execute(select(Order))).scalars().all()
        events = (await session.execute(select(OutboxEvent))).scalars().all()
        print(f"Orders in DB: {len(orders)}")
        print(f"Outbox events in DB: {len(events)}")
        assert len(orders) == 1
        assert len(events) == 1
        assert events[0].status.value == "pending"
    print("PASSED: both rows exist together.\n")


async def test_failed_dual_write():
    print("=== TEST 2: Simulated crash mid-transaction ===")
    try:
        async with async_session() as session:
            async with session.begin():
                order = Order(
                    customer_email="bob@example.com",
                    item="Mouse",
                    amount_cents=2999,
                )
                session.add(order)
                await session.flush()

                outbox = OutboxEvent(
                    aggregate_type="order",
                    aggregate_id=str(order.id),
                    event_type="order.created",
                    payload={"order_id": str(order.id)},
                )
                session.add(outbox)
                await session.flush()

                raise RuntimeError("Simulated crash before commit!")
    except RuntimeError as e:
        print(f"Caught simulated crash: {e}")

    async with async_session() as session:
        orders = (await session.execute(select(Order))).scalars().all()
        events = (await session.execute(select(OutboxEvent))).scalars().all()
        print(f"Orders in DB: {len(orders)} (should still be 1, from Test 1)")
        print(f"Outbox events in DB: {len(events)} (should still be 1, from Test 1)")
        assert len(orders) == 1
        assert len(events) == 1
    print(" PASSED: the failed transaction left NO trace. Atomicity holds.\n")


async def main():
    await create_tables()
    await reset_tables()
    await test_successful_dual_write()
    await test_failed_dual_write()
    print("Phase 0 complete — dual-write atomicity is proven.")


if __name__ == "__main__":
    asyncio.run(main())