import uuid
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from app.db import get_session
from app.models import Order, OutboxEvent
from app.schemas import CreateOrderRequest, OrderResponse

router = APIRouter()


@router.post("/orders", response_model=OrderResponse, status_code=status.HTTP_201_CREATED)
async def create_order(
    payload: CreateOrderRequest,
    session: AsyncSession = Depends(get_session),
):
    # Idempotency check: has this exact request already been processed?
    existing = await session.execute(
        select(Order).where(Order.idempotency_key == payload.idempotency_key)
    )
    existing_order = existing.scalar_one_or_none()
    if existing_order is not None:
        return existing_order

    order = Order(
        customer_email=payload.customer_email,
        item=payload.item,
        amount_cents=payload.amount_cents,
        idempotency_key=payload.idempotency_key,
    )

    try:
        session.add(order)
        await session.flush()

        outbox = OutboxEvent(
            aggregate_type="order",
            aggregate_id=str(order.id),
            event_type="order.created",
            payload={
                "order_id": str(order.id),
                "customer_email": order.customer_email,
                "item": order.item,
                "amount_cents": order.amount_cents,
            },
        )
        session.add(outbox)
        await session.commit()
    except IntegrityError:
        # Race condition: two requests with the same idempotency_key arrived
        # at nearly the same time, and both passed the check above before
        # either committed. The database's UNIQUE constraint caught it here.
        await session.rollback()
        existing = await session.execute(
            select(Order).where(Order.idempotency_key == payload.idempotency_key)
        )
        order = existing.scalar_one()
        return order

    return order