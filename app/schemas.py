import uuid
from datetime import datetime
from pydantic import BaseModel, EmailStr, Field, PositiveInt


class CreateOrderRequest(BaseModel):
    customer_email: EmailStr
    item: str = Field(min_length=1, max_length=200)
    amount_cents: PositiveInt
    idempotency_key: uuid.UUID


class OrderResponse(BaseModel):
    id: uuid.UUID
    customer_email: str
    item: str
    amount_cents: int
    created_at: datetime

    model_config = {"from_attributes": True}