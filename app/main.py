from fastapi import FastAPI
from fastapi.responses import PlainTextResponse

from app.api import router
from app.metrics import (
    get_outbox_counts,
    get_avg_publish_latency_seconds,
    get_processed_events_count,
    get_orders_count,
)

app = FastAPI(title="Outbox Demo")

app.include_router(router)


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/metrics", response_class=PlainTextResponse)
async def metrics():
    outbox = await get_outbox_counts()
    avg_latency = await get_avg_publish_latency_seconds()
    processed = await get_processed_events_count()
    orders = await get_orders_count()

    lines = [
        "# HELP outbox_orders_total Total number of orders created.",
        "# TYPE outbox_orders_total counter",
        f"outbox_orders_total {orders}",
        "",
        "# HELP outbox_events_pending Number of outbox events waiting to be published.",
        "# TYPE outbox_events_pending gauge",
        f"outbox_events_pending {outbox['pending']}",
        "",
        "# HELP outbox_events_published_total Total number of outbox events published.",
        "# TYPE outbox_events_published_total counter",
        f"outbox_events_published_total {outbox['published']}",
        "",
        "# HELP outbox_events_failed Number of outbox events that exhausted retries.",
        "# TYPE outbox_events_failed gauge",
        f"outbox_events_failed {outbox['failed']}",
        "",
        "# HELP outbox_processed_events_total Total number of events fully processed by the consumer.",
        "# TYPE outbox_processed_events_total counter",
        f"outbox_processed_events_total {processed}",
    ]

    if avg_latency is not None:
        lines += [
            "",
            "# HELP outbox_publish_latency_seconds_avg Average seconds between an event's creation and publish.",
            "# TYPE outbox_publish_latency_seconds_avg gauge",
            f"outbox_publish_latency_seconds_avg {avg_latency:.4f}",
        ]

    return "\n".join(lines) + "\n"