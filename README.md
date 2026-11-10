# Outbox Demo: Reliable Event Publishing with the Transactional Outbox Pattern

A small backend system that solves the **dual-write problem**: saving data to a database *and* publishing an event about it, without ever ending up with one but not the other.

Built with FastAPI, PostgreSQL, Redis Streams, SQLAlchemy (async), Alembic, and Docker Compose. Correctness under failure is verified by an automated pytest suite.

---

## The problem

When an order is placed, two things must happen:

1. Save the order to the database.
2. Publish an `order.created` event so other services (email, inventory, analytics) can react.

Done naively, these are two separate writes to two different systems. A crash between them leaves the order saved with no event (silent data loss), or the event published with no order. There is no way to make "write to Postgres" and "publish to a queue" one atomic operation.

## The solution

Write to **one** system twice, in a single transaction: the order row and an `outbox_events` row. A separate relay process then publishes pending outbox rows to Redis. The consumer deduplicates, so redelivery is harmless.

```text
                 one DB transaction
 Client ──► API ──────────────────────► PostgreSQL
 (POST /orders)   orders + outbox_events      │
                                              │ SELECT ... FOR UPDATE SKIP LOCKED
                                              ▼
                                           Poller (relay)
                                              │ XADD
                                              ▼
                                       Redis Stream "outbox-events"
                                              │ XREADGROUP / XACK
                                              ▼
                                      Consumer (idempotent)
                                              │
                                              ▼
                                  processed_events (dedupe ledger)
```

**At-least-once delivery + idempotent consumer = effectively-once processing.**

---

## Components

| Component | Path | Responsibility |
|---|---|---|
| API | `app/` | `POST /orders`, `GET /health`, `GET /metrics`. Writes order and outbox event atomically. |
| Poller | `relay/poller.py` | Publishes `PENDING` outbox rows to the Redis Stream, marks them `PUBLISHED`, marks them `FAILED` after 5 attempts. |
| Consumer | `consumer/worker.py` | Reads the stream through a consumer group, skips events already processed, acknowledges on success. |
| Migrations | `migrations/` | Alembic-managed schema. Runs automatically on `docker compose up`. |
| Tests | `tests/` | Chaos and correctness tests (see below). |

## Key design decisions

- **Atomic dual-write.** The order and its outbox event commit in the same Postgres transaction.
- **API-level idempotency.** Clients send an `idempotency_key`. A `UNIQUE` constraint backs it, and the endpoint handles the race where two identical requests arrive at once by catching `IntegrityError` and returning the existing order.
- **`FOR UPDATE SKIP LOCKED`.** Multiple pollers can run without picking up the same rows.
- **Publish, then mark published.** A crash between the two causes a duplicate publish. This is a deliberate trade-off, handled by the consumer.
- **Consumer idempotency.** `processed_events.event_id` is a primary key, so a duplicate insert is rejected by the database itself, not just by application logic.
- **Unacked messages stay pending.** If the consumer crashes before `XACK`, Redis keeps the entry in the group's pending list for recovery.

---

## Quick start

Requirements: Docker Desktop (or Docker Engine with Compose) and Python 3.12 if you want to run tests locally.

```bash
# 1. Create your environment file
cp .env.example .env

# 2. Generate strong passwords and put them in .env
python -c "import secrets; print(secrets.token_urlsafe(24))"
# Run it once per secret: POSTGRES_APP_PASSWORD, POSTGRES_SUPERUSER_PASSWORD, REDIS_PASSWORD

# 3. Start everything
docker compose up -d --build

# 4. Check status
docker compose ps -a
```

Expected: `postgres` and `redis` healthy, `migrate` exited with code 0 (it runs once and finishes), and `api`, `poller`, `consumer` up.

### Try it

```bash
curl http://127.0.0.1:8000/health

curl -X POST http://127.0.0.1:8000/orders \
  -H "Content-Type: application/json" \
  -d '{"customer_email":"alice@example.com","item":"Keyboard","amount_cents":8999,"idempotency_key":"11111111-1111-1111-1111-111111111111"}'

curl http://127.0.0.1:8000/metrics
```

Sending the same request again returns the same order and does not create a second one.

PowerShell users: use `curl.exe` (not `curl`, which is an alias) or `Invoke-RestMethod`.

### Outage demo

```bash
docker compose stop poller
# create an order: the API still succeeds, outbox_events_pending becomes 1
docker compose start poller
# within seconds pending returns to 0 and the event is processed
```

The API stays available, the event is not lost, and delivery resumes when the poller returns.

---

## Metrics

`GET /metrics` returns Prometheus text format:

| Metric | Type | Meaning |
|---|---|---|
| `outbox_orders_total` | counter | Orders created |
| `outbox_events_pending` | gauge | Events waiting to be published (a growing value means the poller is stuck or behind) |
| `outbox_events_published_total` | counter | Events published |
| `outbox_events_failed` | gauge | Events that exhausted retries (should normally be 0) |
| `outbox_processed_events_total` | counter | Events fully processed by the consumer |
| `outbox_publish_latency_seconds_avg` | gauge | Average delay between event creation and publish |

---

## Tests

Run against the Docker services (Postgres and Redis must be up):

```bash
pip install -r requirements.txt
pytest tests/ -v
```

| Test | What it proves |
|---|---|
| `test_dual_write_is_atomic_on_success` | Order and outbox event are written together |
| `test_dual_write_rolls_back_completely_on_failure` | A failure mid-transaction leaves neither row behind |
| `test_poller_crash_before_commit_causes_redelivery...` | A crash after publish but before commit really produces a duplicate, and the system recovers |
| `test_consumer_crash_before_ack_leaves_entry_pending...` | An unacknowledged entry stays recoverable and is not reprocessed |
| `test_duplicate_request_with_same_idempotency_key...` | A client retry returns the same order |
| `test_concurrent_duplicate_requests...` | Two simultaneous identical requests still create exactly one order |

---

## Security choices

- **Least-privilege database role.** The app connects as `outbox_app`, which has only SELECT/INSERT/UPDATE/DELETE. It is not a superuser and cannot drop the database (verified: `DROP DATABASE` is denied). The superuser password is used only during first-time initialization.
- **No secrets in source control.** `.env` is gitignored; only `.env.example` with placeholders is committed. Passwords are generated with `secrets.token_urlsafe`.
- **No secrets in images.** `.dockerignore` excludes `.env`; credentials are injected at runtime by Compose.
- **Redis requires a password.** Redis has no authentication by default, so `--requirepass` is set.
- **Non-root containers.** The application image runs as an unprivileged user.
- **Safe connection building.** The database URL is built with SQLAlchemy `URL.create()` so special characters in passwords cannot corrupt it.

---

## Known limitations

- **Polling, not CDC.** The poller checks every 2 seconds. A production system at scale would use change data capture (for example Debezium) or `LISTEN/NOTIFY`.
- **Latency metric is an all-time average.** A histogram or a recent time window would be more useful in production.
- **Single consumer.** The consumer is named `worker-1`. Scaling out needs unique consumer names and automatic recovery of pending entries from dead consumers (`XAUTOCLAIM`).
- **No outbox cleanup.** Published rows are kept forever. A retention job should delete old ones.
- **`FAILED` events are not alerted on.** They appear in the metrics but there is no dead-letter handling or alerting.
- **Runtime role can create tables.** For simplicity the app role has `CREATE` on the schema. Stricter setups use a separate migration role.
- **Dev-oriented defaults.** No TLS, and Postgres and Redis ports are exposed on the host for convenience.

---

## Troubleshooting

**Password authentication fails, or "Client sent AUTH, but no password is set".**
You probably have a native Postgres or Redis service on the default port (5432 or 6379) that is answering instead of the container. This project maps the containers to host ports **5433** (Postgres) and **6380** (Redis) to avoid this. On Windows, check with:

```powershell
Get-NetTCPConnection -LocalPort 5432 | Select-Object OwningProcess
Get-Service | Where-Object { $_.Name -like "*postgres*" -or $_.Name -like "*redis*" }
```

**`docker compose down -v` wiped my data.** `-v` deletes the named volumes (both Postgres and Redis data). Run `docker compose up -d` and migrations will recreate the schema.

**Redis `unknown command 'HELLO'`.** The Redis URL includes `?protocol=2` to force RESP2.

**Alembic autogenerate produced an empty migration.** Generate the baseline against an empty database, otherwise the diff finds nothing to create.

**Docker API connection error on Windows.** Docker Desktop is not running. Start it and wait for `docker info` to succeed.

**Scripts fail with `No module named 'app'`.** Run them from the project root as modules: `python -m relay.poller`.

**Windows line endings.** `.gitattributes` forces LF for shell scripts and YAML so they run correctly inside Linux containers.

---

## Project structure

```text
outbox-demo/
├── app/            # FastAPI app, models, schemas, config, metrics
├── relay/          # poller (outbox -> Redis Stream)
├── consumer/       # idempotent stream consumer
├── migrations/     # Alembic migrations
├── tests/          # pytest chaos and correctness tests
├── db/init.sh      # creates the least-privilege DB role
├── Dockerfile
├── docker-compose.yml
├── .env.example
└── README.md
```