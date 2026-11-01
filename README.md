# StreamPulse — Real-Time Library Analytics

## Stage 5: Library Analytics dashboard

The current backend core validates `LibraryEvent` objects, publishes them through
RabbitMQ, consumes and validates messages, updates the existing tumbling and
sliding windows, and stores the latest available summaries in Redis:

`Library client → FastAPI → RabbitMQ queue → single active consumer → existing windows → Redis`

The publisher and consumer are in `app/messaging/rabbitmq.py`; Redis metrics
storage is in `app/storage/redis_metrics.py`; `app/processing/event_processor.py`
connects those adapters to the Stage 2 window managers without duplicating their
logic. The FastAPI entry point is `app/main.py`; the responsive HTML/CSS/vanilla
JavaScript dashboard is in `frontend/` and reads only the API's live responses.

## Configuration

Install dependencies in the project virtual environment with
`.venv\Scripts\python.exe -m pip install -r requirements-dev.txt`.
Copy `.env.example` to `.env`, replace the placeholders locally, and export these
values in the shell that runs the application. The current modules read process
environment variables directly; they do not load `.env` automatically.

| Variable | Purpose |
| --- | --- |
| `RABBITMQ_URL` | CloudAMQP AMQP/AMQPS connection URL |
| `RABBITMQ_QUEUE` | Durable event queue; defaults to `streampulse.events` |
| `REDIS_URL` | Redis/Rediss connection URL |

The app starts even when a service URL is missing. Its health response reports
`not_configured` or `not_verified` until a request actually succeeds or fails;
startup does not claim a live connection.

Never commit `.env` or put credentials in source code. Configure CloudAMQP and
Redis manually; this project does not create cloud resources.

## Message and storage behavior

Messages are persistent JSON events using the Stage 2 schema. The consumer uses
one active RabbitMQ consumer for the queue and a prefetch count of one. It acks
only after event processing and metric writes succeed. Invalid messages and
processing failures are rejected without requeue to avoid poison-message loops;
configure a RabbitMQ dead-letter exchange if rejected messages should be retained.
This is not an exactly-once guarantee. A storage failure can occur after the
in-memory window has advanced, and window state is lost when the consumer process
restarts. Run one active consumer because window state is process-local.

Redis stores one latest JSON summary per window type at:

- `streampulse:metrics:tumbling` (updated only when a complete tumbling window arrives)
- `streampulse:metrics:sliding` (updated on every event, including partial windows)

## HTTP API

Start the API with `.venv\Scripts\python.exe -m uvicorn app.main:app --reload`.

| Method and path | Behavior |
| --- | --- |
| `GET /api/health` | API availability and separate RabbitMQ/Redis status values |
| `POST /api/events` | Validate and publish a library action; returns `202` only after publish succeeds |
| `GET /api/events` | Newest successfully published events in this process's bounded memory buffer |
| `GET /api/metrics` | Latest tumbling and sliding summaries from Redis; missing summaries are `null` |

Example event request:

```json
{
  "event_type": "BOOK_ISSUED",
  "student_id": "student-42",
  "book_id": "book-7"
}
```

The API assigns `event_id` and UTC `timestamp`; it does not assign a numeric event value. A successful
response is `202 Accepted`, for example:

```json
{
  "status": "published",
  "event": {
    "event_id": "generated-uuid",
    "event_type": "BOOK_ISSUED",
    "student_id": "student-42",
    "book_id": "book-7",
    "timestamp": "2026-10-28T10:30:00+00:00"
  }
}
```

Invalid input returns `422`; missing RabbitMQ configuration or a publish failure
returns `503` without claiming success. Metrics return an object with `tumbling`
and `sliding` values containing `count`, `sum`, `average`, `minimum`, and
`maximum`, for example:

```json
{
  "tumbling": {"count": 5, "sum": 5.0, "average": 1.0, "minimum": 1.0, "maximum": 1.0},
  "sliding": null
}
```

Missing stored values are null; unavailable Redis returns `503`. Health returns
`{"status":"available","services":{"rabbitmq":"not_verified","redis":"not_verified"}}`
until service operations verify connectivity.

Recent event history is held only in a bounded in-memory buffer (100 by default)
and reflects successful API publishes in this process. It is not populated from
the consumer and is cleared on restart. Runtime window reconfiguration is omitted:
the active consumer owns its in-memory window instances, and safely replacing them
requires coordinating with consumer lifecycle/state.

## Frontend dashboard

Run the backend and static frontend in separate PowerShell terminals from the
project root. Configure `RABBITMQ_URL` and `REDIS_URL` in the backend terminal
first if you want real publishing and metrics; `.env` is not loaded automatically.

Backend terminal:

```powershell
.\.venv\Scripts\python.exe -m uvicorn app.main:app --reload
```

Frontend terminal:

```powershell
.\.venv\Scripts\python.exe -m http.server 5500 --directory frontend
```

Open `http://127.0.0.1:5500`. The API base URL is configured once at the top of
`frontend/app.js` (`http://127.0.0.1:8000`). FastAPI allows CORS only from
`http://127.0.0.1:5500` and `http://localhost:5500` for local development. The
dashboard refreshes health, metrics, and recent events every 10 seconds while
the page is visible. Chart.js and the optional web fonts load from public CDNs;
if Chart.js is unavailable, the metric cards continue to display API data and the
chart area reports its load issue.

The action panel posts `event_type`, `student_id`, and `book_id`; the API generates the event ID and timestamp. The dashboard shows tumbling and sliding latency sum, average, minimum, and maximum in milliseconds, plus count of events in each current window. Recent events use the API's process-local history and disappear when the
backend restarts. Real event publishing requires RabbitMQ; real metrics require
Redis and the consumer. The dashboard does not simulate connectivity or data.

## Consumer latency measurement

`RabbitMQConsumer.handle_message` starts `time.perf_counter()` as soon as the consumer delivery callback begins. It stops the sample immediately after JSON decoding and `LibraryEvent` validation, converts the duration to milliseconds, and replaces the event value before calling the existing processor. This excludes queue wait and includes callback entry, body decoding, JSON parsing, and schema validation. It excludes window calculation, Redis writes, and acknowledgement so the sample is available as input to those calculations in one pass. The consumer acknowledges only after window processing and required Redis writes succeed. Thus the aggregated sample is consumer-side decode and validation latency, not full delivery-to-ack duration.

The existing one-pass architecture cannot include the Redis write of a summary that depends on the same sample: that summary must be calculated before it can be written, but its input value would only be known after the write. The Redis schema is unchanged. Legacy numeric values remain accepted in API requests and queued messages, but the consumer replaces them before aggregation.
## Tests

Run the complete suite using the project interpreter:

```powershell
.\.venv\Scripts\python.exe -m pytest -v
```

RabbitMQ, Redis, and API tests use fakes and do not require live services. Live
RabbitMQ/Redis connectivity and browser-to-cloud end-to-end behavior have not
been verified as part of this stage.
