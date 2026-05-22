# S3MER: S3 Multi-backend Event-driven Replicator

S3MER is a high-performance, asynchronous S3 proxy that provides **geo-reservation / cross-region durability** when an upstream S3 provider does not offer geo-replication. It combines memory-efficient streaming proxying with a **Zero-Touch** Kafka replication architecture.

Cross-region consistency is **eventual**: the primary is written synchronously; other regions are filled asynchronously. See [Deployment model](#deployment-model) and [TODO.md](TODO.md) for product assumptions and roadmap priorities.

## Deployment model

### Purpose

Bridge **geo-reservation** across multiple S3-compatible backends (regions). One backend is **primary** (synchronous write); all others receive copies via the replication worker.

### Bounded usage (target product)

- **Write once, read a few times** — reads use primary-first fallback.
- **Large objects** may use multipart uploads; see [Multipart uploads](#multipart-uploads) for failover and replication behavior.
- **Orphaned objects are acceptable** — external detection/cleanup exists.
- **Expiry via bucket lifecycle** (tag or prefix), not only client `DeleteObject`.
- **No S3 object versioning** in the application path.

### Client contract (durability without WAL)

For writes through the proxy:

1. S3MER writes to the primary (or the next write candidate on retryable primary failure), then publishes a replication task to Kafka.
2. If Kafka publish fails, the proxy returns a **non-2xx** response even when the primary already holds the object.
3. The **client must retry** with the **same key** (idempotent `PUT` overwrite).
4. Treat only **2xx** as success; retry timeouts and ambiguous responses like failures.

For **multipart**, only **2xx on `CompleteMultipartUpload`** commits the object for replication; failed steps require a **new** upload session (see [Multipart uploads](#multipart-uploads)).

Duplicate Kafka messages and worker `PUT` replays are safe (overwrite semantics). See [TODO.md](TODO.md) for deferred items (WAL, anti-entropy) that are **not** required for this deployment.

### Multipart uploads

Multipart is **supported** in the proxy. Geo replication runs **only after a successful `CompleteMultipartUpload`** (mapped to worker `PUT_OBJECT`, same as a single `PutObject`). `CreateMultipartUpload`, `UploadPart`, and `AbortMultipartUpload` are **not** replicated to Kafka (`replicate=False`).

**In-flight uploads are single-backend.** There is no `UploadId` mapping across backends in `WritePrimaryReplicationStrategy` (only in optional `multi_sync`). If the primary fails after some parts were written only there, a fallback `UploadPart` with the same `UploadId` on a secondary will fail (no session on that backend). The client receives a non-2xx response.

**Client behavior on failure (same contract as `PutObject`):**

1. Treat non-2xx on any multipart step as failure — do not continue the same `UploadId` expecting transparent cross-backend resume.
2. **Retry from scratch**: new `CreateMultipartUpload` (or use `PutObject` for smaller objects), then parts and complete on whichever backend the proxy reaches via write fallback.
3. Only **2xx on `CompleteMultipartUpload`** means the object is durable for geo replication; the worker then copies the finalized object to all other backends.

**Example (primary dies mid-upload):** Parts exist only on primary → next `UploadPart` fails on primary and on secondary → client retries with a new multipart session → complete on secondary while primary is down → Kafka replicates full object to primary when it is back. Orphaned incomplete multipart state on the failed backend is acceptable (external cleanup / lifecycle).

Write fallback still helps when **`CreateMultipartUpload` succeeded on a secondary** while primary was down: later `UploadPart` calls may fail on primary first, then succeed on secondary using the same `UploadId` returned at create.

### Operational defaults (geo)

| Setting | Recommended | Notes |
|---------|-------------|--------|
| `write_strategy` | `primary_replication` | Default. Avoid `multi_sync` unless a single proxy instance and you need synchronous multi-region writes. |
| `replication_mode` | `per_backend` | Default. Isolates a sick region; batch mode pauses all assigned partitions on any secondary failure. |

Monitor replication lag and worker partition pause/retry after successful client writes.

## Core Architecture

### 1. S3 Proxy (`src/s3mer/app.py`)

- **Async/ASGI**: Pure ASGI application for high concurrency and low-overhead HTTP handling.
- **Path-style routing**: Path-style S3 URLs (`/bucket/key`) only (no virtual-hosted-style).
- **Declarative dispatching**: Central `HandlerRegistry` (`routing/registry.py`) maps operations to handlers via `@s3_handler`.
- **Unified handler context**: All handlers receive `HandlerContext` (pool, strategies, metrics, request metadata).
- **Memory-efficient streaming**: `PUT` / `GET` stream through the proxy without loading whole objects into RAM. Request bodies for write fallback use `BufferedStreamReader` (`SpooledTemporaryFile`: in-memory up to `max_memory_stream_buffer_size`, then disk) so the stream can be replayed on backend fallback.
- **AWS chunked decoding**: `AWSChunkedDecoder` unwraps `STREAMING-AWS4-HMAC-SHA256-PAYLOAD` (SigV4 chunked) on the fly.

### 2. Unified Backend & Strategies (`src/s3mer/backends/`)

- **Namespace**: `pool.py`, `client.py`, `strategies.py` — connection pooling, per-backend clients, execution strategies.
- **Primary-synchronous writes**: `WritePrimaryReplicationStrategy` (default) commits to the primary first, then schedules Kafka replication.
- **Write fallback**: On retryable primary failure, tries secondary backends in priority order; successful backend becomes the replication source.
- **Secondary-asynchronous replication**: After a successful write, tasks are published to Kafka for background workers.
- **Optional `MultiSyncWriteStrategy`**: Concurrent writes to all backends with rollback (`write_strategy: multi_sync`). In-memory multipart `UploadId` mapping is process-local — not for horizontally scaled geo proxy.
- **Zero-Touch replication** (worker reuses S3 APIs):
  - **Mapping**: `CompleteMultipartUpload` and `CopyObject` replicate as worker `PUT_OBJECT` (read finalized object from source, write to target).
  - **Fan-out**: `DeleteObjects` → individual `DELETE_OBJECT` tasks per key.
- **Read fallback**: `ReadFallbackStrategy` uses `BackendPool.all_by_latency()` — **primary always first**, then secondaries by latency (tie-break: `priority`).
- **Active latency probing**: `LatencyProber` (`prober.py`) runs `LIST_BUCKETS` on an interval; failed probes set `float("inf")` so reads skip dead backends.

### 3. Kafka Replication Worker (`src/s3mer/worker/`)

- **FastStream** Kafka consumer with **pause–seek–resume** retry and exponential backoff (`kafka/subscribers.py`). There is no separate DLQ topic — transient failures pause the partition and retry in the background; permanent client errors skip the message (`failed_offset + 1` in per-backend mode).
- **Replication modes**:
  - `per_backend` (default): topic per secondary (`{topic}.{backend}`), failure isolation per region.
  - `batch`: single topic, all targets in one message; one sick secondary can pause all assigned partitions.
- **State-sync for metadata**: Tagging, lifecycle, and policy operations fetch current state from the source backend on the worker and apply to targets.

### 4. Observability & Monitoring (`src/s3mer/common/metrics.py`)

- **MetricsTracker protocol**: Decouples business logic from Prometheus (injectable, `NullMetricsTracker` for tests).
- **Prometheus**: HTTP latency/status, ingress/egress bytes, replication fan-out, per-backend health and request metrics.
- **Request ID**: `X-S3MER-Request-ID` on every request; propagated through structlog and Kafka headers.
- **Internal endpoints**: `GET /.internal/metrics`, `GET /.internal/health`.

## Configuration

S3MER uses **Pydantic-settings** (`config/settings.py`). Example: `config/settings.example.yaml`.

- **YAML**: `config/settings.yaml` by default.
- **Environment**: Overrides via `S3MER_` prefix and `__` nesting (e.g. `S3MER_LOG_LEVEL=DEBUG`, `S3MER_KAFKA__BOOTSTRAP_SERVERS`).
- **Backends map**: `backends` is a **dict keyed by backend name** (not a list). Per-field vault injection works, e.g. `S3MER_BACKENDS__primary__SECRET_KEY`.
- **Validation**: Exactly one `is_primary: true` backend.

| Key | Default | Role |
|-----|---------|------|
| `write_strategy` | `primary_replication` | `primary_replication` or `multi_sync` |
| `replication_mode` | `per_backend` | `per_backend` or `batch` |
| `stream_chunk_size` | `65536` | Proxy pipe chunk size (bytes) |
| `max_memory_stream_buffer_size` | `10485760` | Spool threshold before disk (bytes) |
| `latency_probe_interval_seconds` | `30.0` | Background probe interval |
| `kafka.concurrency` | `1` | Parallel consumer workers per process |
| Per-backend | — | `max_pool_connections`, `connect_timeout`, `read_timeout`, `max_attempts` |

## Development Rules

- **PEP-8**: Use `ruff format` for consistency.
- **Strict linting**: `# noqa` only for justified special cases.
- **Dependency injection**: Use `HandlerContext` for pool, strategies, and metrics in handlers.
- **Explicit imports**: Top-level imports; inline imports only to break circular dependencies.
- **Type safety**: All new code must pass `ty check` with zero errors.
- **Accuracy**: Double-check logic and facts; state uncertainty rather than guessing.

### How to add a new S3 Operation

1. **Define operation**: Add a variant to `S3Operation` in `routing/operations.py`.
2. **Classify**: Extend `RequestClassifier._ROUTING_TABLE` and `_refine_operation` in `routing/classifier.py` as needed.
3. **Implement handler**: Add a function under `handlers/` with `@s3_handler`:
    ```python
    @s3_handler(
        S3Operation.YOUR_OPERATION,
        operation_type=OperationType.READ,  # or WRITE
        body_style=BodyStyle.EMPTY,         # or STREAM, BUFFERED
    )
    async def handle_your_op(ctx: HandlerContext) -> ASGIResponse:
        ...
    ```
4. **Register**: Import the handler module in `handlers/__init__.py` (side-effect registration). The proxy loads handlers when it imports `s3mer.handlers.internal` (package `__init__` runs first).

## Supported S3 API Operations

Implemented in the proxy; **target deployment** uses a subset (mostly `PutObject` / `GetObject` / lifecycle-driven expiry).

### Bucket Operations

- `ListBuckets`, `CreateBucket`, `DeleteBucket`, `HeadBucket`
- `ListObjects` (V1 & V2), `DeleteObjects` (multi-delete)
- `GetBucketLifecycle`, `PutBucketLifecycle`, `DeleteBucketLifecycle`
- `GetBucketPolicy`, `PutBucketPolicy`, `DeleteBucketPolicy`

### Object Operations

- `PutObject` (regular and `aws-chunked` streaming)
- `GetObject`, `DeleteObject`, `HeadObject`, `CopyObject` (server-side copy)

### Multipart Uploads

- `CreateMultipartUpload`, `UploadPart`, `CompleteMultipartUpload`, `AbortMultipartUpload`
- Production semantics: [Multipart uploads](#multipart-uploads) (replication at complete; retry whole session on failure).

### Object Metadata & Tagging

- `PutObjectTagging`, `GetObjectTagging`, `DeleteObjectTagging`

## Tech Stack

- **Language**: Python 3.12+
- **Frameworks**: ASGI (Uvicorn), FastStream (Kafka)
- **S3 client**: aiobotocore
- **Infrastructure**: Kafka, S3-compatible storage (e.g. MinIO)
- **Quality**: `ty`, `ruff`, pytest

## Quality Assurance & Testing

### Makefile

- `make lint` — format, ruff check, `ty check`
- `make test-unit` — unit tests
- `make test` — E2E via Docker Compose
- `make clean` — tear down test environment

### Testing

- **Unit**: `uv run pytest tests/unit`
- **E2E**: `make test` (proxy, worker, MinIO, Kafka)

## Reliability & distributed systems

| Mechanism | Status | Notes |
|-----------|--------|--------|
| **Client retry contract** | **Active (target deployment)** | Non-2xx on failed Kafka publish; same-key retry |
| **Pause–seek–resume** | **Implemented** | Worker retries; per-backend skip on permanent errors |
| **ErrorClassifier** | **Implemented** | `RETRY`, `FALLBACK`, `FAIL` for proxy and worker |
| **Latency probing** | **Implemented** | Read routing away from dead backends |
| **Transactional outbox / WAL** | **Deferred** | See [TODO.md](TODO.md) — not required when clients retry |
| **Anti-entropy reconciliation** | **Deferred** | External orphan/detection in target deployment |
| **Declarative error registry** | **Planned** | Refactor procedural `ErrorClassifier` mappings |

Roadmap and priorities: **[TODO.md](TODO.md)**.
