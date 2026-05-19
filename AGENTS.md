# S3MER: S3 Multi-backend Event-driven Replicator

S3MER is a high-performance, asynchronous S3 proxy designed to provide consistent multi-backend storage with memory-efficient streaming and a "Zero-Touch" replication architecture.

## Core Architecture

### 1. S3 Proxy (`src/s3mer/app.py`)
- **Async/ASGI**: Built as a pure ASGI application for maximum concurrency and low-overhead HTTP handling.
- **Path-style Routing**: Currently supports path-style S3 URLs (`/bucket/key`).
- **Declarative Dispatching**: Uses a centralized `HandlerRegistry` (`routing/registry.py`) to map operations to handlers via decorators. This eliminates manual dispatch boilerplate.
- **Unified Handler Context**: All handlers receive a standard `HandlerContext` containing dependencies (pool, strategies, metrics) and request metadata.
- **Memory-Efficient Streaming**: Implements on-the-fly streaming for `PUT` and `GET` operations. Large objects are never buffered in memory.
- **AWS Chunked Decoding**: Custom `AWSChunkedDecoder` handles `STREAMING-AWS4-HMAC-SHA256-PAYLOAD` (SigV4 chunked) unwrapping without memory overhead.

### 2. Unified Backend & Strategies (`src/s3mer/backends/`)
- **Unified Domain Namespace**: Consolidates connection pooling (`pool.py`), connection client session wrapper (`client.py`), and execution strategies (`strategies.py`) under a single flatter namespace to maximize cohesion and eliminate directory sprawl.
- **Primary-Synchronous Writes**: Writes are committed synchronously first to the primary backend.
- **Secondary-Asynchronous Replication**: Upon success, replication tasks are published asynchronously to Kafka for execution by background workers.
- **Replayable Fallbacks**: The `WritePrimaryReplicationStrategy` uses a `BufferedStreamReader` to buffer the request body stream to a temporary disk file, allowing the stream to be "replayed" if a primary write fails and a fallback attempt is needed.
- **Zero-Touch Replication**: Reuses S3 operations to avoid complex worker-side synchronization logic:
    - **Mapping**: Complex operations like `CompleteMultipartUpload` and `CopyObject` are replicated as a standard `PUT_OBJECT` where the worker reads the finalized object from Primary and writes to Secondary.
    - **Fan-out**: `DeleteObjects` (Multi-delete) is fanned out into individual `DELETE_OBJECT` tasks to ensure atomic consistency across backends.
- **Read Fallback Policy**: `ReadFallbackStrategy` executes reads in dynamic latency order using `BackendPool.all_by_latency()`. It always prioritizes the Primary backend first to guarantee read-after-write consistency, followed by secondary backends sorted by latency (lowest first, falling back to priority as a tie-breaker).
- **Active Latency Probing**: The decoupled `LatencyProber` (`prober.py`) periodically runs lightweight, bucket-agnostic probes (`LIST_BUCKETS`) on all S3 backends in the background to update actual round-trip latency statistics, marking failed/timeout probes with `float("inf")` to dynamically route reads away from dead backends.

### 3. Kafka Replication Worker (`src/s3mer/worker/`)
- Uses **FastStream** for robust Kafka message processing with built-in retries and Dead Letter Queues (DLQ).
- **State-Sync for Metadata**: For operations like Tagging, the worker fetches the current state from the Primary backend and applies it to Secondaries, ensuring eventual consistency.

### 4. Observability & Monitoring (`src/s3mer/common/metrics.py`)
- **Metrics Tracker Protocol**: Decouples business logic from monitoring. Implementation is injected via dependency injection.
- **Prometheus Integration**: Native support for metrics including:
    - HTTP request latency and status.
    - Ingress/Egress data transfer throughput.
    - **Replication Fan-out Factor**: Number of tasks generated per request.
    - **Backend Health**: Active monitoring of backend status (1=UP, 0=DOWN).
- **Internal Endpoints**: Dedicated `/.internal/metrics` and `/.internal/health` for operational visibility.

## Configuration

S3MER uses **Pydantic-settings** for robust configuration management.
- **YAML Loading**: Loads from `config/settings.yaml` by default.
- **Environment Variables**: Overrides YAML via `S3MER_` prefixed variables (e.g., `S3MER_LOG_LEVEL=DEBUG`).
- **Validation**: Automatically ensures exactly one "Primary" backend is configured and that all backend names are unique.

## Development Rules

- **PEP-8 Compliance**: All code must be PEP-8 compatible. Use `ruff format` to ensure consistency.
- **Strict Linting**: Disabling linter rules (e.g., `# noqa`) is allowed only for special cases and must be justified.
- **Dependency Injection**: Use `HandlerContext` to access system-level services (like the Metrics Tracker) in S3 handlers.
- **Explicit Imports**: Use top-level imports. Avoid inline imports unless strictly necessary for avoiding circular dependencies.
- **Type Safety**: All new code must pass `ty check` with zero errors.
- **"Make No Mistakes" Diligence**: Prioritize accuracy and absolute correctness over execution speed. Double-check all facts, logic, code syntax, and mental executions, and explicitly call out any uncertainty rather than guessing.

### How to add a new S3 Operation

1.  **Define Operation**: Add a new variant to the `S3Operation` enum in `routing/operations.py`.
2.  **Classify**: Add a routing entry to `RequestClassifier._ROUTING_TABLE` (and refinement logic if needed) in `routing/classifier.py`.
3.  **Implement Handler**: Create a function in `handlers/` and decorate it with `@s3_handler`:
    ```python
    @s3_handler(
        S3Operation.YOUR_OPERATION,
        operation_type=OperationType.READ, # or WRITE
        body_style=BodyStyle.EMPTY,        # or STREAM, BUFFERED
    )
    async def handle_your_op(ctx: HandlerContext) -> ASGIResponse:
        # Use ctx.pool, ctx.read_strategy, etc.
        ...
    ```
4.  **Register**: Ensure the handler module is imported in `app.py`.

## Supported S3 API Operations

### Bucket Operations
- `ListBuckets`, `CreateBucket`, `DeleteBucket`, `HeadBucket`
- `ListObjects` (V1 & V2), `DeleteObjects` (Multi-delete)
- `GetBucketLifecycle`, `PutBucketLifecycle`, `DeleteBucketLifecycle`
- `GetBucketPolicy`, `PutBucketPolicy`, `DeleteBucketPolicy`

### Object Operations
- `PutObject` (Regular & `aws-chunked` streaming)
- `GetObject`, `DeleteObject`, `HeadObject`, `CopyObject` (Server-side copy)

### Multipart Uploads
- `CreateMultipartUpload`, `UploadPart`, `CompleteMultipartUpload`, `AbortMultipartUpload`

### Object Metadata & Tagging
- `PutObjectTagging`, `GetObjectTagging`, `DeleteObjectTagging`

## Tech Stack
- **Language**: Python 3.12+
- **Frameworks**: ASGI (Uvicorn), FastStream (Kafka)
- **S3 Client**: aiobotocore
- **Infrastructure**: Kafka, S3-compatible storage (e.g., MinIO)
- **Quality**: Strict Type Checking (`ty`), Linting (`ruff`), and Pytest.

## Quality Assurance & Testing

### 1. Automation with Makefile
- **Run All Lints**: `make lint`
- **Run Unit Tests**: `make test-unit`
- **Run E2E Suite**: `make test`
- **Clean Environment**: `make clean`

### 2. Testing
- **Unit Testing**: Local tests with mocks: `uv run pytest tests/unit`
- **E2E Testing**: Full integration via Docker Compose: `make test`

## Architectural Insights & Reliability Controls

Our recent system architecture review highlighted several core distributed systems designs and identified reliability safeguards to guarantee high data durability:

1. **Transactional Outbox / Write-Ahead Log (WAL)**: To prevent event loss when a write succeeds on the primary backend but uvicorn crashes before Kafka publishing completes, a local persistent write-ahead log (WAL) should be used as a transactional outbox.
2. **Pausible Queue Failover**: The FastStream background consumer utilizes a pause-seek-resume pattern to pause the partition and retry when encountering transient rate-limiting/timeouts. On permanent client failure, it skips the bad offset (`failed_offset + 1`) to preserve queue progress.
3. **Anti-Entropy Reconciliation**: Out-of-band self-healing reconcilers should periodically perform fast checksum-based validation scans to identify and sync discrepancies between Primary and Secondaries.
4. **Declarative Error Mapping**: Granular classification registry is used to categorise client errors, transient network faults, and server rate-limiting codes into action buckets (`RETRY`, `FALLBACK`, `FAIL`).
