# S3M: Multi-Backend S3 Proxy with Kafka Replication

S3M is a high-performance, asynchronous S3 proxy designed to provide consistent multi-backend storage with memory-efficient streaming and a "Zero-Touch" replication architecture.

## Core Architecture

### 1. S3 Proxy (`src/s3m/app.py`)
- **Async/ASGI**: Built using Python's ASGI spec for high concurrency.
- **Memory-Efficient Streaming**: Implements on-the-fly streaming for `PUT` and `GET` operations. Large objects are never buffered in memory.
- **AWS Chunked Decoding**: Custom `AWSChunkedDecoder` handles `STREAMING-AWS4-HMAC-SHA256-PAYLOAD` (SigV4 chunked) unwrapping without memory overhead.
- **Request Classification**: Advanced routing that differentiates between Bucket, Object, Multipart, and Metadata operations based on URL patterns and query parameters.
- **Replayable Fallbacks**: Uses `BufferedStreamReader` to allow replaying request bodies during backend failover without memory-intensive buffering of entire large objects.

### 2. Replication Strategy (`src/s3m/strategies/`)
- **Primary-Synchronous**: Writes are first committed to a "Primary" backend.
- **Secondary-Asynchronous**: Upon success, a message is published to Kafka for background replication to "Secondary" backends.
- **Zero-Touch Replication**: Cleverly reuses existing S3 operations to avoid complex worker logic. For example:
    - **CopyObject**: Replicated as a `PUT_OBJECT` where the worker reads from Primary and writes to Secondary.
    - **DeleteObjects (Multi-delete)**: Strategy-managed fan-out into individual `DELETE_OBJECT` messages, ensuring atomic consistency across all backends even during fallbacks.

### 3. Kafka Replication Worker (`src/s3m/worker/`)
- Uses **FastStream** for robust Kafka message processing.
- Handles retries and Dead Letter Queues (DLQ) for failed replication tasks.
- **State-Sync for Metadata**: For operations like Tagging, the worker fetches the current state from the Primary backend and applies it to Secondaries, ensuring eventual consistency.

### 4. Observability & Monitoring (`src/s3m/common/metrics.py`)
- **Metrics Tracker Architecture**: Uses a decoupled `MetricsTracker` protocol to abstract telemetry from business logic.
- **Prometheus Integration**: Native support for Prometheus metrics including request latency, data transfer throughput, and backend health status.
- **Internal Endpoints**: Dedicated `/.internal/metrics` and `/.internal/health` handlers for operational visibility.

## Development Rules

- **PEP-8 Compliance**: All code must be PEP-8 compatible. Use `ruff format` to ensure consistency.
- **Strict Linting**: Disabling linter rules (e.g., `# noqa`) is allowed only for special cases and must be justified.
- **Dependency Injection**: Use dependency injection for system-level services (like the Metrics Tracker) to ensure testability.
- **Explicit Imports**: Use top-level imports. Avoid inline imports unless strictly necessary for avoiding circular dependencies.

## Supported S3 API Operations

### Bucket Operations
- `ListBuckets`, `CreateBucket`, `DeleteBucket`, `HeadBucket`
- `ListObjects` (V1 & V2), `DeleteObjects` (Multi-delete)

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

### 2. Linting and Formatting
We use **Ruff** for extremely fast linting and formatting, and **ty** for strict type checking.
- **Check Linting**: `uv run ruff check src tests`
- **Format Code**: `uv run ruff format src tests`
- **Type Checking**: `uv run ty check src tests`

### 3. Testing
- **Unit Testing**: Local tests with mocks: `uv run pytest tests/unit`
- **E2E Testing**: Full integration via Docker: `docker compose -f docker-compose-test.yaml up --build --exit-code-from pytest-runner`
