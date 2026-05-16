# S3M: Multi-Backend S3 Proxy with Kafka Replication

S3M is a high-performance, asynchronous S3 proxy designed to provide consistent multi-backend storage with memory-efficient streaming and a "Zero-Touch" replication architecture.

## Core Architecture

### 1. S3 Proxy (`src/s3m/app.py`)
- **Async/ASGI**: Built using Python's ASGI spec for high concurrency.
- **Memory-Efficient Streaming**: Implements on-the-fly streaming for `PUT` and `GET` operations. Large objects are never buffered in memory.
- **AWS Chunked Decoding**: Custom `AWSChunkedDecoder` handles `STREAMING-AWS4-HMAC-SHA256-PAYLOAD` (SigV4 chunked) unwrapping without memory overhead.
- **Request Classification**: Advanced routing that differentiates between Bucket, Object, Multipart, and Metadata operations based on URL patterns and query parameters.

### 2. Replication Strategy (`src/s3m/strategies/`)
- **Primary-Synchronous**: Writes are first committed to a "Primary" backend.
- **Secondary-Asynchronous**: Upon success, a message is published to Kafka for background replication to "Secondary" backends.
- **Zero-Touch Replication**: Cleverly reuses existing S3 operations to avoid complex worker logic. For example:
    - **CopyObject**: Replicated as a `PUT_OBJECT` where the worker reads from Primary and writes to Secondary.
    - **DeleteObjects (Multi-delete)**: Intercepted and fanned out into individual `DELETE_OBJECT` messages.

### 3. Kafka Replication Worker (`src/s3m/worker/`)
- Uses **FastStream** for robust Kafka message processing.
- Handles retries and Dead Letter Queues (DLQ) for failed replication tasks.
- **State-Sync for Metadata**: For operations like Tagging, the worker fetches the current state from the Primary backend and applies it to Secondaries, ensuring eventual consistency.

## Supported S3 API Operations

### Bucket Operations
- `ListBuckets`
- `CreateBucket`
- `DeleteBucket`
- `HeadBucket`
- `ListObjects` (V1 & V2)
- `DeleteObjects` (Multi-delete)

### Object Operations
- `PutObject` (Regular & `aws-chunked` streaming)
- `GetObject`
- `DeleteObject`
- `HeadObject`
- `CopyObject` (Server-side copy)

### Multipart Uploads
- `CreateMultipartUpload`
- `UploadPart`
- `CompleteMultipartUpload`
- `AbortMultipartUpload`

### Object Metadata & Tagging
- `PutObjectTagging`
- `GetObjectTagging`
- `DeleteObjectTagging`

## Tech Stack
- **Language**: Python 3.12+
- **Frameworks**: ASGI (Uvicorn), FastStream (Kafka)
- **S3 Client**: aiobotocore
- **Infrastructure**: Kafka, S3-compatible storage (e.g., MinIO)
- **Quality**: Strict Type Checking (`ty`), Linting (`ruff`), and Pytest.

## Quality Assurance & Testing

### 1. Linting and Formatting
We use **Ruff** for extremely fast linting and formatting, and **ty** for strict type checking.

- **Check Linting**: `uv run ruff check`
- **Auto-fix Linting**: `uv run ruff check --fix`
- **Format Code**: `uv run ruff format`
- **Type Checking**: `uv run ty check src` (also check `tests`)

### 2. Unit Testing
Unit tests focus on individual components like handlers, strategies, and streaming utilities. They use mocks for external dependencies (S3 backends, Kafka).

- **Run Unit Tests**: `uv run pytest tests/unit`
- **Coverage**: `uv run pytest --cov=src tests/unit`

### 3. End-to-End (E2E) Testing
E2E tests verify the full integration between the S3 Proxy, Kafka, and multiple MinIO backends. They run in a containerized environment to ensure consistency.

- **Run E2E Suite**:
  ```bash
  docker compose -f docker-compose-test.yaml up --build --exit-code-from pytest-runner
  ```
- **Scenario Testing**: The E2E suite covers bucket lifecycles, multipart uploads, and eventually-consistent replication to secondary backends.
