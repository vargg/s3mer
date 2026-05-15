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
- **Frameworks**: ASGI, FastStream (Kafka)
- **S3 Client**: aiobotocore
- **Infrastructure**: Kafka, S3-compatible storage (e.g., MinIO, AWS S3)
- **Quality**: Strict Type Checking (`ty`), Linting (`ruff`), and Pytest.
