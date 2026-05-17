# S3MER Architectural Roadmap & TODO

This document tracks planned improvements for making S3MER production-ready, based on the architectural review.

## 1. Traceability & Observability
- [ ] **Request ID Propagation**: Generate a unique `X-S3MER-Request-ID` for every incoming request.
- [ ] **Unified Logging**: Inject Request ID into all `structlog` contexts (Proxy and Worker).
- [ ] **Kafka Headers**: Pass the Request ID in Kafka message headers to correlate proxy requests with replication tasks.

## 2. Robust Error Handling
- [x] **Granular Error Classifier**: Implement a utility to map `botocore` error codes (e.g., `429`, `503`, `InternalError`) to specific behaviors: `RETRY`, `FALLBACK`, or `FAIL`.
- [ ] **Declarative Error Mapping Registry**: Refactor the procedural conditional mappings in the error classifier to use a clean declarative exception/status-code registry for improved extendability.
- [ ] **Active Circuit Breaker in Backend Pool**: Implement an active circuit breaker (inspired by s3-orchestrator) to temporarily blackhole and skip failing backends during read-fallback sweeps, failing fast immediately rather than letting the client wait on long TCP/socket timeouts.

## 3. Configuration & Resource Management
- [x] **Connection Pool Tuning**: Expose `max_pool_connections`, `connect_timeout`, and `read_timeout` in `BackendConfig`.
- [x] **Worker Scaling**: Add configuration for Kafka consumer concurrency (number of parallel workers per process).

## 4. Consistency & Conflict Resolution
- [x] **Fix Retry Logic**: The current approach of re-publishing failed messages to the same topic breaks Kafka's partition ordering guarantee. Implement an in-memory backoff or a dedicated retry strategy that preserves per-object order. *(Implemented via the Pause-Seek-Resume pattern with partition isolation and in-memory background backoff retry)*
- [ ] **Transactional Outbox / Local WAL**: Mitigate dual-write crash risks by appending S3 write replication payloads to a local write-ahead log (WAL) before/concurrent with S3 execution, replaying lost events on crash recovery.
- [ ] **Anti-Entropy Reconciliation**: Build a background worker/CLI tool to compare ETag and key state checklists across S3 backends and schedule self-healing replication tasks for out-of-sync secondaries.
- [ ] **Health Check Probing**: Implement active background probing for backends instead of relying solely on request-time failures.
- [ ] **AllConsistent ETag Verification Mode**: Implement an optional strict read strategy that queries all backends and ensures ETag consistency before serving the object to the client, preventing reading from out-of-band drifts.
- [ ] **Multi-Sync Write Strategy**: Implement an optional synchronous write strategy that streams write operations to all configured backends concurrently, returning success only when all backends complete successfully, with automatic rollback (deletion) on partial failures.

## 5. S3 API Coverage
- [ ] **Versioning Support**: Add support for `versionId` in operations and replication.
- [x] **Lifecycle & Policy Support**: Implement proxying and Zero-Touch replication for Bucket Lifecycle and Policy configurations.

## 6. Developer Experience & Performance (Inspired by ReplicaT4)
- [ ] **Dynamic Latency-Based Primary Selection**: Implement an active boot-time benchmark that issues parallel probing requests (e.g. `HEAD` bucket) to all configured backends, measures median P50 response times, and automatically selects the fastest backend as the primary or orders read fallbacks.
- [ ] **In-Memory Storage Backend for Mock Testing**: Support a fully in-memory, zero-dependency storage engine configuration option to allow running the proxy instantly for rapid local development, mocking, and lightning-fast unit/integration tests without needing active MinIO instances.

## 7. Enterprise Features & Webhooks (Inspired by s3-orchestrator)
- [ ] **Durable Notification Webhooks (Transactional Outbox)**: Build a webhook event publisher using a transactional database-backed outbox pattern. State-change notifications (object CRUD, circuit breaker transitions, quota alerts) will be written to a local database outbox and dispatched asynchronously using CloudEvents JSON schema.
- [ ] **Stateful Metadata SQLite Engine / PUT Intent Tracking**: Introduce an optional, lightweight SQLite metadata engine to persist object locations, used capacities, and active PUT intents. Implement an in-flight intent tracking pattern (`pending_objects` table) and a background **Pending Reaper** to reconcile partial failures between database metadata and backend S3 contents under crash conditions.
