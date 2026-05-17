# S3MER Architectural Roadmap & TODO

This document tracks planned improvements for making S3MER production-ready, based on the architectural review.

## 1. Traceability & Observability
- [ ] **Request ID Propagation**: Generate a unique `X-S3MER-Request-ID` for every incoming request.
- [ ] **Unified Logging**: Inject Request ID into all `structlog` contexts (Proxy and Worker).
- [ ] **Kafka Headers**: Pass the Request ID in Kafka message headers to correlate proxy requests with replication tasks.

## 2. Robust Error Handling
- [x] **Granular Error Classifier**: Implement a utility to map `botocore` error codes (e.g., `429`, `503`, `InternalError`) to specific behaviors: `RETRY`, `FALLBACK`, or `FAIL`.
- [ ] **Declarative Error Mapping Registry**: Refactor the procedural conditional mappings in the error classifier to use a clean declarative exception/status-code registry for improved extendability.
- [ ] **Circuit Breaker**: Implement a basic circuit breaker in `BackendPool` to temporarily skip backends that are consistently failing.

## 3. Configuration & Resource Management
- [ ] **Connection Pool Tuning**: Expose `max_pool_connections`, `connect_timeout`, and `read_timeout` in `BackendConfig`.
- [ ] **Worker Scaling**: Add configuration for Kafka consumer concurrency (number of parallel workers per process).

## 4. Consistency & Conflict Resolution
- [x] **Fix Retry Logic**: The current approach of re-publishing failed messages to the same topic breaks Kafka's partition ordering guarantee. Implement an in-memory backoff or a dedicated retry strategy that preserves per-object order. *(Implemented via the Pause-Seek-Resume pattern with partition isolation and in-memory background backoff retry)*
- [ ] **Transactional Outbox / Local WAL**: Mitigate dual-write crash risks by appending S3 write replication payloads to a local write-ahead log (WAL) before/concurrent with S3 execution, replaying lost events on crash recovery.
- [ ] **Anti-Entropy Reconciliation**: Build a background worker/CLI tool to compare ETag and key state checklists across S3 backends and schedule self-healing replication tasks for out-of-sync secondaries.
- [ ] **Health Check Probing**: Implement active background probing for backends instead of relying solely on request-time failures.

## 5. S3 API Coverage
- [ ] **Versioning Support**: Add support for `versionId` in operations and replication.
- [ ] **Lifecycle & Policy Support**: Implement proxying for Bucket Lifecycle and Policy configurations.
