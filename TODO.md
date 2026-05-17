# S3MER Architectural Roadmap & TODO

This document tracks planned improvements for making S3MER production-ready, based on the architectural review.

## 1. Traceability & Observability
- [ ] **Request ID Propagation**: Generate a unique `X-S3MER-Request-ID` for every incoming request.
- [ ] **Unified Logging**: Inject Request ID into all `structlog` contexts (Proxy and Worker).
- [ ] **Kafka Headers**: Pass the Request ID in Kafka message headers to correlate proxy requests with replication tasks.

## 2. Robust Error Handling
- [x] **Granular Error Classifier**: Implement a utility to map `botocore` error codes (e.g., `429`, `503`, `InternalError`) to specific behaviors: `RETRY`, `FALLBACK`, or `FAIL`.
- [ ] **Circuit Breaker**: Implement a basic circuit breaker in `BackendPool` to temporarily skip backends that are consistently failing.

## 3. Configuration & Resource Management
- [ ] **Connection Pool Tuning**: Expose `max_pool_connections`, `connect_timeout`, and `read_timeout` in `BackendConfig`.
- [ ] **Worker Scaling**: Add configuration for Kafka consumer concurrency (number of parallel workers per process).

## 4. Consistency & Conflict Resolution
- [x] **Fix Retry Logic**: The current approach of re-publishing failed messages to the same topic breaks Kafka's partition ordering guarantee. Implement an in-memory backoff or a dedicated retry strategy that preserves per-object order. *(Implemented via the Pause-Seek-Resume pattern with partition isolation and in-memory background backoff retry)*
- [ ] **Health Check Probing**: Implement active background probing for backends instead of relying solely on request-time failures.

## 5. S3 API Coverage
- [ ] **Versioning Support**: Add support for `versionId` in operations and replication.
- [ ] **Lifecycle & Policy Support**: Implement proxying for Bucket Lifecycle and Policy configurations.
