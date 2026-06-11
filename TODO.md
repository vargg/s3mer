# S3MER Roadmap & TODO

This document tracks improvements and known gaps. Priorities assume the **target deployment** below—not a generic public S3 gateway.

## Deployment model (target use case)

S3MER bridges **geo-reservation / cross-region durability** when the upstream S3 provider does not offer geo-replication. One region is written synchronously (primary); other regions are filled asynchronously via Kafka.

### Bounded usage assumptions

- **Multipart supported** for large objects; in-flight uploads are single-backend—clients retry failed sessions from scratch; geo copy after `CompleteMultipartUpload` (see [AGENTS.md](AGENTS.md#multipart-uploads)).
- **Write once, read a few times** — reads can go through the proxy with primary-first fallback.
- **Orphaned objects are acceptable** — an external detection/cleanup mechanism exists.
- **Expiry via bucket lifecycle** (by tag or prefix), not only explicit `DeleteObject` from clients.
- **No S3 object versioning** in the application path.

### Client contract (durability without WAL)

For write operations proxied through S3MER:

1. S3MER writes to the primary backend, then publishes a replication task to Kafka.
2. If Kafka publish fails, the proxy returns a **non-success** response even though the primary may already hold the object.
3. The **client must retry** the same operation with the **same key** (idempotent overwrite for `PUT`).
4. Success is only on **2xx** from the proxy. Timeouts and ambiguous responses should be retried like failures.

For **multipart**, the same contract applies at the object level: on failure, start a **new** upload session (new `UploadId`); only successful **Complete** triggers geo replication. Do not resume the same `UploadId` across a backend switch after partial parts on only one backend.

This retry contract replaces a transactional outbox for the target deployment: the client is the reconciliation layer. Duplicate replication messages and worker `PUT` replays are safe (overwrite semantics).

### Operational defaults for geo deployments

- **`write_strategy: primary_replication`** — default for geo. Use `quorum_replication` or `multi_sync_distributed` when synchronous multi-backend writes are required at scale.
- **`replication_mode: per_backend`** — isolates a sick region; avoids batch-mode pausing all partitions on one secondary failure.
- Monitor **replication lag** and worker partition pause/retry — eventual geo consistency is worker-bound after a successful client `PUT`.

---

## Priority overview

| Priority | Focus |
|----------|--------|
| **Now** | Ops visibility, fast failover on dead backends, docs/contract clarity |
| **Later** | Code quality (error registry, settings injection), broader S3 API |
| **Deferred** | WAL/outbox, anti-entropy, strict multi-backend ETag reads, versioning — not required for the deployment model above |

---

## 1. Traceability & Observability

- [x] **Request ID Propagation**: Generate a unique `X-S3MER-Request-ID` for every incoming request.
- [x] **Unified Logging**: Inject Request ID into all `structlog` contexts (Proxy and Worker).
- [x] **Kafka Headers**: Pass the Request ID in Kafka message headers to correlate proxy requests with replication tasks.
- [ ] **Replication lag & consumer health alerts**: Dashboards/alerts for per-backend consumer lag, partition pause duration, and background retry backlog (recommended for geo deployments).

## 2. Robust Error Handling

- [x] **Granular Error Classifier**: Map `botocore` error codes to `RETRY`, `FALLBACK`, or `FAIL`.
- [ ] **Declarative Error Mapping Registry**: Refactor procedural mappings in `ErrorClassifier` to a declarative registry (maintainability; lower urgency for bounded API surface).
- [ ] **Active Circuit Breaker in Backend Pool**: Skip failing backends quickly during read-fallback instead of waiting on TCP/socket timeouts (still valuable for geo read paths).

## 3. Configuration & Resource Management

- [x] **Connection Pool Tuning**: `max_pool_connections`, `connect_timeout`, `read_timeout` on `BackendConfig`.
- [x] **Worker Scaling**: Kafka consumer concurrency per process.
- [x] **Inject settings at startup**: Avoid repeated `load_settings()` on streaming hot paths (`get_chunk_size`, buffer limits).

## 4. Consistency & Conflict Resolution

- [x] **Kafka retry / ordering**: Pause–seek–resume with per-partition backoff (replaces re-publish-to-same-topic).
- [x] **Health Check Probing**: `LatencyProber` background `LIST_BUCKETS` probes.
- [x] **Multi-Sync Write Strategies** *(optional)*: `SimpleMultiSyncWriteStrategy`, `QuorumReplicationStrategy`, `DistributedMultiSyncWriteStrategy` (Valkey MPU sessions, proxy UUID upload IDs). Horizontally scalable; no compensating rollback.
- [~] **Transactional Outbox / Local WAL**: *Deferred for target deployment.* Client retry on publish failure + same-key `PUT` is the chosen consistency model. Revisit only if non-retrying clients or crash windows without client involvement must be covered.
- [~] **Anti-Entropy Reconciliation**: *Deferred.* External orphan/detection mechanism covers drift for target deployment.
- [~] **AllConsistent ETag Verification Mode**: *Deferred.* Write-once / primary-first reads do not require cross-backend ETag agreement on every read.

## 5. S3 API Coverage

- [~] **Versioning Support**: *Out of scope* for target application (no `versionId` in usage).
- [x] **Lifecycle & Policy Support**: Proxy + Zero-Touch replication for bucket lifecycle and policy.

## 6. Developer Experience & Performance

- [/] **Dynamic Latency-Based Read Ordering**: `all_by_latency()` for read fallback (primary always first). Boot-time auto-primary selection remains optional future work.
- [ ] **In-Memory Storage Backend**: Optional mock backend for local dev/tests without MinIO.

## 7. Enterprise / optional (not planned for target deployment)

- [~] **Durable Notification Webhooks**: *Deferred* — no webhook/outbox requirement in current product.
- [~] **SQLite PUT Intent / Pending Reaper**: *Deferred* — client retry + external orphan handling supersede in-proxy intent tracking for the target model.
