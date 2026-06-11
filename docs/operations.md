# S3MER Operations Runbook

This guide helps on-call engineers triage replication worker behavior from metrics and logs without reading source code.

## Key metrics

| Metric | Labels | Meaning |
|--------|--------|---------|
| `s3mer_replication_consumer_outcomes_total` | `operation`, `target_backend`, `outcome` | Per-message result |
| `s3mer_replication_paused_partitions` | `topic`, `partition` | `1` while partition is paused for retry |
| `s3mer_replication_background_retries_in_flight` | `mode` (`batch` / `per_backend`) | Active background retry goroutines |
| `s3mer_replication_retries_total` | `operation`, `target_backend` | Retry attempt counter |
| `s3mer_replication_dlq_total` | `reason` | Messages sent to DLQ |
| `s3mer_backend_circuit_state` | `backend_name`, `state` | Circuit breaker state |

Use an external Kafka exporter (e.g. Burrow, kafka-exporter) for consumer lag; S3MER does not embed lag polling.

## PromQL examples

**Permanent skips (intentional drift on one backend):**

```promql
sum by (target_backend, operation) (
  rate(s3mer_replication_consumer_outcomes_total{outcome="skipped_permanent"}[5m])
)
```

**Exhausted retries (needs DLQ review):**

```promql
sum by (target_backend) (
  rate(s3mer_replication_consumer_outcomes_total{outcome="skipped_max_retries"}[5m])
)
```

**Poison messages (bad Kafka payload):**

```promql
sum(rate(s3mer_replication_consumer_outcomes_total{outcome="skipped_poison"}[5m]))
```

**Paused partitions (worker backing off):**

```promql
s3mer_replication_paused_partitions == 1
```

**DLQ volume:**

```promql
sum by (reason) (rate(s3mer_replication_dlq_total[5m]))
```

## Outcome reference

| Outcome | Expected? | Action |
|---------|-----------|--------|
| `success` | Yes | None |
| `skipped_already_absent` | Yes | Idempotent delete replay |
| `skipped_already_synced` | Yes | ETag optimization skipped redundant PUT |
| `skipped_source_gone` | Yes | Source deleted before worker ran; no target write |
| `skipped_permanent` | Yes | 4xx on target; **intentional drift** on that backend |
| `skipped_max_retries` | Investigate | Check DLQ topic; fix target; replay |
| `skipped_poison` | Investigate | Fix publisher or discard bad message |
| `skipped_unsupported` | Rare | Upgrade worker or fix message mapping |
| `failed_no_consumer` | Bug / startup | Worker lost Kafka consumer handle |

## Manual recovery

1. **Skipped permanent (403, etc.)** — Fix IAM/policy on target backend, then re-publish or use `scripts/replay_dlq.py`.
2. **Skipped max retries** — Consume DLQ topic (`{topic}.dlq` or `{topic}.{backend}.dlq`), fix root cause, replay to main topic.
3. **Geo drift after skip** — Run external anti-entropy: `PUT` missing keys to the sick region, or delete orphans per your cleanup process.
4. **Paused partition stuck** — Check target backend health and worker logs; partition resumes automatically after successful retry or skip.

## Client contract reminder

Proxy returns **non-2xx** when Kafka publish fails after a successful primary write. Clients must retry with the **same key**. Only **2xx** means durable for geo replication.
