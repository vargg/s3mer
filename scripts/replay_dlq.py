#!/usr/bin/env python3
"""Replay replication DLQ messages back to the main replication topic."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer


async def replay(
    bootstrap_servers: str,
    dlq_topic: str,
    target_topic: str,
    *,
    dry_run: bool,
    limit: int,
) -> int:
    consumer = AIOKafkaConsumer(
        dlq_topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id="s3mer-dlq-replayer",
    )
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap_servers)
    await consumer.start()
    await producer.start()
    replayed = 0
    try:
        async for msg in consumer:
            if replayed >= limit:
                break
            payload = json.loads(msg.value.decode())
            original = payload.get("original_message")
            if original is None:
                print(f"skip offset={msg.offset}: missing original_message", file=sys.stderr)
                continue
            key = f"{original['bucket']}/{original.get('key') or ''}"
            if dry_run:
                print(f"would replay message_id={original.get('message_id')} key={key}")
            else:
                await producer.send_and_wait(
                    target_topic,
                    json.dumps(original).encode(),
                    key=key.encode(),
                )
                print(f"replayed message_id={original.get('message_id')} key={key}")
            replayed += 1
    finally:
        await consumer.stop()
        await producer.stop()
    return replayed


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay S3MER replication DLQ messages")
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--dlq-topic", required=True)
    parser.add_argument("--target-topic", required=True)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=100)
    args = parser.parse_args()
    count = asyncio.run(
        replay(args.bootstrap, args.dlq_topic, args.target_topic, dry_run=args.dry_run, limit=args.limit)
    )
    print(f"Done: {count} message(s) processed")


if __name__ == "__main__":
    main()
