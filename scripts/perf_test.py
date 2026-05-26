#!/usr/bin/env python3
"""Performance testing script for S3MER.

Compares direct S3 backend operations vs operations through S3MER proxy.
Generates a target request rate (default 40 RPS) with objects of sizes 0.5MB to 1MB.
"""

import argparse
import asyncio
import contextlib
import math
import os
import random
import sys
import time
from typing import Any

from aiobotocore.config import AioConfig
from aiobotocore.session import get_session


def calculate_percentile(data: list[float], percent: float) -> float:
    """Calculate percentile using linear interpolation in pure Python."""
    if not data:
        return 0.0
    sorted_data = sorted(data)
    k = (len(sorted_data) - 1) * percent
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return sorted_data[int(k)]
    d0 = sorted_data[int(f)] * (c - k)
    d1 = sorted_data[int(c)] * (k - f)
    return d0 + d1


async def recreate_bucket(client: Any, bucket_name: str) -> None:
    """Ensure the target bucket exists and is completely empty."""
    with contextlib.suppress(Exception):
        paginator = client.get_paginator("list_objects_v2")
        async for page in paginator.paginate(Bucket=bucket_name):
            if "Contents" in page:
                objects = [{"Key": obj["Key"]} for obj in page["Contents"]]
                await client.delete_objects(Bucket=bucket_name, Delete={"Objects": objects})

    with contextlib.suppress(Exception):
        await client.delete_bucket(Bucket=bucket_name)

    await client.create_bucket(Bucket=bucket_name)


async def run_benchmark(
    client: Any,
    bucket_name: str,
    payloads: list[bytes],
    op_type: str,
    target_name: str,
    rps: float,
    duration: float,
) -> dict[str, Any]:
    """Run PUT or GET benchmark phase at a target RPS rate."""
    print(f"  Starting {op_type} benchmark against {target_name} at {rps} RPS...")

    latencies: list[float] = []
    success_count = 0
    failure_count = 0
    bytes_transferred = 0

    start_time = time.perf_counter()
    end_time = start_time + duration

    tasks = []
    interval = 1.0 / rps
    next_request_time = time.perf_counter()

    async def make_request(key: str, payload: bytes) -> None:
        nonlocal success_count, failure_count, bytes_transferred
        req_start = time.perf_counter()
        try:
            if op_type == "PUT":
                await client.put_object(Bucket=bucket_name, Key=key, Body=payload)
                bytes_transferred += len(payload)
            elif op_type == "GET":
                resp = await client.get_object(Bucket=bucket_name, Key=key)
                body_data = await resp["Body"].read()
                bytes_transferred += len(body_data)
            latencies.append(time.perf_counter() - req_start)
            success_count += 1
        except Exception:
            failure_count += 1

    idx = 0
    while time.perf_counter() < end_time:
        now = time.perf_counter()
        if now >= next_request_time:
            key = f"perf_obj_{idx}"
            payload = payloads[idx % len(payloads)]
            tasks.append(asyncio.create_task(make_request(key, payload)))
            idx += 1
            next_request_time += interval
        else:
            # Yield control to allow async tasks to run without busy-looping
            await asyncio.sleep(min(0.001, next_request_time - now))

    # Wait for all outstanding tasks to complete
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)

    actual_duration = time.perf_counter() - start_time
    return {
        "latencies": latencies,
        "success": success_count,
        "failure": failure_count,
        "duration": actual_duration,
        "bytes": bytes_transferred,
        "total_requests": idx,
    }


def format_diff(direct: float, proxy: float, _lower_is_better: bool = True) -> str:
    """Format the difference percentage between Direct and Proxy."""
    if direct == 0:
        return "-"
    diff = ((proxy - direct) / direct) * 100
    sign = "+" if diff >= 0 else ""
    # Decide if change is good (green-ish/improved vs degraded)
    # But in plain CLI, we just print the value.
    return f"{sign}{diff:.2f}%"


def print_results(
    direct_put: dict[str, Any],
    proxy_put: dict[str, Any],
    direct_get: dict[str, Any],
    proxy_get: dict[str, Any],
    target_rps: float,
) -> None:
    """Render the ASCII comparison table."""
    # Pre-calculate PUT metrics
    dp_success = (
        (direct_put["success"] / direct_put["total_requests"] * 100) if direct_put["total_requests"] > 0 else 0
    )
    pp_success = (proxy_put["success"] / proxy_put["total_requests"] * 100) if proxy_put["total_requests"] > 0 else 0

    dp_actual_rps = direct_put["total_requests"] / direct_put["duration"]
    pp_actual_rps = proxy_put["total_requests"] / proxy_put["duration"]

    dp_throughput = (direct_put["bytes"] / (1024 * 1024)) / direct_put["duration"]
    pp_throughput = (proxy_put["bytes"] / (1024 * 1024)) / proxy_put["duration"]

    dp_mean = (sum(direct_put["latencies"]) / len(direct_put["latencies"]) * 1000) if direct_put["latencies"] else 0.0
    pp_mean = (sum(proxy_put["latencies"]) / len(proxy_put["latencies"]) * 1000) if proxy_put["latencies"] else 0.0

    dp_p50 = calculate_percentile(direct_put["latencies"], 0.50) * 1000
    pp_p50 = calculate_percentile(proxy_put["latencies"], 0.50) * 1000

    dp_p90 = calculate_percentile(direct_put["latencies"], 0.90) * 1000
    pp_p90 = calculate_percentile(proxy_put["latencies"], 0.90) * 1000

    dp_p95 = calculate_percentile(direct_put["latencies"], 0.95) * 1000
    pp_p95 = calculate_percentile(proxy_put["latencies"], 0.95) * 1000

    dp_p99 = calculate_percentile(direct_put["latencies"], 0.99) * 1000
    pp_p99 = calculate_percentile(proxy_put["latencies"], 0.99) * 1000

    # Pre-calculate GET metrics
    dg_success = (
        (direct_get["success"] / direct_get["total_requests"] * 100) if direct_get["total_requests"] > 0 else 0
    )
    pg_success = (proxy_get["success"] / proxy_get["total_requests"] * 100) if proxy_get["total_requests"] > 0 else 0

    dg_actual_rps = direct_get["total_requests"] / direct_get["duration"]
    pg_actual_rps = proxy_get["total_requests"] / proxy_get["duration"]

    dg_throughput = (direct_get["bytes"] / (1024 * 1024)) / direct_get["duration"]
    pg_throughput = (proxy_get["bytes"] / (1024 * 1024)) / proxy_get["duration"]

    dg_mean = (sum(direct_get["latencies"]) / len(direct_get["latencies"]) * 1000) if direct_get["latencies"] else 0.0
    pg_mean = (sum(proxy_get["latencies"]) / len(proxy_get["latencies"]) * 1000) if proxy_get["latencies"] else 0.0

    dg_p50 = calculate_percentile(direct_get["latencies"], 0.50) * 1000
    pg_p50 = calculate_percentile(proxy_get["latencies"], 0.50) * 1000

    dg_p90 = calculate_percentile(direct_get["latencies"], 0.90) * 1000
    pg_p90 = calculate_percentile(proxy_get["latencies"], 0.90) * 1000

    dg_p95 = calculate_percentile(direct_get["latencies"], 0.95) * 1000
    pg_p95 = calculate_percentile(proxy_get["latencies"], 0.95) * 1000

    dg_p99 = calculate_percentile(direct_get["latencies"], 0.99) * 1000
    pg_p99 = calculate_percentile(proxy_get["latencies"], 0.99) * 1000

    line = "=" * 80
    print(line)
    print(f"{'S3MER PERFORMANCE COMPARISON':^80}")
    print(line)
    print(f"{'Metric':<25} | {'Direct S3':<15} | {'S3MER Proxy':<15} | {'Difference':<15}")
    print("-" * 80)
    print("  [PUT Operations]")
    print(f"{'  Target RPS':<25} | {target_rps:<15.2f} | {target_rps:<15.2f} | -")
    print(f"{'  Actual RPS':<25} | {dp_actual_rps:<15.2f} | {pp_actual_rps:<15.2f} | {format_diff(dp_actual_rps, pp_actual_rps, False)}")
    print(f"{'  Success Rate':<25} | {dp_success:<14.2f}% | {pp_success:<14.2f}% | -")
    print(f"{'  Mean Latency':<25} | {dp_mean:<12.2f} ms | {pp_mean:<12.2f} ms | {format_diff(dp_mean, pp_mean)}")
    print(f"{'  P50 Latency':<25} | {dp_p50:<12.2f} ms | {pp_p50:<12.2f} ms | {format_diff(dp_p50, pp_p50)}")
    print(f"{'  P90 Latency':<25} | {dp_p90:<12.2f} ms | {pp_p90:<12.2f} ms | {format_diff(dp_p90, pp_p90)}")
    print(f"{'  P95 Latency':<25} | {dp_p95:<12.2f} ms | {pp_p95:<12.2f} ms | {format_diff(dp_p95, pp_p95)}")
    print(f"{'  P99 Latency':<25} | {dp_p99:<12.2f} ms | {pp_p99:<12.2f} ms | {format_diff(dp_p99, pp_p99)}")
    print(f"{'  Throughput':<25} | {dp_throughput:<10.2f} MB/s | {pp_throughput:<10.2f} MB/s | {format_diff(dp_throughput, pp_throughput, False)}")

    print("-" * 80)
    print("  [GET Operations]")
    print(f"{'  Target RPS':<25} | {target_rps:<15.2f} | {target_rps:<15.2f} | -")
    print(f"{'  Actual RPS':<25} | {dg_actual_rps:<15.2f} | {pg_actual_rps:<15.2f} | {format_diff(dg_actual_rps, pg_actual_rps, False)}")
    print(f"{'  Success Rate':<25} | {dg_success:<14.2f}% | {pg_success:<14.2f}% | -")
    print(f"{'  Mean Latency':<25} | {dg_mean:<12.2f} ms | {pg_mean:<12.2f} ms | {format_diff(dg_mean, pg_mean)}")
    print(f"{'  P50 Latency':<25} | {dg_p50:<12.2f} ms | {pg_p50:<12.2f} ms | {format_diff(dg_p50, pg_p50)}")
    print(f"{'  P90 Latency':<25} | {dg_p90:<12.2f} ms | {pg_p90:<12.2f} ms | {format_diff(dg_p90, pg_p90)}")
    print(f"{'  P95 Latency':<25} | {dg_p95:<12.2f} ms | {pg_p95:<12.2f} ms | {format_diff(dg_p95, pg_p95)}")
    print(f"{'  P99 Latency':<25} | {dg_p99:<12.2f} ms | {pg_p99:<12.2f} ms | {format_diff(dg_p99, pg_p99)}")
    print(f"{'  Throughput':<25} | {dg_throughput:<10.2f} MB/s | {pg_throughput:<10.2f} MB/s | {format_diff(dg_throughput, pg_throughput, False)}")
    print(line)


async def main() -> None:
    """Parse arguments, setup S3 clients, run benchmarks, and print results."""
    parser = argparse.ArgumentParser(description="S3MER Perf Comparison Script")
    parser.add_argument("--proxy-url", default="http://localhost:8000", help="S3MER Proxy URL")
    parser.add_argument("--direct-url", default="http://localhost:9000", help="Direct Primary S3 URL")
    parser.add_argument("--access-key", default="minioadmin", help="S3 Access Key")
    parser.add_argument("--secret-key", default="minioadmin", help="S3 Secret Key")
    parser.add_argument("--bucket", default="s3mer-perf-test", help="Target Bucket Name")
    parser.add_argument("--rps", type=float, default=50.0, help="Target RPS")
    parser.add_argument("--duration", type=float, default=10.0, help="Phase Duration in seconds")
    parser.add_argument("--min-size-kb", type=int, default=1000, help="Min Object Size (KB)")
    parser.add_argument("--max-size-kb", type=int, default=2000, help="Max Object Size (KB)")
    parser.add_argument("--pool-size", type=int, default=100, help="Max Connection Pool Size")
    args = parser.parse_args()

    # Pre-generate random payloads to avoid generating CPU load during benchmark
    print(f"Pre-generating 10 random payloads ({args.min_size_kb}KB to {args.max_size_kb}KB)...")
    payloads = [
        os.urandom(random.randint(args.min_size_kb * 1024, args.max_size_kb * 1024))
        for _ in range(20)
    ]

    session = get_session()
    config = AioConfig(
        s3={"addressing_style": "path", "payload_signing_enabled": False},
        max_pool_connections=args.pool_size,
        connect_timeout=5.0,
        read_timeout=15.0,
    )

    # 1. Direct Benchmark
    async with session.create_client(
        "s3",
        endpoint_url=args.direct_url,
        aws_access_key_id=args.access_key,
        aws_secret_access_key=args.secret_key,
        config=config,
    ) as direct_client:
        print("\n[Phase 1] Initializing Direct S3 Backend bucket...")
        await recreate_bucket(direct_client, args.bucket)

        direct_put = await run_benchmark(
            direct_client, args.bucket, payloads, "PUT", "Direct S3 Backend", args.rps, args.duration
        )
        direct_get = await run_benchmark(
            direct_client, args.bucket, payloads, "GET", "Direct S3 Backend", args.rps, args.duration
        )

        # Cleanup direct S3 bucket
        print("Cleaning up Direct S3 Backend bucket...")
        await recreate_bucket(direct_client, args.bucket)

    # 2. Proxy Benchmark
    async with session.create_client(
        "s3",
        endpoint_url=args.proxy_url,
        aws_access_key_id=args.access_key,
        aws_secret_access_key=args.secret_key,
        config=config,
    ) as proxy_client:
        print("\n[Phase 2] Initializing S3MER Proxy bucket...")
        await recreate_bucket(proxy_client, args.bucket)

        proxy_put = await run_benchmark(
            proxy_client, args.bucket, payloads, "PUT", "S3MER Proxy", args.rps, args.duration
        )
        proxy_get = await run_benchmark(
            proxy_client, args.bucket, payloads, "GET", "S3MER Proxy", args.rps, args.duration
        )

        # Cleanup proxy bucket
        print("Cleaning up S3MER Proxy bucket...")
        await recreate_bucket(proxy_client, args.bucket)

    # Display comparison
    print_results(direct_put, proxy_put, direct_get, proxy_get, args.rps)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nBenchmark interrupted.")
        sys.exit(1)
