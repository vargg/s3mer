#!/usr/bin/env python3
"""Performance testing script for S3MER.

Compares direct S3 backend operations vs operations through S3MER proxy.
Generates a target request rate (default 50 RPS) with objects of sizes 0.5MB to 2MB.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import math
import os
import random
import sys
import time
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from aiobotocore.config import AioConfig
from aiobotocore.session import get_session

PAYLOAD_COUNT = 20


class OperationType(StrEnum):
    PUT = "PUT"
    GET = "GET"


@dataclass(frozen=True, slots=True)
class PerfTestConfig:
    proxy_url: str
    direct_url: str
    access_key: str
    secret_key: str
    bucket: str
    rps: float
    duration: float
    min_size_kb: int
    max_size_kb: int
    pool_size: int

    @classmethod
    def from_args(cls, args: argparse.Namespace) -> PerfTestConfig:
        return cls(
            proxy_url=args.proxy_url,
            direct_url=args.direct_url,
            access_key=args.access_key,
            secret_key=args.secret_key,
            bucket=args.bucket,
            rps=args.rps,
            duration=args.duration,
            min_size_kb=args.min_size_kb,
            max_size_kb=args.max_size_kb,
            pool_size=args.pool_size,
        )

    def aio_config(self) -> AioConfig:
        return AioConfig(
            s3={"addressing_style": "path", "payload_signing_enabled": False},
            max_pool_connections=self.pool_size,
            connect_timeout=5.0,
            read_timeout=15.0,
        )


@dataclass(slots=True)
class BenchmarkResult:
    latencies: list[float] = field(default_factory=list)
    success: int = 0
    failure: int = 0
    duration: float = 0.0
    bytes: int = 0
    total_requests: int = 0


@dataclass(frozen=True, slots=True)
class BenchmarkMetrics:
    success_rate: float
    actual_rps: float
    throughput_mbps: float
    mean_ms: float
    p50_ms: float
    p90_ms: float
    p95_ms: float
    p99_ms: float

    @classmethod
    def from_result(cls, result: BenchmarkResult) -> BenchmarkMetrics:
        latencies = result.latencies
        duration = result.duration or 1.0
        total = result.total_requests

        return cls(
            success_rate=(result.success / total * 100) if total > 0 else 0.0,
            actual_rps=total / duration,
            throughput_mbps=(result.bytes / (1024 * 1024)) / duration,
            mean_ms=(sum(latencies) / len(latencies) * 1000) if latencies else 0.0,
            p50_ms=Percentiles.calculate(latencies, 0.50) * 1000,
            p90_ms=Percentiles.calculate(latencies, 0.90) * 1000,
            p95_ms=Percentiles.calculate(latencies, 0.95) * 1000,
            p99_ms=Percentiles.calculate(latencies, 0.99) * 1000,
        )


class Percentiles:
    """Linear-interpolation percentile helper."""

    @staticmethod
    def calculate(data: list[float], percent: float) -> float:
        if not data:
            return 0.0
        sorted_data = sorted(data)
        k = (len(sorted_data) - 1) * percent
        floor_idx = math.floor(k)
        ceil_idx = math.ceil(k)
        if floor_idx == ceil_idx:
            return sorted_data[int(k)]
        d0 = sorted_data[floor_idx] * (ceil_idx - k)
        d1 = sorted_data[ceil_idx] * (k - floor_idx)
        return d0 + d1


class PayloadGenerator:
    def __init__(self, min_size_kb: int, max_size_kb: int, count: int = PAYLOAD_COUNT) -> None:
        self._min_size_kb = min_size_kb
        self._max_size_kb = max_size_kb
        self._count = count

    def generate(self) -> list[bytes]:
        print(
            f"Pre-generating {self._count} random payloads "
            f"({self._min_size_kb}KB to {self._max_size_kb}KB)..."
        )
        return [
            os.urandom(random.randint(self._min_size_kb * 1024, self._max_size_kb * 1024))
            for _ in range(self._count)
        ]


class S3Bucket:
    def __init__(self, client: Any, name: str) -> None:
        self._client = client
        self.name = name

    async def recreate(self) -> None:
        """Ensure the bucket exists and is empty."""
        with contextlib.suppress(Exception):
            paginator = self._client.get_paginator("list_objects_v2")
            async for page in paginator.paginate(Bucket=self.name):
                if "Contents" in page:
                    objects = [{"Key": obj["Key"]} for obj in page["Contents"]]
                    await self._client.delete_objects(Bucket=self.name, Delete={"Objects": objects})

        with contextlib.suppress(Exception):
            await self._client.delete_bucket(Bucket=self.name)

        await self._client.create_bucket(Bucket=self.name)


class BenchmarkRunner:
    def __init__(self, client: Any, bucket: S3Bucket, target_name: str) -> None:
        self._client = client
        self._bucket = bucket
        self._target_name = target_name

    async def run(
        self,
        operation: OperationType,
        payloads: list[bytes],
        rps: float,
        duration: float,
    ) -> BenchmarkResult:
        print(f"  Starting {operation.value} benchmark against {self._target_name} at {rps} RPS...")

        result = BenchmarkResult()
        start_time = time.perf_counter()
        end_time = start_time + duration
        interval = 1.0 / rps
        next_request_time = time.perf_counter()
        tasks: list[asyncio.Task[None]] = []
        request_index = 0

        while time.perf_counter() < end_time:
            now = time.perf_counter()
            if now >= next_request_time:
                key = f"perf_obj_{request_index}"
                payload = payloads[request_index % len(payloads)]
                tasks.append(asyncio.create_task(self._execute(operation, key, payload, result)))
                request_index += 1
                next_request_time += interval
            else:
                await asyncio.sleep(min(0.001, next_request_time - now))

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        result.duration = time.perf_counter() - start_time
        result.total_requests = request_index
        return result

    async def _execute(
        self,
        operation: OperationType,
        key: str,
        payload: bytes,
        result: BenchmarkResult,
    ) -> None:
        req_start = time.perf_counter()
        try:
            if operation == OperationType.PUT:
                await self._client.put_object(Bucket=self._bucket.name, Key=key, Body=payload)
                result.bytes += len(payload)
            else:
                response = await self._client.get_object(Bucket=self._bucket.name, Key=key)
                body_data = await response["Body"].read()
                result.bytes += len(body_data)
            result.latencies.append(time.perf_counter() - req_start)
            result.success += 1
        except Exception:
            result.failure += 1


@dataclass(frozen=True, slots=True)
class TargetBenchmarks:
    put: BenchmarkResult
    get: BenchmarkResult

    @property
    def put_metrics(self) -> BenchmarkMetrics:
        return BenchmarkMetrics.from_result(self.put)

    @property
    def get_metrics(self) -> BenchmarkMetrics:
        return BenchmarkMetrics.from_result(self.get)


class S3BenchmarkTarget:
    def __init__(self, client: Any, bucket_name: str, label: str) -> None:
        self._client = client
        self._bucket = S3Bucket(client, bucket_name)
        self.label = label

    async def initialize(self) -> None:
        print(f"\nInitializing {self.label} bucket...")
        await self._bucket.recreate()

    async def run_phase(
        self,
        payloads: list[bytes],
        rps: float,
        duration: float,
    ) -> TargetBenchmarks:
        runner = BenchmarkRunner(self._client, self._bucket, self.label)
        put_result = await runner.run(OperationType.PUT, payloads, rps, duration)
        get_result = await runner.run(OperationType.GET, payloads, rps, duration)
        return TargetBenchmarks(put=put_result, get=get_result)

    async def cleanup(self) -> None:
        print(f"Cleaning up {self.label} bucket...")
        await self._bucket.recreate()


class ComparisonReport:
    WIDTH = 80

    def __init__(
        self,
        direct: TargetBenchmarks,
        proxy: TargetBenchmarks,
        target_rps: float,
    ) -> None:
        self._direct = direct
        self._proxy = proxy
        self._target_rps = target_rps

    def print(self) -> None:
        line = "=" * self.WIDTH
        print(line)
        print(f"{'S3MER PERFORMANCE COMPARISON':^{self.WIDTH}}")
        print(line)
        print(f"{'Metric':<25} | {'Direct S3':<15} | {'S3MER Proxy':<15} | {'Difference':<15}")
        print("-" * self.WIDTH)
        self._print_section("PUT Operations", self._direct.put_metrics, self._proxy.put_metrics)
        print("-" * self.WIDTH)
        self._print_section("GET Operations", self._direct.get_metrics, self._proxy.get_metrics)
        print(line)

    def _print_section(
        self,
        title: str,
        direct: BenchmarkMetrics,
        proxy: BenchmarkMetrics,
    ) -> None:
        print(f"  [{title}]")
        self._print_row("  Target RPS", self._target_rps, self._target_rps, diff=False)
        self._print_row("  Actual RPS", direct.actual_rps, proxy.actual_rps)
        self._print_row_percent("  Success Rate", direct.success_rate, proxy.success_rate)
        self._print_row_ms("  Mean Latency", direct.mean_ms, proxy.mean_ms)
        self._print_row_ms("  P50 Latency", direct.p50_ms, proxy.p50_ms)
        self._print_row_ms("  P90 Latency", direct.p90_ms, proxy.p90_ms)
        self._print_row_ms("  P95 Latency", direct.p95_ms, proxy.p95_ms)
        self._print_row_ms("  P99 Latency", direct.p99_ms, proxy.p99_ms)
        self._print_row_throughput("  Throughput", direct.throughput_mbps, proxy.throughput_mbps)

    @staticmethod
    def _format_diff(direct: float, proxy: float) -> str:
        if direct == 0:
            return "-"
        diff = ((proxy - direct) / direct) * 100
        sign = "+" if diff >= 0 else ""
        return f"{sign}{diff:.2f}%"

    def _print_row(
        self,
        label: str,
        direct: float,
        proxy: float,
        *,
        diff: bool = True,
    ) -> None:
        difference = self._format_diff(direct, proxy) if diff else "-"
        print(f"{label:<25} | {direct:<15.2f} | {proxy:<15.2f} | {difference}")

    def _print_row_percent(self, label: str, direct: float, proxy: float) -> None:
        print(f"{label:<25} | {direct:<14.2f}% | {proxy:<14.2f}% | -")

    def _print_row_ms(self, label: str, direct: float, proxy: float) -> None:
        difference = self._format_diff(direct, proxy)
        print(f"{label:<25} | {direct:<12.2f} ms | {proxy:<12.2f} ms | {difference}")

    def _print_row_throughput(self, label: str, direct: float, proxy: float) -> None:
        difference = self._format_diff(direct, proxy)
        print(f"{label:<25} | {direct:<10.2f} MB/s | {proxy:<10.2f} MB/s | {difference}")


class PerfComparison:
    def __init__(self, config: PerfTestConfig) -> None:
        self._config = config
        self._payloads = PayloadGenerator(config.min_size_kb, config.max_size_kb).generate()
        self._session = get_session()

    async def run(self) -> None:
        direct = await self._run_target(
            phase_label="Phase 1",
            endpoint_url=self._config.direct_url,
            target_label="Direct S3 Backend",
        )
        proxy = await self._run_target(
            phase_label="Phase 2",
            endpoint_url=self._config.proxy_url,
            target_label="S3MER Proxy",
        )
        ComparisonReport(direct, proxy, self._config.rps).print()

    async def _run_target(
        self,
        phase_label: str,
        endpoint_url: str,
        target_label: str,
    ) -> TargetBenchmarks:
        print(f"\n[{phase_label}] {target_label}")
        async with self._session.create_client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=self._config.access_key,
            aws_secret_access_key=self._config.secret_key,
            config=self._config.aio_config(),
        ) as client:
            target = S3BenchmarkTarget(client, self._config.bucket, target_label)
            await target.initialize()
            results = await target.run_phase(self._payloads, self._config.rps, self._config.duration)
            await target.cleanup()
            return results


def parse_args() -> PerfTestConfig:
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
    return PerfTestConfig.from_args(parser.parse_args())


async def main() -> None:
    await PerfComparison(parse_args()).run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nBenchmark interrupted.")
        sys.exit(1)
