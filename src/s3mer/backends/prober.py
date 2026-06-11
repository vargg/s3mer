"""Latency prober for background S3 backend checks."""

import asyncio
import contextlib
import time
import typing as t

from s3mer.backends.client import S3BackendClient
from s3mer.common.logging import get_logger
from s3mer.routing.operations import S3Operation

logger = get_logger(__name__)


class Prober(t.Protocol):
    """Protocol for latency probers."""

    def start(self) -> None:
        """Start the latency probing loop."""

    async def close(self) -> None:
        """Cleanly stop the latency probing task."""


class DummyLatencyProber:
    """Dummy latency prober that does nothing."""

    def __init__(self, clients: list[S3BackendClient], probe_interval: float) -> None:
        self._clients = clients
        self._probe_interval = probe_interval
        self._task: asyncio.Task[None] | None = None

    def start(self) -> None:
        """Start the dummy latency probing loop."""

    async def close(self) -> None:
        """Cleanly stop the dummy latency probing task."""


class LatencyProber:
    """
    Background worker that periodically probes S3 backends.

    Executes cheap LIST_BUCKETS calls to keep actual latency metrics updated.
    """

    def __init__(self, clients: list[S3BackendClient], probe_interval: float) -> None:
        self._clients = clients
        self._probe_interval = probe_interval
        self._task: asyncio.Task[None] | None = None

    def start(self) -> None:
        """Start the background latency probing loop."""
        if self._task is not None:
            return
        self._task = asyncio.create_task(self._run_loop())
        logger.info(
            "Latency prober started",
            probe_interval_seconds=self._probe_interval,
            backends=[c.name for c in self._clients],
        )

    async def close(self) -> None:
        """Cleanly stop the background latency probing task."""
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
            logger.info("Latency prober stopped")

    async def _run_loop(self) -> None:
        """Periodic loop that executes LIST_BUCKETS on all backends."""
        await asyncio.sleep(1.0)
        while True:
            try:
                for client in self._clients:
                    start_time = time.perf_counter()
                    try:
                        await client.execute(S3Operation.LIST_BUCKETS, {})
                        duration = time.perf_counter() - start_time
                        client.last_latency = duration
                        logger.debug(
                            "Measured backend latency",
                            backend=client.name,
                            latency_ms=duration * 1000.0,
                        )
                    except Exception as e:
                        logger.warning(
                            "Failed to measure backend latency",
                            backend=client.name,
                            error=str(e),
                        )
                        client.last_latency = float("inf")

                await asyncio.sleep(self._probe_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception("Error in latency prober background loop", error=str(e))
                await asyncio.sleep(self._probe_interval)
