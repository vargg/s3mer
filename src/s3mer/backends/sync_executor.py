"""Shared concurrent write execution and quorum checks."""

import asyncio
import typing as t
from collections.abc import Callable
from dataclasses import dataclass

from s3mer.backends.client import S3BackendClient
from s3mer.backends.pool import BackendPool
from s3mer.routing.operations import S3Operation


@dataclass(frozen=True, slots=True)
class SyncExecutionResult:
    """Outcome of a concurrent multi-backend write."""

    successes: list[tuple[S3BackendClient, dict[str, t.Any]]]
    failures: list[tuple[S3BackendClient, Exception]]


MULTIPART_OPERATIONS = frozenset(
    {
        S3Operation.CREATE_MULTIPART_UPLOAD,
        S3Operation.UPLOAD_PART,
        S3Operation.COMPLETE_MULTIPART_UPLOAD,
        S3Operation.ABORT_MULTIPART_UPLOAD,
    }
)


def resolve_sync_backends(pool: BackendPool, sync_backend_names: list[str] | None) -> list[S3BackendClient]:
    """Resolve configured sync backend names to clients (default: all backends)."""
    if not sync_backend_names:
        return pool.all_clients
    return [pool.get(name) for name in sync_backend_names]


def select_client_response(
    successes: list[tuple[S3BackendClient, dict[str, t.Any]]], response_backend: str | None
) -> dict[str, t.Any]:
    """Pick the response dict returned to the S3 client."""
    if not successes:
        raise RuntimeError("No successful backends to select response from")

    if response_backend is not None:
        for backend, response in successes:
            if backend.name == response_backend:
                return response

    for backend, response in successes:
        if backend.is_primary:
            return response

    return successes[0][1]


async def execute_concurrent(
    backends: list[S3BackendClient],
    operation: S3Operation,
    params_for_backend: Callable[[S3BackendClient], dict[str, t.Any]],
) -> SyncExecutionResult:
    """Execute the same logical operation on multiple backends concurrently."""
    if not backends:
        raise RuntimeError("No sync backends configured")

    tasks = [backend.execute(operation, params_for_backend(backend)) for backend in backends]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    successes: list[tuple[S3BackendClient, dict[str, t.Any]]] = []
    failures: list[tuple[S3BackendClient, Exception]] = []
    for backend, result in zip(backends, results, strict=True):
        if isinstance(result, Exception):
            failures.append((backend, result))
        elif isinstance(result, dict):
            successes.append((backend, result))
        else:
            failures.append((backend, RuntimeError(f"Unexpected concurrent result type: {type(result)!r}")))

    return SyncExecutionResult(successes=successes, failures=failures)


def quorum_met(success_count: int, sync_quorum: int) -> bool:
    return success_count >= sync_quorum


def raise_best_failure(failures: list[tuple[S3BackendClient, Exception]]) -> t.NoReturn:
    """Raise the most relevant failure for client-visible errors."""
    for backend, exc in failures:
        if backend.is_primary:
            raise exc
    raise failures[0][1]
