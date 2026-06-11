"""S3 Operation & Replication Strategies."""

import asyncio
import contextlib
import os
import tempfile
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any, Protocol

from s3mer.backends.client import S3BackendClient
from s3mer.backends.pool import BackendPool
from s3mer.common.errors import ErrorAction, ErrorClassifier
from s3mer.common.logging import get_logger
from s3mer.common.metrics import MetricsTracker
from s3mer.common.streaming import BufferedStreamReader, ConcurrentFileStream, StreamConfig, get_stream_config
from s3mer.kafka.manager import BaseReplicationManager
from s3mer.kafka.publisher import ReplicationPublisher
from s3mer.routing.operations import S3Operation

logger = get_logger(__name__)


class OperationStrategy(Protocol):
    """Protocol for S3 operation execution strategies."""

    async def execute(
        self,
        operation: S3Operation,
        pool: BackendPool,
        params: dict[str, Any],
        *,
        replicate: bool = True,
    ) -> Any:
        """
        Execute an S3 operation using the configured strategy.

        Args:
            operation: The S3 operation to execute.
            pool: The backend pool to use.
            params: Boto3 method parameters.

        Returns:
            The operation result (response dict or streaming body).
        """
        ...


class ReadFallbackStrategy:
    """
    Read strategy that iterates backends by priority.

    Tries each backend in priority order (lowest first).
    Returns the first successful response.
    If all backends fail, raises the last error.
    """

    async def execute(
        self,
        operation: S3Operation,
        pool: BackendPool,
        params: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Execute a read operation with fallback across backends.

        Args:
            operation: The read operation (GetObject, HeadObject, etc.).
            pool: Backend pool sorted by priority.
            params: Boto3 method parameters.

        Returns:
            The first successful response dict.

        Raises:
            The last exception if all backends fail.
        """
        backends = pool.all_by_latency()
        last_error: Exception | None = None

        for backend in backends:
            try:
                response = await backend.execute(operation, params)
                logger.info(
                    "Read operation succeeded",
                    operation=operation.value,
                    backend=backend.name,
                    bucket=params.get("Bucket"),
                    key=params.get("Key"),
                )
            except Exception as exc:
                logger.warning(
                    "Read operation failed on backend, trying next",
                    operation=operation.value,
                    backend=backend.name,
                    error=str(exc),
                )
                last_error = exc
            else:
                return response

        logger.error(
            "Read operation failed on all backends",
            operation=operation.value,
            bucket=params.get("Bucket"),
            key=params.get("Key"),
        )
        if last_error is None:
            raise RuntimeError("No backends configured")
        raise last_error


class WritePrimaryReplicationStrategy:
    """
    Write strategy that writes to the primary backend synchronously,
    then publishes a replication message to Kafka for async replication
    to secondary backends.
    """

    def __init__(
        self,
        replication_manager: BaseReplicationManager,
        metrics: MetricsTracker,
        stream_config: StreamConfig | None = None,
    ) -> None:
        self._replication_manager = replication_manager
        self._metrics = metrics
        self._stream_config = stream_config or get_stream_config()

    @property
    def publisher(self) -> ReplicationPublisher:
        """Get the underlying replication publisher."""
        return self._replication_manager.publisher

    async def execute(
        self,
        operation: S3Operation,
        pool: BackendPool,
        params: dict[str, Any],
        *,
        replicate: bool = True,
    ) -> dict[str, Any]:
        """
        Execute a write operation with fallback support.

        Tries backends in order (Primary first). If a backend fails with a
        retryable error, attempts the next available backend.
        """
        candidates = pool.get_write_candidates()

        original_body = params.get("Body")
        is_stream = False
        if original_body and isinstance(original_body, AsyncIterator):
            params["Body"] = BufferedStreamReader(
                original_body,
                self._metrics,
                stream_config=self._stream_config,
            )
            is_stream = True

        response: dict[str, Any] | None = None
        successful_backend = None
        last_error = None

        for i, backend in enumerate(candidates):
            try:
                if i > 0 and is_stream:
                    params["Body"].seek_to_start()

                response = await backend.execute(operation, params)
                successful_backend = backend
                logger.info(
                    "Write operation succeeded",
                    backend=backend.name,
                    operation=operation.value,
                    attempt=i + 1,
                )
                break
            except Exception as e:
                last_error = e
                action = ErrorClassifier.classify(e)
                if action == ErrorAction.FAIL:
                    logger.warning(
                        "Client/permanent error on write, failing immediately without fallback",
                        backend=backend.name,
                        operation=operation.value,
                        error=str(e),
                    )
                    raise
                logger.warning(
                    "Error on write, trying fallback",
                    backend=backend.name,
                    operation=operation.value,
                    action=action.value,
                    error=str(e),
                )

        if successful_backend is None or response is None:
            if last_error:
                raise last_error
            raise RuntimeError("No backends succeeded for write operation")

        if is_stream:
            params["Body"].close()

        if replicate:
            targets = [b for b in pool.all_clients if b.name != successful_backend.name]
            if targets:
                await self._replication_manager.schedule_replication(
                    operation=operation,
                    params=params,
                    response=response,
                    source_backend_name=successful_backend.name,
                    target_backend_names=[b.name for b in targets],
                )

        return response


class MultiSyncWriteStrategy:
    """
    A synchronous write strategy that writes to all configured backends concurrently.
    If any backend fails, it performs automatic rollback (deletion/cleanup) of
    successful writes and raises the failure.
    """

    def __init__(self, metrics: MetricsTracker, stream_config: StreamConfig | None = None) -> None:
        self._metrics = metrics
        self._stream_config = stream_config or get_stream_config()
        # Maps (bucket, key, primary_upload_id) -> {backend_name: backend_upload_id}
        self._upload_id_map: dict[tuple[str, str, str], dict[str, str]] = {}

    def _get_backend_upload_id(self, bucket: str, key: str | None, primary_upload_id: str, backend_name: str) -> str:
        """Resolve backend-specific upload ID from primary upload ID mapping."""
        k = key or ""
        mapping = self._upload_id_map.get((bucket, k, primary_upload_id))
        if mapping and backend_name in mapping:
            return mapping[backend_name]
        return primary_upload_id

    async def _buffer_body(self, body: Any) -> str | None:
        """Buffer incoming async stream body into a temporary file on disk."""
        if not body or not isinstance(body, AsyncIterator):
            return None

        temp_fd, temp_file_path = tempfile.mkstemp(
            prefix="s3mer_multisync_",
            dir=self._stream_config.buffer_dir,
        )
        os.close(temp_fd)

        f: Any = await asyncio.to_thread(Path(temp_file_path).open, "wb")
        try:
            async for chunk in body:
                await asyncio.to_thread(f.write, chunk)
        finally:
            await asyncio.to_thread(f.close)

        return temp_file_path

    def _prepare_params(
        self,
        backend: S3BackendClient,
        operation: S3Operation,
        params: dict[str, Any],
        temp_file_path: str | None,
        file_readers: list[ConcurrentFileStream],
    ) -> dict[str, Any]:
        """Prepare S3 parameters specifically for a given backend client."""
        backend_params = params.copy()
        if temp_file_path is not None:
            reader = ConcurrentFileStream(temp_file_path, chunk_size=self._stream_config.chunk_size)
            file_readers.append(reader)
            backend_params["Body"] = reader

        if operation in (
            S3Operation.UPLOAD_PART,
            S3Operation.COMPLETE_MULTIPART_UPLOAD,
            S3Operation.ABORT_MULTIPART_UPLOAD,
        ):
            primary_upload_id = params.get("UploadId")
            if primary_upload_id:
                bucket = params.get("Bucket", "")
                key = params.get("Key")
                backend_params["UploadId"] = self._get_backend_upload_id(bucket, key, primary_upload_id, backend.name)

        return backend_params

    def _update_multipart_mappings(
        self,
        operation: S3Operation,
        params: dict[str, Any],
        successful_backends: list[tuple[S3BackendClient, dict[str, Any]]],
    ) -> None:
        """Register or clean up multipart upload ID mappings as needed."""
        if operation == S3Operation.CREATE_MULTIPART_UPLOAD:
            primary_result = next((res for b, res in successful_backends if b.is_primary), None)
            if not primary_result and successful_backends:
                primary_result = successful_backends[0][1]

            if primary_result:
                primary_upload_id = primary_result.get("UploadId")
                if primary_upload_id:
                    bucket = params.get("Bucket", "")
                    key = params.get("Key", "")
                    mapping: dict[str, str] = {
                        b.name: str(res.get("UploadId"))
                        for b, res in successful_backends
                        if res.get("UploadId") is not None
                    }
                    self._upload_id_map[(bucket, key, primary_upload_id)] = mapping

        elif operation in (S3Operation.COMPLETE_MULTIPART_UPLOAD, S3Operation.ABORT_MULTIPART_UPLOAD):
            primary_upload_id = params.get("UploadId")
            if primary_upload_id:
                bucket = params.get("Bucket", "")
                key = params.get("Key", "")
                self._upload_id_map.pop((bucket, key, primary_upload_id), None)

    async def execute(
        self,
        operation: S3Operation,
        pool: BackendPool,
        params: dict[str, Any],
        *,
        replicate: bool = True,
    ) -> dict[str, Any]:
        _ = replicate

        original_body = params.get("Body")
        temp_file_path = None
        file_readers: list[ConcurrentFileStream] = []

        try:
            temp_file_path = await self._buffer_body(original_body)

            candidates = pool.get_write_candidates()
            if not candidates:
                raise RuntimeError("No write candidate backends configured")

            tasks = [
                backend.execute(
                    operation, self._prepare_params(backend, operation, params, temp_file_path, file_readers)
                )
                for backend in candidates
            ]

            results = await asyncio.gather(*tasks, return_exceptions=True)

            failures, successful_backends, primary_response = self._analyze_results(candidates, results)

            if failures:
                await self._rollback(operation, params, successful_backends)
                self._raise_failure(failures)

            self._update_multipart_mappings(operation, params, successful_backends)

            # If we don't have a primary response (e.g. if primary wasn't configured, though it should be),
            # use the first backend's response.
            if primary_response is None and successful_backends:
                primary_response = successful_backends[0][1]

            return primary_response

        finally:
            for reader in file_readers:
                with contextlib.suppress(Exception):
                    await reader.close()

            if temp_file_path:
                temp_path = Path(temp_file_path)
                if await asyncio.to_thread(temp_path.exists):
                    try:
                        await asyncio.to_thread(temp_path.unlink)
                    except Exception as e:
                        logger.warning("Failed to delete temp file", path=temp_file_path, error=str(e))

    def _analyze_results(
        self,
        candidates: list[S3BackendClient],
        results: list[Any],
    ) -> tuple[list[tuple[S3BackendClient, Exception]], list[tuple[S3BackendClient, dict[str, Any]]], Any]:
        """Analyze concurrent results, separating failures and successes, identifying primary response."""
        failures = []
        successful_backends = []
        primary_response = None
        for backend, result in zip(candidates, results, strict=True):
            if isinstance(result, Exception):
                failures.append((backend, result))
            else:
                successful_backends.append((backend, result))
                if backend.is_primary:
                    primary_response = result
        return failures, successful_backends, primary_response

    def _raise_failure(self, failures: list[tuple[S3BackendClient, Exception]]) -> None:
        """Prioritize raising primary backend's exception if failed, otherwise the first exception."""
        for backend, exc in failures:
            if backend.is_primary:
                raise exc
        raise failures[0][1]

    async def _rollback(
        self,
        operation: S3Operation,
        params: dict[str, Any],
        successful_backends: list[tuple[Any, Any]],
    ) -> None:
        """Rollback successful writes on partial failures."""
        if not successful_backends:
            return

        logger.warning(
            "Partial failure in multi-sync write. Initiating automatic rollback on successful backends.",
            operation=operation.value,
            successful_backends=[b.name for b, _ in successful_backends],
        )

        rollback_tasks = []
        for backend, response in successful_backends:
            bucket = params.get("Bucket", "")
            key = params.get("Key")

            rollback_op = None
            rollback_params = {}

            if operation in (S3Operation.PUT_OBJECT, S3Operation.COPY_OBJECT, S3Operation.COMPLETE_MULTIPART_UPLOAD):
                rollback_op = S3Operation.DELETE_OBJECT
                rollback_params = {"Bucket": bucket, "Key": key}
            elif operation == S3Operation.CREATE_MULTIPART_UPLOAD:
                upload_id = response.get("UploadId")
                if upload_id:
                    rollback_op = S3Operation.ABORT_MULTIPART_UPLOAD
                    rollback_params = {"Bucket": bucket, "Key": key, "UploadId": upload_id}
            elif operation == S3Operation.UPLOAD_PART:
                primary_upload_id = params.get("UploadId", "")
                backend_upload_id = self._get_backend_upload_id(bucket, key, primary_upload_id, backend.name)
                rollback_op = S3Operation.ABORT_MULTIPART_UPLOAD
                rollback_params = {"Bucket": bucket, "Key": key, "UploadId": backend_upload_id}

                # Also clean up from mapping
                self._upload_id_map.pop((bucket, key or "", primary_upload_id), None)
            elif operation == S3Operation.CREATE_BUCKET:
                rollback_op = S3Operation.DELETE_BUCKET
                rollback_params = {"Bucket": bucket}
            elif operation == S3Operation.PUT_BUCKET_LIFECYCLE:
                rollback_op = S3Operation.DELETE_BUCKET_LIFECYCLE
                rollback_params = {"Bucket": bucket}
            elif operation == S3Operation.PUT_BUCKET_POLICY:
                rollback_op = S3Operation.DELETE_BUCKET_POLICY
                rollback_params = {"Bucket": bucket}
            elif operation == S3Operation.PUT_OBJECT_TAGGING:
                rollback_op = S3Operation.DELETE_OBJECT_TAGGING
                rollback_params = {"Bucket": bucket, "Key": key}

            if rollback_op:

                async def run_rollback(
                    b: S3BackendClient = backend,
                    op: S3Operation = rollback_op,
                    p: dict[str, Any] = rollback_params,
                ) -> None:
                    try:
                        await b.execute(op, p)
                        logger.info("Rollback execution succeeded", backend=b.name, operation=op.value)
                    except Exception:
                        logger.exception("Rollback execution failed", backend=b.name, operation=op.value)

                rollback_tasks.append(run_rollback())

        if rollback_tasks:
            await asyncio.gather(*rollback_tasks, return_exceptions=True)
