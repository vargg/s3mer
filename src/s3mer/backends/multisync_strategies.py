"""Multi-backend synchronous write strategies (simple, quorum, distributed)."""

from __future__ import annotations

import uuid
from typing import Any

from s3mer.backends.pool import BackendPool
from s3mer.backends.sync_executor import (
    MULTIPART_OPERATIONS,
    SyncExecutionResult,
    execute_concurrent,
    quorum_met,
    raise_best_failure,
    resolve_sync_backends,
    select_client_response,
)
from s3mer.backends.types import BackendClient
from s3mer.common.errors import OperationNotSupportedError, S3Errors
from s3mer.common.logging import get_logger
from s3mer.common.metrics import MetricsTracker
from s3mer.common.streaming import MultiSyncBodyBuffer, StreamConfig, get_stream_config
from s3mer.kafka.manager import BaseReplicationManager
from s3mer.routing.operations import S3Operation
from s3mer.state.protocol import MultipartSession, MultipartSessionStore

logger = get_logger(__name__)


class _BaseMultiBackendWriteStrategy:
    """Shared fan-out execution with optional body buffering."""

    def __init__(
        self,
        metrics: MetricsTracker,
        stream_config: StreamConfig | None = None,
        sync_backend_names: list[str] | None = None,
        sync_quorum: int = 1,
        response_backend: str | None = None,
    ) -> None:
        self._metrics = metrics
        self._stream_config = stream_config or get_stream_config()
        self._sync_backend_names = sync_backend_names
        self._sync_quorum = sync_quorum
        self._response_backend = response_backend

    def _reject_multipart_if_disabled(self, operation: S3Operation) -> None:
        if operation in MULTIPART_OPERATIONS:
            msg = (
                f"{operation.value} is not supported by this write strategy; "
                "use multi_sync_distributed or primary_replication"
            )
            raise OperationNotSupportedError(msg)

    def _prepare_backend_params(
        self,
        backend: BackendClient,
        operation: S3Operation,
        params: dict[str, Any],
        session: MultipartSession | None,
    ) -> dict[str, Any]:
        del backend, operation, session
        return params

    async def _run_concurrent(
        self,
        operation: S3Operation,
        pool: BackendPool,
        params: dict[str, Any],
        session: MultipartSession | None = None,
    ) -> tuple[SyncExecutionResult, list[BackendClient]]:
        body_buffer = await MultiSyncBodyBuffer.from_body(params.get("Body"), self._stream_config)
        try:
            backends = resolve_sync_backends(pool, self._sync_backend_names)

            def params_for_backend(backend: BackendClient) -> dict[str, Any]:
                backend_params = self._prepare_backend_params(backend, operation, params, session)
                if body_buffer is not None and "Body" in backend_params:
                    backend_params = backend_params.copy()
                    backend_params["Body"] = body_buffer.open_reader()
                return backend_params

            result = await execute_concurrent(backends, operation, params_for_backend)
            return result, backends
        finally:
            if body_buffer is not None:
                await body_buffer.close()

    async def _schedule_async_fill(
        self,
        replication_manager: BaseReplicationManager,
        operation: S3Operation,
        pool: BackendPool,
        params: dict[str, Any],
        response: dict[str, Any],
        successes: list[tuple[BackendClient, dict[str, Any]]],
    ) -> None:
        successful_names = {backend.name for backend, _ in successes}
        targets = [client.name for client in pool.all_clients if client.name not in successful_names]
        if not targets:
            return
        source_backend = next(
            (backend.name for backend, _ in successes if backend.name == self._response_backend),
            successes[0][0].name,
        )
        await replication_manager.schedule_replication(
            operation=operation,
            params=params,
            response=response,
            source_backend_name=source_backend,
            target_backend_names=targets,
        )


class SimpleMultiSyncWriteStrategy(_BaseMultiBackendWriteStrategy):
    """
      Stateless concurrent writes to all sync backends.

    Multipart uploads are not supported. No rollback; partial success returns non-2xx.
    """

    async def execute(
        self,
        operation: S3Operation,
        pool: BackendPool,
        params: dict[str, Any],
        *,
        replicate: bool = True,
    ) -> dict[str, Any]:
        del replicate
        self._reject_multipart_if_disabled(operation)

        backends = resolve_sync_backends(pool, self._sync_backend_names)
        required = len(backends)
        result, _ = await self._run_concurrent(operation, pool, params)

        if not quorum_met(len(result.successes), required):
            logger.warning(
                "Simple multi-sync write failed quorum",
                operation=operation.value,
                required=required,
                successes=[backend.name for backend, _ in result.successes],
                failures=[(backend.name, str(exc)) for backend, exc in result.failures],
            )
            raise_best_failure(result.failures)

        return select_client_response(result.successes, self._response_backend)


class QuorumReplicationStrategy(_BaseMultiBackendWriteStrategy):
    """
    Concurrent writes with a configurable quorum.

    Backends that miss the synchronous quorum are filled asynchronously via Kafka.
    Multipart uploads are not supported.
    """

    def __init__(
        self,
        replication_manager: BaseReplicationManager,
        metrics: MetricsTracker,
        stream_config: StreamConfig | None = None,
        sync_backend_names: list[str] | None = None,
        sync_quorum: int = 1,
        response_backend: str | None = None,
    ) -> None:
        super().__init__(metrics, stream_config, sync_backend_names, sync_quorum, response_backend)
        self._replication_manager = replication_manager

    async def execute(
        self,
        operation: S3Operation,
        pool: BackendPool,
        params: dict[str, Any],
        *,
        replicate: bool = True,
    ) -> dict[str, Any]:
        self._reject_multipart_if_disabled(operation)

        result, _ = await self._run_concurrent(operation, pool, params)
        if not quorum_met(len(result.successes), self._sync_quorum):
            logger.warning(
                "Quorum write failed",
                operation=operation.value,
                quorum=self._sync_quorum,
                successes=[backend.name for backend, _ in result.successes],
                failures=[(backend.name, str(exc)) for backend, exc in result.failures],
            )
            raise_best_failure(result.failures)

        response = select_client_response(result.successes, self._response_backend)
        if replicate:
            await self._schedule_async_fill(
                self._replication_manager,
                operation,
                pool,
                params,
                response,
                result.successes,
            )
        return response


class DistributedMultiSyncWriteStrategy(_BaseMultiBackendWriteStrategy):
    """
    Horizontally scalable multi-sync with Valkey-backed multipart sessions.

    Returns a proxy-issued UUID upload ID to clients and maps per-backend native IDs internally.
    """

    def __init__(
        self,
        replication_manager: BaseReplicationManager,
        session_store: MultipartSessionStore,
        metrics: MetricsTracker,
        stream_config: StreamConfig | None = None,
        sync_backend_names: list[str] | None = None,
        sync_quorum: int = 1,
        response_backend: str | None = None,
    ) -> None:
        super().__init__(metrics, stream_config, sync_backend_names, sync_quorum, response_backend)
        self._replication_manager = replication_manager
        self._session_store = session_store

    def _prepare_backend_params(
        self,
        backend: BackendClient,
        operation: S3Operation,
        params: dict[str, Any],
        session: MultipartSession | None,
    ) -> dict[str, Any]:
        backend_params = params.copy()
        if session is None:
            return backend_params

        native_upload_id = session.backend_upload_ids.get(backend.name)
        if native_upload_id and operation in MULTIPART_OPERATIONS:
            backend_params["UploadId"] = native_upload_id

        if operation == S3Operation.COMPLETE_MULTIPART_UPLOAD:
            client_parts = params.get("MultipartUpload", {}).get("Parts", [])
            backend_parts = []
            for part in client_parts:
                part_number = part["PartNumber"]
                etag = session.part_etags.get(part_number, {}).get(backend.name)
                if etag is not None:
                    backend_parts.append({"PartNumber": part_number, "ETag": etag})
            backend_params["MultipartUpload"] = {"Parts": backend_parts}

        return backend_params

    async def _handle_create_multipart(
        self,
        pool: BackendPool,
        params: dict[str, Any],
        *,
        replicate: bool,
    ) -> dict[str, Any]:
        canonical_upload_id = str(uuid.uuid4())
        bucket = params.get("Bucket", "")
        key = params.get("Key", "")
        await self._session_store.create_session(bucket, key, canonical_upload_id)

        create_params = {k: v for k, v in params.items() if k != "UploadId"}
        try:
            result, _ = await self._run_concurrent(
                S3Operation.CREATE_MULTIPART_UPLOAD,
                pool,
                create_params,
            )
            if not quorum_met(len(result.successes), self._sync_quorum):
                raise_best_failure(result.failures)

            backend_upload_ids = {
                backend.name: str(response["UploadId"])
                for backend, response in result.successes
                if response.get("UploadId") is not None
            }
            await self._session_store.set_backend_upload_ids(canonical_upload_id, backend_upload_ids)

            response = select_client_response(result.successes, self._response_backend)
            response = dict(response)
            response["UploadId"] = canonical_upload_id
            if replicate:
                await self._schedule_async_fill(
                    self._replication_manager,
                    S3Operation.CREATE_MULTIPART_UPLOAD,
                    pool,
                    params,
                    response,
                    result.successes,
                )
        except Exception:
            await self._session_store.delete_session(canonical_upload_id)
            raise
        else:
            return response

    async def _load_session(self, canonical_upload_id: str) -> MultipartSession:
        session = await self._session_store.get_session(canonical_upload_id)
        if session is None:
            msg = (
                "The specified upload does not exist. "
                "The upload ID may be invalid, or the upload may have been aborted."
            )
            raise OperationNotSupportedError(msg, error_code=S3Errors.NO_SUCH_UPLOAD)
        return session

    async def _handle_upload_part(
        self,
        pool: BackendPool,
        params: dict[str, Any],
    ) -> dict[str, Any]:
        canonical_upload_id = params.get("UploadId", "")
        session = await self._load_session(canonical_upload_id)
        result, _ = await self._run_concurrent(S3Operation.UPLOAD_PART, pool, params, session=session)
        if not quorum_met(len(result.successes), self._sync_quorum):
            raise_best_failure(result.failures)

        part_number = int(params.get("PartNumber", 0))
        backend_etags = {
            backend.name: str(response["ETag"]) for backend, response in result.successes if "ETag" in response
        }
        await self._session_store.record_part_etags(canonical_upload_id, part_number, backend_etags)
        return select_client_response(result.successes, self._response_backend)

    async def _handle_complete_or_abort(
        self,
        operation: S3Operation,
        pool: BackendPool,
        params: dict[str, Any],
        *,
        replicate: bool,
    ) -> dict[str, Any]:
        canonical_upload_id = params.get("UploadId", "")
        session = await self._load_session(canonical_upload_id)
        result, _ = await self._run_concurrent(operation, pool, params, session=session)
        if not quorum_met(len(result.successes), self._sync_quorum):
            raise_best_failure(result.failures)

        response = select_client_response(result.successes, self._response_backend)
        await self._session_store.delete_session(canonical_upload_id)
        if replicate:
            await self._schedule_async_fill(
                self._replication_manager,
                operation,
                pool,
                params,
                response,
                result.successes,
            )
        return response

    async def execute(
        self,
        operation: S3Operation,
        pool: BackendPool,
        params: dict[str, Any],
        *,
        replicate: bool = True,
    ) -> dict[str, Any]:
        if operation == S3Operation.CREATE_MULTIPART_UPLOAD:
            return await self._handle_create_multipart(pool, params, replicate=replicate)
        if operation == S3Operation.UPLOAD_PART:
            return await self._handle_upload_part(pool, params)
        if operation in (S3Operation.COMPLETE_MULTIPART_UPLOAD, S3Operation.ABORT_MULTIPART_UPLOAD):
            return await self._handle_complete_or_abort(operation, pool, params, replicate=replicate)

        result, _ = await self._run_concurrent(operation, pool, params)
        if not quorum_met(len(result.successes), self._sync_quorum):
            raise_best_failure(result.failures)

        response = select_client_response(result.successes, self._response_backend)
        if replicate:
            await self._schedule_async_fill(
                self._replication_manager,
                operation,
                pool,
                params,
                response,
                result.successes,
            )
        return response


# Backward-compatible alias for the legacy name.
MultiSyncWriteStrategy = SimpleMultiSyncWriteStrategy
