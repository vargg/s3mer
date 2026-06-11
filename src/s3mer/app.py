"""Pure ASGI application that proxies S3 requests to configured backends."""

from typing import Any

from s3mer.backends.pool import BackendPool
from s3mer.backends.strategies import (
    MultiSyncWriteStrategy,
    ReadFallbackStrategy,
    WritePrimaryReplicationStrategy,
)
from s3mer.common.logging import get_logger, setup_logging
from s3mer.common.metrics import get_tracker
from s3mer.common.streaming import StreamConfig
from s3mer.common.types import Receive, Scope, Send
from s3mer.config.settings import ReplicationMode, WriteStrategyType, load_settings
from s3mer.kafka.broker import create_broker
from s3mer.kafka.manager import BatchReplicationManager, PerBackendReplicationManager
from s3mer.kafka.publisher import ReplicationPublisher
from s3mer.routing.classifier import RequestClassifier
from s3mer.routing.dispatcher import RequestDispatcher
from s3mer.routing.http_handler import S3HTTPHandler

logger = get_logger(__name__)


class S3ProxyApp:
    """
    Pure ASGI application that intercepts all HTTP requests,
    classifies them as S3 operations, and dispatches to the
    appropriate handler via read/write strategies.
    """

    def __init__(self) -> None:
        settings = load_settings()
        metrics_tracker = get_tracker()
        stream_config = StreamConfig.from_settings(settings)

        self._broker = create_broker(settings.kafka)
        self._pool = BackendPool(
            settings.backends,
            metrics_tracker,
            settings.latency_probe_interval_seconds,
        )

        publisher = ReplicationPublisher(self._broker, settings.kafka.topic)
        if settings.replication_mode == ReplicationMode.PER_BACKEND:
            replication_manager = PerBackendReplicationManager(publisher, metrics_tracker)
        else:
            replication_manager = BatchReplicationManager(publisher, metrics_tracker)

        if settings.write_strategy == WriteStrategyType.MULTI_SYNC:
            write_strategy: Any = MultiSyncWriteStrategy(metrics_tracker, stream_config)
        else:
            write_strategy = WritePrimaryReplicationStrategy(
                replication_manager,
                metrics_tracker,
                stream_config,
            )

        dispatcher = RequestDispatcher(
            self._pool,
            ReadFallbackStrategy(),
            write_strategy,
            metrics_tracker,
            stream_config,
        )

        self._http_handler = S3HTTPHandler(
            RequestClassifier(),
            dispatcher,
            metrics_tracker,
        )

    async def startup(self) -> None:
        """Initialize all components. Called once by the ASGI server."""
        settings = load_settings()
        setup_logging(settings.log_level, settings.log_file)

        log = get_logger("s3mer.startup")
        log.info("Starting s3mer proxy", backends=list(settings.backends.keys()))

        await self._pool.start()
        await self._broker.start()

        log.info("s3mer proxy ready")

    async def shutdown(self) -> None:
        """Clean up resources. Called once by the ASGI server."""
        log = get_logger("s3mer.shutdown")
        log.info("Shutting down s3mer proxy")

        await self._broker.stop()
        await self._pool.close()

        log.info("s3mer proxy stopped")

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """ASGI entry point."""
        call_type = scope["type"]
        if call_type == "http":
            await self._http_handler(scope, receive, send)
        elif call_type == "lifespan":
            await self._handle_lifespan(scope, receive, send)
        else:
            logger.warning("Unsupported call type", call_type=call_type)

    async def _handle_lifespan(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Handle ASGI lifespan events (startup/shutdown)."""
        del scope
        while True:
            message = await receive()
            if message["type"] == "lifespan.startup":
                try:
                    await self.startup()
                    await send({"type": "lifespan.startup.complete"})
                except Exception as exc:
                    logger.exception("Startup failed", error=str(exc))
                    await send({"type": "lifespan.startup.failed", "message": str(exc)})
                    return
            elif message["type"] == "lifespan.shutdown":
                await self.shutdown()
                await send({"type": "lifespan.shutdown.complete"})
                return


def create_app() -> S3ProxyApp:
    """Create the ASGI application."""
    return S3ProxyApp()
