"""Standalone FastStream worker application for async replication."""

from faststream.asgi import AsgiFastStream

from s3mer.backends.pool import BackendPool
from s3mer.common.logging import get_logger, setup_logging
from s3mer.common.metrics import get_tracker
from s3mer.config.settings import load_settings
from s3mer.handlers.internal import health_handler, metrics_handler
from s3mer.kafka.broker import create_broker
from s3mer.kafka.subscribers import register_subscribers

logger = get_logger(__name__)


def create_worker_app() -> AsgiFastStream:
    """Create the FastStream worker application."""
    settings = load_settings()
    setup_logging(settings.log_level, settings.log_file)

    metrics = get_tracker()
    broker = create_broker(settings.kafka)
    pool = BackendPool(
        settings.backends,
        metrics,
        settings.latency_probe_interval_seconds,
    )

    register_subscribers(
        broker,
        settings.kafka.topic,
        pool,
        settings.replication_mode,
        settings.kafka,
        metrics,
    )

    app = AsgiFastStream(
        broker,
        asgi_routes=[
            ("/.internal/metrics", metrics_handler),
            ("/.internal/health", health_handler),
        ],
    )

    @app.on_startup
    async def startup() -> None:
        logger.info("Starting s3mer worker", backends=list(settings.backends.keys()))
        await pool.start()
        logger.info("s3mer worker ready")

    @app.on_shutdown
    async def shutdown() -> None:
        logger.info("Shutting down s3mer worker")
        await pool.close()
        logger.info("s3mer worker stopped")

    return app


worker_app = create_worker_app()


if __name__ == "__main__":
    settings = load_settings()
    from granian import Granian
    from granian.constants import Interfaces

    server = Granian(
        target="s3mer.worker.app:worker_app",
        address=settings.worker.host,
        port=settings.worker.port,
        interface=Interfaces.ASGI,
    )
    server.serve()
