"""Standalone FastStream worker application for async replication."""

import asyncio

from faststream import FastStream

from s3mer.backends.pool import BackendPool
from s3mer.common.logging import get_logger, setup_logging
from s3mer.common.metrics import get_tracker
from s3mer.config.settings import load_settings
from s3mer.kafka.broker import create_broker
from s3mer.kafka.subscribers import register_subscribers

logger = get_logger(__name__)


def create_worker_app() -> FastStream:
    """Create the FastStream worker application."""
    settings = load_settings()
    setup_logging(settings.log_level)

    metrics = get_tracker()
    broker = create_broker(settings.kafka)
    pool = BackendPool(settings.backends, metrics)

    # Register the replication subscriber
    register_subscribers(
        broker,
        settings.kafka.topic,
        pool,
        settings.replication_mode,
        settings.kafka,
    )

    app = FastStream(broker)

    @app.on_startup
    async def startup() -> None:
        logger.info("Starting s3mer worker", backends=[b.name for b in settings.backends])
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
    asyncio.run(worker_app.run())
