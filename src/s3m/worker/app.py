"""Standalone FastStream worker application for async replication."""

from __future__ import annotations

import asyncio

from faststream import FastStream

from s3m.backends.pool import BackendPool
from s3m.common.logging import get_logger, setup_logging
from s3m.config.settings import load_settings
from s3m.kafka.broker import create_broker
from s3m.kafka.subscribers import register_subscribers

logger = get_logger(__name__)


def create_worker_app() -> FastStream:
    """Create the FastStream worker application."""
    settings = load_settings()
    setup_logging(settings.log_level)

    broker = create_broker(settings.kafka)
    pool = BackendPool(settings.backends)

    # Register the replication subscriber
    register_subscribers(broker, settings.kafka.topic, pool)

    app = FastStream(broker)

    @app.on_startup
    async def startup() -> None:
        logger.info("Starting s3m worker", backends=[b.name for b in settings.backends])
        await pool.start()
        logger.info("s3m worker ready")

    @app.on_shutdown
    async def shutdown() -> None:
        logger.info("Shutting down s3m worker")
        await pool.close()
        logger.info("s3m worker stopped")

    return app


worker_app = create_worker_app()


if __name__ == "__main__":
    asyncio.run(worker_app.run())
