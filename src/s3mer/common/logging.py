"""Structured logging configuration using structlog."""

import logging
import sys

import structlog


def setup_logging(log_level: str = "INFO", log_file: str | None = None) -> None:
    """Configure structlog with console (no color) and optional JSON file output."""
    shared_processors = (
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.dev.set_exc_info,
        structlog.processors.TimeStamper(fmt="iso"),
    )

    structlog.configure(
        processors=(
            *shared_processors,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ),
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    root_logger = logging.getLogger()

    for handler in list(root_logger.handlers):
        root_logger.removeHandler(handler)

    level = getattr(logging, log_level.upper(), logging.INFO)
    root_logger.setLevel(level)

    console_handler = logging.StreamHandler(sys.stderr)
    console_formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=shared_processors,
        processor=structlog.dev.ConsoleRenderer(colors=False),
    )
    console_handler.setFormatter(console_formatter)
    console_handler.setLevel(level)
    root_logger.addHandler(console_handler)

    if log_file:
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_formatter = structlog.stdlib.ProcessorFormatter(
            foreign_pre_chain=shared_processors,
            processor=structlog.processors.JSONRenderer(),
        )
        file_handler.setFormatter(file_formatter)
        file_handler.setLevel(level)
        root_logger.addHandler(file_handler)


def get_logger(name: str | None = None, **kwargs: object) -> structlog.stdlib.BoundLogger:
    """Get a bound logger instance with optional initial context."""
    logger: structlog.stdlib.BoundLogger = structlog.get_logger(name)
    if kwargs:
        logger = logger.bind(**kwargs)
    return logger
