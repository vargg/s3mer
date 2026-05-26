import json
import logging
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
import structlog

from s3mer.common.logging import get_logger, setup_logging


@pytest.fixture
def cleanup_logging() -> Iterator[None]:
    # Save original handlers
    root_logger = logging.getLogger()
    original_handlers = list(root_logger.handlers)
    original_level = root_logger.level

    yield

    # Restore original handlers
    root_logger = logging.getLogger()
    for handler in list(root_logger.handlers):
        root_logger.removeHandler(handler)
    for handler in original_handlers:
        root_logger.addHandler(handler)
    root_logger.setLevel(original_level)


def test_setup_logging_console_only(cleanup_logging: Iterator[None]) -> None:
    _ = cleanup_logging
    setup_logging(log_level="DEBUG")

    root_logger = logging.getLogger()
    # There should be exactly 1 handler (console)
    assert len(root_logger.handlers) == 1
    console_handler = root_logger.handlers[0]
    assert isinstance(console_handler, logging.StreamHandler)
    assert console_handler.stream is sys.stderr

    # Inspect the formatter and verify it does not contain color codes
    formatter: Any = console_handler.formatter
    assert isinstance(formatter, structlog.stdlib.ProcessorFormatter)
    assert isinstance(formatter.processors[-1], structlog.dev.ConsoleRenderer)

    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname="test.py",
        lineno=10,
        msg="test message",
        args=(),
        exc_info=None,
    )
    # The record needs to have _val (or event dict) for ProcessorFormatter to run properly.
    # Let's bind it like structlog does, or simply log using structlog and inspect what gets printed.
    # However, testing with a standard LogRecord is also possible.
    # Let's verify by actually calling a logger and capturing stderr, or just checking the formatter directly.
    # ProcessorFormatter handles foreign log records by running them through the foreign_pre_chain.
    formatted = formatter.format(record)
    assert "\x1b[" not in formatted


def test_setup_logging_with_file(tmp_path: Path, cleanup_logging: Iterator[None]) -> None:
    _ = cleanup_logging
    log_file = tmp_path / "test.log"
    setup_logging(log_level="INFO", log_file=str(log_file))

    root_logger = logging.getLogger()
    # There should be exactly 2 handlers (console and file)
    expected_handlers_count = 2
    assert len(root_logger.handlers) == expected_handlers_count

    file_handler = next(h for h in root_logger.handlers if isinstance(h, logging.FileHandler))
    assert Path(file_handler.baseFilename) == log_file

    # Verify the formatter uses JSONRenderer
    formatter: Any = file_handler.formatter
    assert isinstance(formatter, structlog.stdlib.ProcessorFormatter)
    assert isinstance(formatter.processors[-1], structlog.processors.JSONRenderer)

    # Let's log something and check the file
    logger = get_logger("test_file_logger")
    logger.info("hello file", test_key="test_val")

    # Flush the file handler
    file_handler.flush()
    file_handler.close()

    # Read the file
    assert log_file.exists()
    content = log_file.read_text(encoding="utf-8")
    assert content != ""

    # Parse as JSON
    parsed = json.loads(content.strip())
    assert parsed["event"] == "hello file"
    assert parsed["test_key"] == "test_val"
    assert parsed["level"] == "info"
    assert "timestamp" in parsed
