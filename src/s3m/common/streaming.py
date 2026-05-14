"""Async streaming utilities for proxying S3 object bodies."""

from __future__ import annotations

from collections.abc import AsyncGenerator

# Default chunk size: 64 KB — good balance between throughput and memory
DEFAULT_CHUNK_SIZE = 65_536


async def stream_s3_body(
    s3_response: dict,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> AsyncGenerator[bytes, None]:
    """
    Yield chunks from an aiobotocore StreamingBody response.

    aiobotocore wraps aiohttp's StreamReader. We use iter_chunks()
    for efficient chunked reading without buffering the full object.
    """
    stream = s3_response["Body"]
    try:
        while True:
            chunk = await stream.content.read(chunk_size)
            if not chunk:
                break
            yield chunk
    finally:
        stream.close()


async def collect_request_body(
    body_iterator: AsyncGenerator[bytes, None],
) -> bytes:
    """
    Collect an async body stream into bytes.

    Use only for small payloads (bucket creation XML, etc.).
    For large objects, stream directly to the backend.
    """
    chunks: list[bytes] = []
    async for chunk in body_iterator:
        chunks.append(chunk)
    return b"".join(chunks)
