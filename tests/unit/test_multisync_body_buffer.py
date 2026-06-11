from collections.abc import AsyncIterator
from pathlib import Path

from s3mer.common.streaming import AsyncBytesReader, ConcurrentFileStream, MultiSyncBodyBuffer, StreamConfig


async def test_buffers_small_body_in_memory() -> None:
    config = StreamConfig(chunk_size=1024, max_memory_size=10_485_760, buffer_dir=None)
    buffer = await MultiSyncBodyBuffer.from_body(b"hello", config)
    assert buffer is not None

    reader_a = buffer.open_reader()
    reader_b = buffer.open_reader()
    assert isinstance(reader_a, AsyncBytesReader)
    assert isinstance(reader_b, AsyncBytesReader)

    assert await reader_a.read() == b"hello"
    assert await reader_b.read() == b"hello"
    await buffer.close()


async def test_buffers_stream_in_memory() -> None:
    config = StreamConfig(chunk_size=4, max_memory_size=1024, buffer_dir=None)

    async def stream() -> AsyncIterator[bytes]:
        yield b"ab"
        yield b"cd"

    buffer = await MultiSyncBodyBuffer.from_body(stream(), config)
    assert buffer is not None
    reader = buffer.open_reader()
    assert isinstance(reader, AsyncBytesReader)
    assert await reader.read() == b"abcd"
    await buffer.close()


async def test_spills_large_stream_to_disk(tmp_path: Path) -> None:
    config = StreamConfig(chunk_size=4, max_memory_size=8, buffer_dir=str(tmp_path))

    async def stream() -> AsyncIterator[bytes]:
        yield b"123456789"

    buffer = await MultiSyncBodyBuffer.from_body(stream(), config)
    assert buffer is not None
    reader = buffer.open_reader()
    assert isinstance(reader, ConcurrentFileStream)
    assert await reader.read() == b"123456789"
    await buffer.close()
