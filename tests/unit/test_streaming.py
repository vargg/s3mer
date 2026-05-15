import pytest

from s3m.common.streaming import ASGIStreamReader, AWSChunkedDecoder


class MockReceive:
    def __init__(self, chunks: list[bytes]) -> None:
        self.chunks = chunks
        self.index = 0

    async def __call__(self) -> dict:
        if self.index < len(self.chunks):
            chunk = self.chunks[self.index]
            self.index += 1
            return {"type": "http.request", "body": chunk, "more_body": self.index < len(self.chunks)}
        return {"type": "http.disconnect"}


@pytest.mark.asyncio
async def test_aws_chunked_decoder() -> None:
    payload = b"5;chunk-signature=12345\r\nhello\r\n6;chunk-signature=67890\r\n world\r\n0;chunk-signature=abcd\r\n"

    # Split payload into artificial ASGI chunks
    asgi_chunks = [payload[:10], payload[10:20], payload[20:]]
    receive = MockReceive(asgi_chunks)
    reader = ASGIStreamReader(receive)
    decoder = AWSChunkedDecoder(reader)

    result = await decoder.read()
    assert result == b"hello world"


@pytest.mark.asyncio
async def test_aws_chunked_decoder_read_chunks() -> None:
    payload = b"5;chunk-signature=12345\r\nhello\r\n6;chunk-signature=67890\r\n world\r\n0;chunk-signature=abcd\r\n"

    receive = MockReceive([payload])
    reader = ASGIStreamReader(receive)
    decoder = AWSChunkedDecoder(reader)

    # Read partial
    part1 = await decoder.read(3)
    assert part1 == b"hel"

    part2 = await decoder.read(4)
    assert part2 == b"lo w"

    part3 = await decoder.read(10)
    assert part3 == b"orld"

    part4 = await decoder.read(10)
    assert part4 == b""
