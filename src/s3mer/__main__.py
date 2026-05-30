"""Entrypoint for running the s3mer proxy server."""

from granian import Granian
from granian.constants import Interfaces

if __name__ == "__main__":
    server = Granian(
        target="s3mer.app:create_app",
        address="0.0.0.0",  # noqa: S104
        port=8000,
        interface=Interfaces.ASGI,
        factory=True,
        reload=True,
    )
    server.serve()
