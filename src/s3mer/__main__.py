"""Entrypoint for running the s3mer proxy server."""

import uvicorn

from s3mer.app import create_app

app = create_app()

if __name__ == "__main__":
    uvicorn.run(
        "s3mer.__main__:app",
        host="0.0.0.0",  # noqa: S104
        port=8000,
        reload=True,
        log_level="info",
    )
