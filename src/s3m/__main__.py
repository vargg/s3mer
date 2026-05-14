"""Entrypoint for running the s3m proxy server."""

import uvicorn

from s3m.app import create_app

app = create_app()

if __name__ == "__main__":
    uvicorn.run(
        "s3m.__main__:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info",
    )
