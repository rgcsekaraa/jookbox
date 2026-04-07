#!/usr/bin/env python3
import os

from backend.main import app, create_app


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "backend.main:create_app",
        host=os.environ.get("ISAIBOX_HOST", "127.0.0.1"),
        port=int(os.environ.get("ISAIBOX_PORT", "8000")),
        factory=True,
        log_level=os.environ.get("ISAIBOX_LOG_LEVEL", "info"),
    )
