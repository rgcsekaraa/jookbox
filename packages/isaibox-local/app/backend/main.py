from __future__ import annotations

import os

import uvicorn

from backend.services import app
import backend.routers.api  # noqa: F401
import backend.routers.spa  # noqa: F401


def create_app():
    return app


if __name__ == "__main__":
    uvicorn.run(
        "backend.main:create_app",
        host=os.environ.get("ISAIBOX_HOST", "127.0.0.1"),
        port=int(os.environ.get("ISAIBOX_PORT", "8000")),
        factory=True,
        log_level=os.environ.get("ISAIBOX_LOG_LEVEL", "info"),
    )
