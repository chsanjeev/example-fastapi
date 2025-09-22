"""
This module sets up the FastAPI application for the Example FastAPI + DuckDB project.

Features:
- Initializes the FastAPI app with a custom title.
- Adds middleware to generate and log a unique request ID for each HTTP request, enhancing observability.
- Logs request method, path, status code, duration, and request ID for each request.
- Includes modular routers for item management and health checks.
- Provides an entry point for running the application with Uvicorn.

Designed for modularity, observability, and maintainability in API development.
"""

import logging
import os
import time

from fastapi import FastAPI, Request

from .health import router as health_router
from .routes import router as items_router

# Select DB backend
DB_BACKEND = os.getenv("DB_BACKEND", "duckdb")

logger = logging.getLogger(__name__)


def create_app() -> FastAPI:
    """
    This module defines the FastAPI application setup for the Example FastAPI + DuckDB project.

    It provides the `create_app` function, which initializes a FastAPI app with:
    - Middleware for generating and logging a unique request ID for each HTTP request.
    - Logging of request method, path, status code, duration, and request ID.
    - Inclusion of routers for item management and health checks.

    The application is designed to facilitate observability and modular routing.
    """
    app = FastAPI(title="Example FastAPI + DuckDB")

    # request id + simple request logging middleware
    @app.middleware("http")
    async def request_id_middleware(request: Request, call_next):
        # prefer incoming header or generate one
        req_id = request.headers.get("x-request-id")
        if not req_id:
            import uuid

            req_id = str(uuid.uuid4())

        request.state.request_id = req_id
        start = time.time()
        response = await call_next(request)
        duration = (time.time() - start) * 1000
        # include request id in response headers
        response.headers["X-Request-ID"] = req_id
        logger.info("%s %s %s %.2fms request_id=%s", request.method, request.url.path, response.status_code, duration, req_id)
        return response

    print(f"Using DB backend: {DB_BACKEND}")
    app.include_router(items_router)
    app.include_router(health_router)
    return app


app = create_app()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
