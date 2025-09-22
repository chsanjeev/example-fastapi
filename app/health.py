"""
This module provides health and readiness endpoints for the FastAPI application.

Endpoints:
- /health: Returns a simple status indicating the application is running.
- /ready: Checks the application's readiness by verifying the database connection.
          Readiness behavior is configurable via environment variables:
            - EXAMPLE_FASTAPI_READY_RETRIES: Number of DB connection retries (default: 3)
            - EXAMPLE_FASTAPI_READY_DELAY: Delay between retries in seconds (default: 0.1)
            - EXAMPLE_FASTAPI_READY_TIMEOUT: Timeout for readiness check in seconds (default: 1.0)
          Responds with HTTP 503 if the database is unavailable.

Imports:
- fastapi.APIRouter, fastapi.Response: For routing and response handling.
- os: For reading environment variables.
- asyncio: For asynchronous operations and timeouts.
- db: Local database utility module.
"""

import asyncio
import os

from fastapi import APIRouter, Response

from . import db

router = APIRouter()


@router.get("/health")
async def health():
    return {"status": "ok"}


@router.get("/ready")
async def ready(response: Response):
    """
    This module defines the health check endpoint for the FastAPI application.

    It provides a `/ready` route that verifies the application's readiness by checking
    the database connection. The readiness check uses configurable environment variables
    for retries, delay, and timeout to control the behavior of the health probe.
    If the database connection cannot be established within the specified timeout,
    the endpoint responds with a 503 status code indicating the service is unavailable.
    """
    # Read readiness-related env vars (with sensible defaults)
    retries = int(os.getenv("EXAMPLE_FASTAPI_READY_RETRIES", "3"))
    delay = float(os.getenv("EXAMPLE_FASTAPI_READY_DELAY", "0.1"))
    timeout = float(os.getenv("EXAMPLE_FASTAPI_READY_TIMEOUT", "1.0"))

    # Execute the synchronous check in the DBManager's threadpool and bound
    # the operation with an asyncio timeout to avoid long blocking.
    try:
        ok = await asyncio.wait_for(db.db.run(db.db.check_connection, retries, delay), timeout=timeout)
    except asyncio.TimeoutError:
        ok = False

    if not ok:
        response.status_code = 503
        return {"status": "unavailable"}
    return {"status": "ready"}
