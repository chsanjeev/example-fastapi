"""
This module contains tests for the FastAPI application's readiness endpoint.

It provides asynchronous utilities and test cases to simulate HTTP requests to the '/ready' endpoint,
verifying that the service is available and responds with the expected status and JSON payload.
"""

import asyncio

from httpx import AsyncClient
from httpx._transports.asgi import ASGITransport

from app.main import app


async def _get_ready():
    """
    This module contains tests for the application's readiness endpoint.

    It provides asynchronous utilities to simulate HTTP requests to the FastAPI app,
    specifically targeting the '/ready' endpoint to verify service availability.
    """
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        r = await ac.get("/ready")
        return r


def test_ready_endpoint():
    """
    Module for testing the /ready endpoint of the FastAPI application.

    This module contains tests to verify that the /ready endpoint returns a 200 status code
    and a JSON response indicating the application's readiness.
    """
    r = asyncio.run(_get_ready())
    assert r.status_code == 200
    assert r.json().get("status") == "ready"
