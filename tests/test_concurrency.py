import asyncio

import pytest
from httpx import ASGITransport, AsyncClient

from app.main import app


@pytest.mark.asyncio
async def test_100_concurrent_creates_and_reads(tmp_path):
    """Create 100 items concurrently, then read them concurrently."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:

        async def create(i):
            resp = await client.post("/items/", json={"name": f"item-{i}", "value": str(i)})
            assert resp.status_code == 201
            return resp.json()["id"]

        # create 100 items concurrently
        create_tasks = [asyncio.create_task(create(i)) for i in range(100)]
        ids = await asyncio.gather(*create_tasks)
        assert len(ids) == 100

        # now concurrently fetch them
        async def fetch(item_id):
            resp = await client.get(f"/items/{item_id}")
            assert resp.status_code == 200
            data = resp.json()
            assert data["id"] == item_id
            return data

        fetch_tasks = [asyncio.create_task(fetch(i)) for i in ids]
        results = await asyncio.gather(*fetch_tasks)
        assert len(results) == 100
