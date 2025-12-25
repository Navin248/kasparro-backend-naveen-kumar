import pytest
from httpx import AsyncClient, ASGITransport
from api.main import app
from core import db


@pytest.mark.asyncio
async def test_health_failure(monkeypatch):

    def broken_execute(*args, **kwargs):
        raise Exception("DB Broken")

    # Break DB query execution
    monkeypatch.setattr(db, "get_db", lambda: broken_execute)

    transport = ASGITransport(app=app)

    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        response = await ac.get("/health")

    assert response.status_code == 200
    json_data = response.json()
    assert json_data["status"] == "unhealthy"
