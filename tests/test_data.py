import pytest
from httpx import AsyncClient
from httpx import ASGITransport

from api.main import app

@pytest.mark.asyncio
async def test_data_endpoint():
    transport = ASGITransport(app=app)

    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        response = await ac.get("/data?page=1&limit=5")


    assert response.status_code == 200
    
    body = response.json()
    
    assert "data" in body
    assert "total_records" in body
    assert isinstance(body["data"], list)
