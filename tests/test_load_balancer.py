import httpx
import pytest

LB_URL = "http://localhost:8000"


@pytest.mark.asyncio
@pytest.mark.timeout(30)
async def test_lb_health():
    async with httpx.AsyncClient(base_url=LB_URL, timeout=10) as client:
        resp = await client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert "status" in data
        assert "active_strategy" in data


@pytest.mark.asyncio
@pytest.mark.timeout(30)
async def test_lb_strategies():
    async with httpx.AsyncClient(base_url=LB_URL, timeout=10) as client:
        resp = await client.get("/strategies")
        assert resp.status_code == 200
        data = resp.json()
        assert "active" in data
        assert "available" in data
        assert isinstance(data["available"], dict)


@pytest.mark.asyncio
@pytest.mark.timeout(30)
async def test_lb_circuit_breakers():
    async with httpx.AsyncClient(base_url=LB_URL, timeout=10) as client:
        resp = await client.get("/circuit-breakers")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, dict)
