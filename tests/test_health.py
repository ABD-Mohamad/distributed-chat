import httpx
import pytest

GATEWAY_URL = "http://localhost:8080"
LB_URL = "http://localhost:8000"


@pytest.mark.asyncio
@pytest.mark.timeout(30)
async def test_all_health_endpoints():
    results = {}
    async with httpx.AsyncClient(timeout=10) as client:
        gateway_resp = await client.get(f"{GATEWAY_URL}/health")
        results["gateway"] = gateway_resp
        lb_resp = await client.get(f"{LB_URL}/health")
        results["load_balancer"] = lb_resp
    assert results["gateway"].status_code == 200
    assert results["gateway"].json() == {"status": "ok"}
    assert results["load_balancer"].status_code == 200
    lb_data = results["load_balancer"].json()
    assert "status" in lb_data
    assert "active_strategy" in lb_data
