import uuid

import httpx
import pytest

EVENT_SERVICE_URL = "http://localhost:8001"


@pytest.mark.asyncio
@pytest.mark.timeout(30)
async def test_event_count(async_client, token):
    chat_name = f"evt_{uuid.uuid4().hex[:6]}"
    await async_client.post(
        "/chats",
        json={"name": chat_name},
        headers={"Authorization": f"Bearer {token}"},
    )
    try:
        async with httpx.AsyncClient(base_url=EVENT_SERVICE_URL, timeout=10) as client:
            resp = await client.get("/events/count")
            assert resp.status_code == 200
            data = resp.json()
            assert data["total"] > 0
    except httpx.ConnectError:
        pytest.skip("Event service not reachable — expose event-service on port 8001")


@pytest.mark.asyncio
@pytest.mark.timeout(30)
async def test_events_list(async_client, token):
    try:
        async with httpx.AsyncClient(base_url=EVENT_SERVICE_URL, timeout=10) as client:
            resp = await client.get("/events")
            assert resp.status_code == 200
            events = resp.json()
            assert isinstance(events, list)
    except httpx.ConnectError:
        pytest.skip("Event service not reachable — expose event-service on port 8001")
