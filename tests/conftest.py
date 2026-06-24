import uuid
from datetime import UTC, datetime, timedelta

import pytest
from jose import jwt

GATEWAY_URL = "http://localhost:8080"
JWT_SECRET = "nexuschat-dev-secret-change-in-production"
JWT_ALGORITHM = "HS256"


@pytest.fixture
async def async_client():
    import httpx
    async with httpx.AsyncClient(base_url=GATEWAY_URL, timeout=30) as client:
        yield client


@pytest.fixture
async def token(async_client):
    username = f"testuser_{uuid.uuid4().hex[:8]}"
    password = "testpass123"
    resp = await async_client.post("/register", json={"username": username, "password": password})
    assert resp.status_code == 200
    data = resp.json()
    return data["access_token"]


@pytest.fixture
async def refresh_token(async_client):
    username = f"testuser_{uuid.uuid4().hex[:8]}"
    password = "testpass123"
    resp = await async_client.post("/register", json={"username": username, "password": password})
    assert resp.status_code == 200
    access_token = resp.json()["access_token"]
    payload = jwt.decode(access_token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    user_id = payload["sub"]
    expire = datetime.now(UTC) + timedelta(days=7)
    refresh = jwt.encode(
        {"sub": user_id, "exp": expire, "type": "refresh"},
        JWT_SECRET,
        algorithm=JWT_ALGORITHM,
    )
    return refresh


@pytest.fixture
async def chat_id(async_client, token):
    resp = await async_client.post(
        "/chats",
        json={"name": f"test_chat_{uuid.uuid4().hex[:6]}"},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 200
    return resp.json()["id"]
