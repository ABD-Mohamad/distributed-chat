import uuid

import pytest


@pytest.mark.asyncio
@pytest.mark.timeout(30)
async def test_register_success(async_client):
    username = f"reg_{uuid.uuid4().hex[:8]}"
    resp = await async_client.post("/register", json={"username": username, "password": "testpass123"})
    assert resp.status_code == 200
    data = resp.json()
    assert "access_token" in data
    assert data["token_type"] == "bearer"


@pytest.mark.asyncio
@pytest.mark.timeout(30)
async def test_register_duplicate(async_client):
    username = f"dup_{uuid.uuid4().hex[:8]}"
    await async_client.post("/register", json={"username": username, "password": "testpass123"})
    resp = await async_client.post("/register", json={"username": username, "password": "testpass123"})
    assert resp.status_code == 409


@pytest.mark.asyncio
@pytest.mark.timeout(30)
async def test_register_invalid_username(async_client):
    resp = await async_client.post("/register", json={"username": "", "password": "testpass123"})
    assert resp.status_code == 422


@pytest.mark.asyncio
@pytest.mark.timeout(30)
async def test_login_success(async_client):
    username = f"login_{uuid.uuid4().hex[:8]}"
    await async_client.post("/register", json={"username": username, "password": "testpass123"})
    resp = await async_client.post("/login", json={"username": username, "password": "testpass123"})
    assert resp.status_code == 200
    data = resp.json()
    assert "access_token" in data
    assert data["token_type"] == "bearer"


@pytest.mark.asyncio
@pytest.mark.timeout(30)
async def test_login_wrong_password(async_client):
    username = f"wrong_{uuid.uuid4().hex[:8]}"
    await async_client.post("/register", json={"username": username, "password": "testpass123"})
    resp = await async_client.post("/login", json={"username": username, "password": "wrongpassword"})
    assert resp.status_code == 401


@pytest.mark.asyncio
@pytest.mark.timeout(30)
async def test_refresh_success(async_client, refresh_token):
    resp = await async_client.post("/refresh", json={"refresh_token": refresh_token})
    assert resp.status_code == 200
    data = resp.json()
    assert "access_token" in data
    assert "refresh_token" in data
    assert data["token_type"] == "bearer"


@pytest.mark.asyncio
@pytest.mark.timeout(30)
async def test_refresh_invalid(async_client):
    resp = await async_client.post("/refresh", json={"refresh_token": "garbage_token_here"})
    assert resp.status_code == 401


@pytest.mark.asyncio
@pytest.mark.timeout(30)
async def test_logout(async_client, token):
    logout_resp = await async_client.post("/logout", headers={"Authorization": f"Bearer {token}"})
    assert logout_resp.status_code == 200
    chats_resp = await async_client.get("/chats", headers={"Authorization": f"Bearer {token}"})
    assert chats_resp.status_code == 401
