import uuid

import pytest


@pytest.mark.asyncio
@pytest.mark.timeout(30)
async def test_create_chat(async_client, token):
    resp = await async_client.post(
        "/chats",
        json={"name": f"chat_{uuid.uuid4().hex[:6]}"},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert "id" in data
    assert data["name"].startswith("chat_")


@pytest.mark.asyncio
@pytest.mark.timeout(30)
async def test_create_chat_no_auth(async_client):
    resp = await async_client.post("/chats", json={"name": "noauth_chat"})
    assert resp.status_code in (401, 422)


@pytest.mark.asyncio
@pytest.mark.timeout(30)
async def test_list_chats(async_client, token):
    chat_name = f"list_{uuid.uuid4().hex[:6]}"
    await async_client.post(
        "/chats",
        json={"name": chat_name},
        headers={"Authorization": f"Bearer {token}"},
    )
    resp = await async_client.get("/chats", headers={"Authorization": f"Bearer {token}"})
    assert resp.status_code == 200
    chats = resp.json()
    assert isinstance(chats, list)
    assert any(c["name"] == chat_name for c in chats)


@pytest.mark.asyncio
@pytest.mark.timeout(30)
async def test_join_chat(async_client, token, chat_id):
    username2 = f"user2_{uuid.uuid4().hex[:8]}"
    register_resp = await async_client.post("/register", json={"username": username2, "password": "pass2"})
    assert register_resp.status_code == 200
    token2 = register_resp.json()["access_token"]
    resp = await async_client.post(
        f"/chats/{chat_id}/join",
        headers={"Authorization": f"Bearer {token2}"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["detail"] == "Joined chat"


@pytest.mark.asyncio
@pytest.mark.timeout(30)
async def test_get_messages(async_client, token, chat_id):
    resp = await async_client.get(
        f"/chats/{chat_id}/messages",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 200
    messages = resp.json()
    assert isinstance(messages, list)
    assert len(messages) == 0
