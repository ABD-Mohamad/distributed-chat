import asyncio
import json

import pytest
import websockets

GATEWAY_WS_URL = "ws://localhost:8080"


@pytest.mark.asyncio
@pytest.mark.timeout(30)
async def test_ws_connect(async_client, token, chat_id):
    ws_url = f"{GATEWAY_WS_URL}/ws/{chat_id}?token={token}"
    async with websockets.connect(ws_url) as ws:
        assert ws.close_code is None


@pytest.mark.asyncio
@pytest.mark.timeout(30)
async def test_ws_send_message(async_client, token, chat_id):
    ws_url = f"{GATEWAY_WS_URL}/ws/{chat_id}?token={token}"
    async with websockets.connect(ws_url) as ws:
        await ws.send(json.dumps({"body": "Hello from test"}))
        response = await asyncio.wait_for(ws.recv(), timeout=10)
        data = json.loads(response)
        assert "id" in data
        assert data["chat_id"] == chat_id
        assert "sender_username" in data
        assert data["body"] == "Hello from test"
        assert "sent_at" in data


@pytest.mark.asyncio
@pytest.mark.timeout(30)
async def test_ws_invalid_token(async_client, chat_id):
    ws_url = f"{GATEWAY_WS_URL}/ws/{chat_id}?token=invalid_token_here"
    try:
        async with websockets.connect(ws_url) as ws:
            await ws.recv()
            pytest.fail("Connection should have been closed")
    except websockets.exceptions.ConnectionClosed as e:
        assert e.code == 4001
    except websockets.exceptions.InvalidHandshake:
        pass
