import asyncio
import logging

import websockets as ws_lib
from fastapi import WebSocket, WebSocketDisconnect

logger = logging.getLogger(__name__)


async def ws_proxy_handler(websocket: WebSocket, backend_url: str, chat_id: str, token: str):
    target = f"{backend_url.rstrip('/')}/ws/{chat_id}?token={token}"
    await websocket.accept()
    try:
        async with ws_lib.connect(target) as downstream:
            async def client_to_server():
                try:
                    while True:
                        msg = await websocket.receive_text()
                        await downstream.send(msg)
                except WebSocketDisconnect:
                    pass

            async def server_to_client():
                try:
                    async for msg in downstream:
                        await websocket.send_text(msg)
                except WebSocketDisconnect:
                    pass

            await asyncio.gather(client_to_server(), server_to_client())
    except Exception as exc:
        logger.warning(f"WS proxy error for {chat_id}: {exc}")
        try:
            await websocket.close()
        except Exception:
            pass
