import asyncio
import json
import logging

from fastapi import WebSocket

logger = logging.getLogger(__name__)


class ConnectionManager:
    def __init__(self):
        self._rooms: dict[str, set[WebSocket | asyncio.Queue]] = {}

    async def connect(self, chat_id: str, ws: WebSocket):
        await ws.accept()
        self._rooms.setdefault(chat_id, set()).add(ws)
        logger.info(f"WS connected to chat {chat_id}: {len(self._rooms[chat_id])} total in room")

    def disconnect(self, chat_id: str, ws: WebSocket):
        room = self._rooms.get(chat_id, set())
        room.discard(ws)
        if not room:
            self._rooms.pop(chat_id, None)
        logger.info(f"WS disconnected from chat {chat_id}: {len(room)} remaining")

    async def broadcast(self, chat_id: str, payload: dict):
        room = self._rooms.get(chat_id, set())
        logger.info(f"Broadcasting to chat {chat_id}: {len(room)} connections")
        dead = set()
        for sub in room:
            try:
                if isinstance(sub, WebSocket):
                    await sub.send_text(json.dumps(payload))
                else:
                    await sub.put(payload)
            except Exception:
                dead.add(sub)
        for sub in dead:
            if isinstance(sub, WebSocket):
                self.disconnect(chat_id, sub)
            else:
                self._rooms.get(chat_id, set()).discard(sub)

    async def broadcast_with_rmq(self, chat_id: str, payload: dict):
        await self.broadcast(chat_id, payload)
        from .rabbitmq import publish_to_fanout
        asyncio.ensure_future(publish_to_fanout({"chat_id": chat_id, "payload": payload}))


manager = ConnectionManager()
