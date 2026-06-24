import json
import uuid
import asyncio
import logging

import grpc
from sqlalchemy import select

import chat_pb2
import chat_pb2_grpc
from .models import Chat, ChatMember, Message
from .sharding import get_primary_session, get_replica_session, get_all_primary_sessions
from .ws_manager import manager
from .cache import invalidate_history_cache

logger = logging.getLogger(__name__)


class ChatServicer(chat_pb2_grpc.ChatServiceServicer):
    async def CreateChat(self, request, context):
        async with get_primary_session(f"new:{uuid.uuid4()}") as db:
            chat_id = str(uuid.uuid4())
            db.add(Chat(id=uuid.UUID(chat_id), name=request.name))
            db.add(ChatMember(chat_id=uuid.UUID(chat_id), user_id=uuid.UUID(request.user_id)))
            await db.commit()
            return chat_pb2.CreateChatResponse(chat_id=chat_id, name=request.name)

    async def ListChats(self, request, context):
        all_chats = []
        for session_factory in get_all_primary_sessions():
            async with session_factory as db:
                result = await db.execute(
                    select(Chat).join(ChatMember, ChatMember.chat_id == Chat.id)
                    .where(ChatMember.user_id == uuid.UUID(request.user_id))
                    .order_by(Chat.created_at.desc())
                )
                for c in result.scalars().all():
                    all_chats.append(chat_pb2.Chat(id=str(c.id), name=c.name, created_at=c.created_at.isoformat()))
        return chat_pb2.ListChatsResponse(chats=all_chats)

    async def JoinChat(self, request, context):
        async with get_primary_session(request.chat_id) as db:
            result = await db.execute(select(Chat).where(Chat.id == uuid.UUID(request.chat_id)))
            if not result.scalar_one_or_none():
                await context.abort(grpc.StatusCode.NOT_FOUND, "Chat not found")
            result = await db.execute(
                select(ChatMember).where(
                    ChatMember.chat_id == uuid.UUID(request.chat_id),
                    ChatMember.user_id == uuid.UUID(request.user_id),
                )
            )
            if result.scalar_one_or_none():
                return chat_pb2.JoinChatResponse(detail="Already a member")
            db.add(ChatMember(chat_id=uuid.UUID(request.chat_id), user_id=uuid.UUID(request.user_id)))
            await db.commit()
            return chat_pb2.JoinChatResponse(detail="Joined chat")

    async def SendMessage(self, request, context):
        async with get_primary_session(request.chat_id) as db:
            msg_id = str(uuid.uuid4())
            db.add(Message(
                id=uuid.UUID(msg_id),
                chat_id=uuid.UUID(request.chat_id),
                sender_id=uuid.UUID(request.user_id),
                body=request.body,
            ))
            await db.commit()
            await invalidate_history_cache(request.chat_id)
        from datetime import datetime, timezone
        import asyncio
        from .event_producer import publish_event
        payload = {
            "id": msg_id,
            "chat_id": request.chat_id,
            "sender_id": request.user_id,
            "sender_username": request.sender_username,
            "body": request.body,
            "sent_at": datetime.now(timezone.utc).isoformat(),
        }
        await manager.broadcast_with_rmq(request.chat_id, payload)
        asyncio.ensure_future(publish_event("message.sent", payload))
        return chat_pb2.SendMessageResponse(message_id=msg_id)

    async def GetHistory(self, request, context):
        from .cache import get_cached_history, set_cached_history
        cached = await get_cached_history(request.chat_id)
        if cached is not None:
            for msg in cached:
                yield chat_pb2.Message(
                    id=msg["id"],
                    chat_id=msg["chat_id"],
                    sender_id=msg["sender_id"],
                    sender_username=msg.get("sender_username", ""),
                    body=msg["body"],
                    sent_at=msg["sent_at"],
                )
            return
        async with get_replica_session(request.chat_id) as db:
            result = await db.execute(
                select(Message).where(Message.chat_id == uuid.UUID(request.chat_id))
                .order_by(Message.sent_at.desc())
                .limit(request.limit or 50)
            )
            msgs = [
                {
                    "id": str(m.id),
                    "chat_id": str(m.chat_id),
                    "sender_id": str(m.sender_id),
                    "body": m.body,
                    "sent_at": m.sent_at.isoformat(),
                }
                for m in reversed(result.scalars().all())
            ]
        await set_cached_history(request.chat_id, msgs)
        for msg in msgs:
            yield chat_pb2.Message(
                id=msg["id"],
                chat_id=msg["chat_id"],
                sender_id=msg["sender_id"],
                body=msg["body"],
                sent_at=msg["sent_at"],
            )

    async def SubscribeMessages(self, request, context):
        q: asyncio.Queue[dict] = asyncio.Queue()
        async def _push(msg: dict):
            await q.put(msg)
        manager._rooms.setdefault(request.chat_id, set()).add(q)
        try:
            while True:
                msg = await q.get()
                yield chat_pb2.Message(
                    id=msg.get("id", ""),
                    chat_id=request.chat_id,
                    sender_id=msg.get("sender_id", ""),
                    sender_username=msg.get("sender_username", ""),
                    body=msg.get("body", ""),
                    sent_at=msg.get("sent_at", ""),
                )
        except (asyncio.CancelledError, Exception):
            manager._rooms.get(request.chat_id, set()).discard(q)
