import asyncio
import json
import logging
import time
import uuid
from contextlib import asynccontextmanager

START_TIME = time.time()

import aio_pika
import chat_pb2_grpc
import grpc
from fastapi import Depends, FastAPI, Header, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from sqlalchemy import select, text

from .config import settings
from .telemetry import setup_telemetry

logger = logging.getLogger(__name__)
from datetime import UTC

from .auth import decode_token
from .cache import (
    close_cache,
    get_cached_history,
    init_cache,
    invalidate_history_cache,
    redis_client,
    set_cached_history,
)
from .database import auth_async_session
from .event_producer import close_kafka, init_kafka, kafka_producer, publish_event
from .grpc_server import ChatServicer
from .models import Chat, ChatMember, Message
from .rabbitmq import _connection as _rmq_connection
from .rabbitmq import close_rabbitmq, init_rabbitmq, start_consumer
from .sharding import _shards, dispose_shards, get_primary_session, get_replica_session, init_shards
from .ws_manager import manager


def get_user_id(authorization: str = Header(...)) -> str:
    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Invalid authorization header")
    payload = decode_token(authorization[7:])
    if payload is None:
        raise HTTPException(status_code=401, detail="Invalid or expired token")
    return payload["sub"]


async def get_username(user_id: str) -> str:
    async with auth_async_session() as db:
        result = await db.execute(text("SELECT username FROM users WHERE id = :uid"), {"uid": user_id})
        row = result.scalar_one_or_none()
        return row if row else "unknown"


async def _on_rmq_message(message: aio_pika.IncomingMessage):
    async with message.process():
        try:
            body = json.loads(message.body)
            chat_id = body.get("chat_id")
            payload = body.get("payload")
            if chat_id and payload:
                logger.info(f"RMQ received for chat {chat_id}")
                await manager.broadcast(chat_id, payload)
            else:
                logger.warning(f"RMQ invalid payload: {body}")
        except Exception as e:
            logger.warning(f"RMQ callback error: {e}")


@asynccontextmanager
async def lifespan(_app: FastAPI):
    init_shards()
    await init_cache()
    await init_kafka()
    await init_rabbitmq(consumer_callback=_on_rmq_message)
    grpc_server = grpc.aio.server()
    servicer = ChatServicer()
    chat_pb2_grpc.add_ChatServiceServicer_to_server(servicer, grpc_server)
    grpc_server.add_insecure_port(f"0.0.0.0:{settings.grpc_port}")
    await grpc_server.start()
    asyncio.ensure_future(start_consumer())
    yield
    await grpc_server.stop(5)
    await close_rabbitmq()
    await close_kafka()
    await close_cache()
    await dispose_shards()


app = FastAPI(title="NexusChat Service", lifespan=lifespan)

setup_telemetry(app=app)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins.split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class AuthRequest(BaseModel):
    username: str = Field(..., min_length=3, max_length=50)
    password: str = Field(..., min_length=6, max_length=128)


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"


class CreateChatRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)


class SendMessageRequest(BaseModel):
    body: str = Field(..., min_length=1, max_length=1000)


@app.get("/health")
async def health():
    return {"status": "ok", "service": "chat-service", "version": "1.0.0", "server_id": settings.grpc_port, "uptime": round(time.time() - START_TIME, 2)}


@app.get("/ready")
async def ready():
    from sqlalchemy import text
    if redis_client:
        try:
            await redis_client.ping()
        except Exception:
            raise HTTPException(status_code=503, detail="Redis not ready")
    if not kafka_producer:
        raise HTTPException(status_code=503, detail="Kafka not ready")
    if not _rmq_connection or _rmq_connection.is_closed:
        raise HTTPException(status_code=503, detail="RabbitMQ not ready")
    for i, shard in enumerate(_shards):
        try:
            async with shard.primary_session() as sess:
                await sess.execute(text("SELECT 1"))
        except Exception:
            raise HTTPException(status_code=503, detail=f"Shard {i} not ready")
    try:
        async with auth_async_session() as sess:
            await sess.execute(text("SELECT 1"))
    except Exception:
        raise HTTPException(status_code=503, detail="Auth DB not ready")
    return {"status": "ok", "service": "chat-service"}


@app.get("/live")
async def live():
    return {"status": "ok", "service": "chat-service"}


@app.post("/register", response_model=TokenResponse)
async def register(req: AuthRequest):
    import uuid as _uuid

    from .auth import create_token, hash_password
    async with auth_async_session() as db:
        result = await db.execute(text("SELECT id FROM users WHERE username = :un"), {"un": req.username})
        if result.scalar_one_or_none():
            raise HTTPException(status_code=409, detail="Username already taken")
        uid = str(_uuid.uuid4())
        await db.execute(
            text("INSERT INTO users (id, username, password_hash) VALUES (:id, :un, :ph)"),
            {"id": uid, "un": req.username, "ph": hash_password(req.password)},
        )
        await db.commit()
        return TokenResponse(access_token=create_token(uid))


@app.post("/login", response_model=TokenResponse)
async def login(req: AuthRequest):
    from .auth import create_token, verify_password
    async with auth_async_session() as db:
        result = await db.execute(text("SELECT id, password_hash FROM users WHERE username = :un"), {"un": req.username})
        row = result.fetchone()
        if not row or not verify_password(req.password, row[1]):
            raise HTTPException(status_code=401, detail="Invalid credentials")
        return TokenResponse(access_token=create_token(str(row[0])))


@app.post("/chats")
async def create_chat(req: CreateChatRequest, user_id: str = Depends(get_user_id)):
    async with get_primary_session(f"new:{uuid.uuid4()}") as db:
        chat_id = str(uuid.uuid4())
        db.add(Chat(id=uuid.UUID(chat_id), name=req.name))
        db.add(ChatMember(chat_id=uuid.UUID(chat_id), user_id=uuid.UUID(user_id)))
        await db.commit()
    asyncio.ensure_future(publish_event("chat.created", {"chat_id": chat_id, "name": req.name, "creator_id": user_id}))
    return {"id": chat_id, "name": req.name}


@app.get("/chats")
async def list_chats(user_id: str = Depends(get_user_id)):
    all_chats = []
    for i in range(2):
        async with get_primary_session(f"all:{i}") as db:
            result = await db.execute(
                select(Chat).join(ChatMember, ChatMember.chat_id == Chat.id)
                .where(ChatMember.user_id == uuid.UUID(user_id))
                .order_by(Chat.created_at.desc())
            )
            for c in result.scalars().all():
                all_chats.append({"id": str(c.id), "name": c.name, "created_at": c.created_at.isoformat()})
    return all_chats


@app.post("/chats/{chat_id}/join")
async def join_chat(chat_id: str, user_id: str = Depends(get_user_id)):
    async with get_primary_session(chat_id) as db:
        result = await db.execute(select(Chat).where(Chat.id == uuid.UUID(chat_id)))
        if not result.scalar_one_or_none():
            raise HTTPException(status_code=404, detail="Chat not found")
        result = await db.execute(
            select(ChatMember).where(
                ChatMember.chat_id == uuid.UUID(chat_id),
                ChatMember.user_id == uuid.UUID(user_id),
            )
        )
        if result.scalar_one_or_none():
            return {"detail": "Already a member"}
        db.add(ChatMember(chat_id=uuid.UUID(chat_id), user_id=uuid.UUID(user_id)))
        await db.commit()
    asyncio.ensure_future(publish_event("chat.joined", {"chat_id": chat_id, "user_id": user_id}))
    return {"detail": "Joined chat"}


@app.get("/chats/{chat_id}/messages")
async def get_messages(chat_id: str, limit: int = 50, user_id: str = Depends(get_user_id)):
    cached = await get_cached_history(chat_id)
    if cached is not None:
        return cached
    async with get_replica_session(chat_id) as db:
        result = await db.execute(
            select(ChatMember).where(
                ChatMember.chat_id == uuid.UUID(chat_id),
                ChatMember.user_id == uuid.UUID(user_id),
            )
        )
        if not result.scalar_one_or_none():
            raise HTTPException(status_code=403, detail="Not a member")
        result = await db.execute(
            select(Message).where(Message.chat_id == uuid.UUID(chat_id))
            .order_by(Message.sent_at.desc())
            .limit(limit)
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
    async with auth_async_session() as db:
        for msg in msgs:
            result = await db.execute(text("SELECT username FROM users WHERE id = :uid"), {"uid": msg["sender_id"]})
            username = result.scalar_one_or_none()
            msg["sender_username"] = username if username else "unknown"
    await set_cached_history(chat_id, msgs)
    return msgs


@app.websocket("/ws/{chat_id}")
async def websocket_endpoint(websocket: WebSocket, chat_id: str, token: str = Query(...)):
    payload = decode_token(token)
    if payload is None:
        await websocket.close(code=4001)
        return
    user_id = payload["sub"]
    await manager.connect(chat_id, websocket)
    try:
        while True:
            raw = await websocket.receive_text()
            data = json.loads(raw)
            body = data.get("body", "")
            if not body.strip():
                continue
            async with get_primary_session(chat_id) as db:
                username = await get_username(user_id)
                msg_id = str(uuid.uuid4())
                db.add(Message(id=uuid.UUID(msg_id), chat_id=uuid.UUID(chat_id), sender_id=uuid.UUID(user_id), body=body))
                await db.commit()
                from datetime import datetime
                payload_out = {
                    "id": msg_id,
                    "chat_id": chat_id,
                    "sender_id": user_id,
                    "sender_username": username,
                    "body": body,
                    "sent_at": datetime.now(UTC).isoformat(),
                }
            await manager.broadcast_with_rmq(chat_id, payload_out)
            asyncio.ensure_future(publish_event("message.sent", payload_out))
            await invalidate_history_cache(chat_id)
    except (WebSocketDisconnect, Exception):
        manager.disconnect(chat_id, websocket)
