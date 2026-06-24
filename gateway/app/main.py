import asyncio
import logging
import time
from pathlib import Path

import websockets

START_TIME = time.time()
from fastapi import FastAPI, Header, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .auth import create_refresh_token, create_token, decode_refresh_token, decode_token
from .config import settings
from .http_client import (
    create_chat as lb_create_chat,
)
from .http_client import (
    get_history as lb_get_history,
)
from .http_client import (
    join_chat as lb_join_chat,
)
from .http_client import (
    list_chats as lb_list_chats,
)
from .http_client import (
    login as lb_login,
)
from .http_client import (
    register as lb_register,
)
from .ratelimit import blacklist_token, check_rate_limit, is_blacklisted
from .telemetry import setup_telemetry

logger = logging.getLogger(__name__)

app = FastAPI(title="NexusChat Gateway")

setup_telemetry(app=app)

app.add_middleware(CORSMiddleware, allow_origins=settings.cors_origins.split(","), allow_credentials=True, allow_methods=["*"], allow_headers=["*"])
app.add_middleware(GZipMiddleware, minimum_size=1000)

STATIC_DIR = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


class RegisterRequest(BaseModel):
    username: str = Field(..., min_length=3, max_length=50)
    password: str = Field(..., min_length=6, max_length=128)


class LoginRequest(BaseModel):
    username: str = Field(..., min_length=3, max_length=50)
    password: str = Field(..., min_length=6, max_length=128)


class CreateChatRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)


class SendMessageRequest(BaseModel):
    body: str = Field(..., min_length=1, max_length=1000)


class RefreshRequest(BaseModel):
    refresh_token: str


LB_WS_URL = settings.lb_ws_url


@app.get("/health")
async def health():
    return {"status": "ok", "service": "gateway", "version": "1.0.0", "uptime": round(time.time() - START_TIME, 2)}


@app.get("/ready")
async def ready():
    return {"status": "ok", "service": "gateway"}


@app.get("/live")
async def live():
    return {"status": "ok", "service": "gateway"}


@app.post("/register")
async def register(req: RegisterRequest):
    if not await check_rate_limit(f"register:{req.username}", 5, 60):
        raise HTTPException(status_code=429, detail="Rate limit exceeded")
    result = await lb_register(req.username, req.password)
    if result is None:
        raise HTTPException(status_code=409, detail="Registration failed")
    return result


@app.post("/login")
async def login(req: LoginRequest):
    if not await check_rate_limit(f"login:{req.username}", 10, 60):
        raise HTTPException(status_code=429, detail="Rate limit exceeded")
    result = await lb_login(req.username, req.password)
    if result is None:
        raise HTTPException(status_code=401, detail="Login failed")
    return result


@app.post("/refresh")
async def refresh(req: RefreshRequest):
    if not await check_rate_limit(f"refresh:{req.refresh_token[:16]}", 10, 60):
        raise HTTPException(status_code=429, detail="Rate limit exceeded")
    payload = decode_refresh_token(req.refresh_token)
    if payload is None:
        raise HTTPException(status_code=401, detail="Invalid or expired refresh token")
    user_id = payload["sub"]
    new_access_token = create_token(user_id)
    new_refresh_token = create_refresh_token(user_id)
    return {"access_token": new_access_token, "refresh_token": new_refresh_token, "token_type": "bearer"}


@app.post("/logout")
async def logout(authorization: str = Header(...)):
    token = authorization.replace("Bearer ", "")
    if not token:
        raise HTTPException(status_code=401, detail="Missing token")
    payload = decode_token(token)
    if payload is None:
        raise HTTPException(status_code=401, detail="Invalid or expired token")
    exp = payload.get("exp", 0)
    import time as _time
    remaining = max(1, int(exp - _time.time()))
    await blacklist_token(token, remaining)
    return {"detail": "Logged out"}


@app.post("/chats")
async def create_chat(req: CreateChatRequest, authorization: str = Header(...)):
    token = authorization.replace("Bearer ", "")
    if await is_blacklisted(token):
        raise HTTPException(status_code=401, detail="Token is blacklisted")
    if not await check_rate_limit(f"chats:{token}", 30, 60):
        raise HTTPException(status_code=429, detail="Rate limit exceeded")
    result = await lb_create_chat(token, req.name)
    if result is None:
        raise HTTPException(status_code=502, detail="Upstream error")
    return result


@app.get("/chats")
async def list_chats(authorization: str = Header(...)):
    token = authorization.replace("Bearer ", "")
    if await is_blacklisted(token):
        raise HTTPException(status_code=401, detail="Token is blacklisted")
    if not await check_rate_limit(f"chats:{token}", 30, 60):
        raise HTTPException(status_code=429, detail="Rate limit exceeded")
    result = await lb_list_chats(token)
    if result is None:
        raise HTTPException(status_code=502, detail="Upstream error")
    return result


@app.post("/chats/{chat_id}/join")
async def join_chat(chat_id: str, authorization: str = Header(...)):
    token = authorization.replace("Bearer ", "")
    if await is_blacklisted(token):
        raise HTTPException(status_code=401, detail="Token is blacklisted")
    if not await check_rate_limit(f"chats:{token}", 30, 60):
        raise HTTPException(status_code=429, detail="Rate limit exceeded")
    result = await lb_join_chat(token, chat_id)
    if result is None:
        raise HTTPException(status_code=502, detail="Upstream error")
    return result


@app.get("/chats/{chat_id}/messages")
async def get_messages(chat_id: str, limit: int = 50, authorization: str = Header(...)):
    token = authorization.replace("Bearer ", "")
    if await is_blacklisted(token):
        raise HTTPException(status_code=401, detail="Token is blacklisted")
    if not await check_rate_limit(f"chats:{token}", 30, 60):
        raise HTTPException(status_code=429, detail="Rate limit exceeded")
    result = await lb_get_history(token, chat_id, limit)
    if result is None:
        raise HTTPException(status_code=502, detail="Upstream error")
    return result


WS_RECONNECT_DELAY = 1
WS_MAX_RECONNECT = 5


@app.websocket("/ws/{chat_id}")
async def websocket_proxy(websocket: WebSocket, chat_id: str, token: str = Query(...)):
    payload = decode_token(token)
    if payload is None:
        await websocket.close(code=4001)
        return
    if await is_blacklisted(token):
        await websocket.close(code=4001)
        return
    await websocket.accept()
    lb_ws = None
    reconnect_attempts = 0
    try:
        while reconnect_attempts < WS_MAX_RECONNECT:
            try:
                lb_ws = await websockets.connect(f"{LB_WS_URL}/ws/{chat_id}?token={token}")
                reconnect_attempts = 0
                async def upstream():
                    async for msg in lb_ws:
                        await websocket.send_text(msg)
                async def downstream():
                    while True:
                        raw = await websocket.receive_text()
                        await lb_ws.send(raw)
                tasks = [asyncio.create_task(upstream()), asyncio.create_task(downstream())]
                done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                for t in pending:
                    t.cancel()
                break
            except (websockets.ConnectionClosed, OSError):
                reconnect_attempts += 1
                if reconnect_attempts >= WS_MAX_RECONNECT:
                    break
                await asyncio.sleep(WS_RECONNECT_DELAY)
    except WebSocketDisconnect:
        pass
    finally:
        if lb_ws:
            await lb_ws.close()


@app.get("/", response_class=HTMLResponse)
async def index():
    return FileResponse(str(STATIC_DIR / "index.html"))


@app.get("/chat", response_class=HTMLResponse)
async def chat_page():
    return FileResponse(str(STATIC_DIR / "chat.html"))


@app.get("/admin", response_class=HTMLResponse)
async def admin_page():
    return FileResponse(str(STATIC_DIR / "admin.html"))


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard_page():
    return FileResponse(str(STATIC_DIR / "dashboard.html"))
