import asyncio
import json
import logging

import websockets
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Query, Header, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from .auth import decode_token
from .ratelimit import RateLimiter
from .config import settings
from .telemetry import setup_telemetry
from .http_client import (
    register as lb_register,
    login as lb_login,
    create_chat as lb_create_chat,
    list_chats as lb_list_chats,
    join_chat as lb_join_chat,
    get_history as lb_get_history,
)

logger = logging.getLogger(__name__)

app = FastAPI(title="NexusChat Gateway")

setup_telemetry("gateway", app=app)

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

limiter = RateLimiter()


class RegisterRequest(BaseModel):
    username: str
    password: str


class LoginRequest(BaseModel):
    username: str
    password: str


class CreateChatRequest(BaseModel):
    name: str


class SendMessageRequest(BaseModel):
    body: str


LB_WS_URL = settings.lb_ws_url


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/register")
async def register(req: RegisterRequest):
    result = await lb_register(req.username, req.password)
    if result is None:
        raise HTTPException(status_code=409, detail="Registration failed")
    return result


@app.post("/login")
async def login(req: LoginRequest):
    result = await lb_login(req.username, req.password)
    if result is None:
        raise HTTPException(status_code=401, detail="Login failed")
    return result


@app.post("/chats")
async def create_chat(req: CreateChatRequest, authorization: str = Header(...)):
    token = authorization.replace("Bearer ", "")
    if not limiter.allow(token):
        raise HTTPException(status_code=429, detail="Rate limit exceeded")
    result = await lb_create_chat(token, req.name)
    if result is None:
        raise HTTPException(status_code=502, detail="Upstream error")
    return result


@app.get("/chats")
async def list_chats(authorization: str = Header(...)):
    token = authorization.replace("Bearer ", "")
    if not limiter.allow(token):
        raise HTTPException(status_code=429, detail="Rate limit exceeded")
    result = await lb_list_chats(token)
    if result is None:
        raise HTTPException(status_code=502, detail="Upstream error")
    return result


@app.post("/chats/{chat_id}/join")
async def join_chat(chat_id: str, authorization: str = Header(...)):
    token = authorization.replace("Bearer ", "")
    if not limiter.allow(token):
        raise HTTPException(status_code=429, detail="Rate limit exceeded")
    result = await lb_join_chat(token, chat_id)
    if result is None:
        raise HTTPException(status_code=502, detail="Upstream error")
    return result


@app.get("/chats/{chat_id}/messages")
async def get_messages(chat_id: str, limit: int = 50, authorization: str = Header(...)):
    token = authorization.replace("Bearer ", "")
    if not limiter.allow(token):
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
            except (websockets.ConnectionClosed, OSError) as e:
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
    return HTMLResponse(content=HTML_PAGE)


HTML_PAGE = """<!DOCTYPE html>
<html>
<head><title>NexusChat</title>
<style>
body { font-family: sans-serif; max-width: 800px; margin: auto; padding: 20px; }
#messages { height: 300px; overflow-y: scroll; border: 1px solid #ccc; padding: 10px; }
input, button { margin: 4px; }
</style></head>
<body>
<h1>NexusChat</h1>
<div id="auth">
  <input id="username" placeholder="Username"/>
  <input id="password" type="password" placeholder="Password"/>
  <button onclick="register()">Register</button>
  <button onclick="login()">Login</button>
</div>
<div id="chat" style="display:none">
  <div>
    <input id="chatName" placeholder="Chat name"/>
    <button onclick="createChat()">Create Chat</button>
  </div>
  <select id="chatList" size="5" style="width:100%" onchange="selectChat()"></select>
  <div id="messages"></div>
  <input id="msgInput" placeholder="Type a message"/>
  <button onclick="sendMsg()">Send</button>
</div>
<script>
let token = null; let currentChat = null; let ws = null;
async function api(method, path, body) {
  const opts = { method, headers: { 'Content-Type': 'application/json' } };
  if (token) opts.headers['Authorization'] = 'Bearer ' + token;
  if (body) opts.body = JSON.stringify(body);
  const r = await fetch(path, opts);
  return r.json();
}
async function register() { const r = await api('POST','/register',{username:u.value,password:p.value}); token = r.access_token; showChat(); }
async function login() { const r = await api('POST','/login',{username:u.value,password:p.value}); token = r.access_token; showChat(); }
async function showChat() {
  document.getElementById('auth').style.display='none';
  document.getElementById('chat').style.display='block';
  await loadChats();
}
async function loadChats() {
  const chats = await api('GET','/chats');
  const sel = document.getElementById('chatList'); sel.innerHTML = '';
  for (const c of chats) { const o = document.createElement('option'); o.value=c.id; o.text=c.name; sel.appendChild(o); }
}
async function createChat() {
  await api('POST','/chats',{name: document.getElementById('chatName').value});
  await loadChats();
}
async function selectChat() {
  currentChat = document.getElementById('chatList').value;
  const msgs = await api('GET','/chats/'+currentChat+'/messages');
  const div = document.getElementById('messages'); div.innerHTML = '';
  for (const m of msgs) div.innerHTML += '<b>'+m.sender_username+'</b>: '+m.body+'<br/>';
  if (ws) ws.close();
  ws = new WebSocket('ws://'+location.host+'/ws/'+currentChat+'?token='+token);
  ws.onmessage = (e) => {
    const m = JSON.parse(e.data);
    div.innerHTML += '<b>'+m.sender_username+'</b>: '+m.body+'<br/>';
  };
}
async function sendMsg() {
  const inp = document.getElementById('msgInput');
  ws.send(JSON.stringify({body: inp.value}));
  inp.value = '';
}
</script>
</body></html>"""
