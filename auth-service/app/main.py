import time
import uuid
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from .auth import create_refresh_token, create_token, decode_refresh_token, hash_password, verify_password
from .config import settings
from .database import engine, get_db
from .models import User
from .telemetry import setup_telemetry

START_TIME = time.time()


@asynccontextmanager
async def lifespan(_app: FastAPI):
    yield
    await engine.dispose()


app = FastAPI(title="NexusChat Auth Service", lifespan=lifespan)

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


class RefreshRequest(BaseModel):
    refresh_token: str


@app.get("/health")
async def health():
    return {"status": "ok", "service": "auth-service", "version": "1.0.0", "uptime": round(time.time() - START_TIME, 2)}


@app.get("/ready")
async def ready():
    from sqlalchemy import text
    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        return {"status": "ok", "service": "auth-service"}
    except Exception:
        raise HTTPException(status_code=503, detail="Database not ready")


@app.get("/live")
async def live():
    return {"status": "ok", "service": "auth-service"}


@app.post("/register", response_model=TokenResponse)
async def register(req: AuthRequest, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(User).where(User.username == req.username))
    if result.scalar_one_or_none():
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Username already taken")
    user = User(id=uuid.uuid4(), username=req.username, password_hash=hash_password(req.password))
    db.add(user)
    await db.commit()
    return TokenResponse(access_token=create_token(str(user.id)))


@app.post("/login", response_model=TokenResponse)
async def login(req: AuthRequest, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(User).where(User.username == req.username))
    user = result.scalar_one_or_none()
    if not user or not verify_password(req.password, user.password_hash):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")
    return TokenResponse(access_token=create_token(str(user.id)))


@app.post("/refresh")
async def refresh(req: RefreshRequest):
    payload = decode_refresh_token(req.refresh_token)
    if payload is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or expired refresh token")
    user_id = payload["sub"]
    new_access_token = create_token(user_id)
    new_refresh_token = create_refresh_token(user_id)
    return {"access_token": new_access_token, "refresh_token": new_refresh_token, "token_type": "bearer"}
