from pydantic import BaseModel


class RegisterRequest(BaseModel):
    username: str
    password: str


class LoginRequest(BaseModel):
    username: str
    password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"


class CreateChatRequest(BaseModel):
    name: str


class ChatResponse(BaseModel):
    id: str
    name: str
    created_at: str


class MessageResponse(BaseModel):
    id: str
    chat_id: str
    sender_id: str
    sender_username: str
    body: str
    sent_at: str


class ErrorResponse(BaseModel):
    detail: str
