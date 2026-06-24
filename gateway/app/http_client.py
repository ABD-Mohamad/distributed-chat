import httpx

from .config import settings


async def register(username: str, password: str) -> dict | None:
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.post(f"{settings.lb_url}/register", json={"username": username, "password": password})
        if resp.is_error:
            return None
        return resp.json()


async def login(username: str, password: str) -> dict | None:
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.post(f"{settings.lb_url}/login", json={"username": username, "password": password})
        if resp.is_error:
            return None
        return resp.json()


async def create_chat(token: str, name: str) -> dict | None:
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.post(f"{settings.lb_url}/chats", json={"name": name}, headers={"Authorization": f"Bearer {token}"})
        if resp.is_error:
            return None
        return resp.json()


async def list_chats(token: str) -> list | None:
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get(f"{settings.lb_url}/chats", headers={"Authorization": f"Bearer {token}"})
        if resp.is_error:
            return None
        return resp.json()


async def join_chat(token: str, chat_id: str) -> dict | None:
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.post(f"{settings.lb_url}/chats/{chat_id}/join", headers={"Authorization": f"Bearer {token}"})
        if resp.is_error:
            return None
        return resp.json()


async def get_history(token: str, chat_id: str, limit: int = 50) -> list | None:
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get(
            f"{settings.lb_url}/chats/{chat_id}/messages?limit={limit}",
            headers={"Authorization": f"Bearer {token}"},
        )
        if resp.is_error:
            return None
        return resp.json()
