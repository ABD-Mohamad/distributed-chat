import grpc

import chat_pb2
import chat_pb2_grpc


class ChatClient:
    def __init__(self, target: str = "chat-service:50051"):
        self.channel = grpc.aio.insecure_channel(target)
        self.stub = chat_pb2_grpc.ChatServiceStub(self.channel)

    async def create_chat(self, name: str, user_id: str) -> dict:
        resp = await self.stub.CreateChat(
            chat_pb2.CreateChatRequest(name=name, user_id=user_id)
        )
        return {"id": resp.id, "name": resp.name, "created_at": resp.created_at}

    async def list_chats(self, user_id: str) -> list[dict]:
        resp = await self.stub.ListChats(
            chat_pb2.ListChatsRequest(user_id=user_id)
        )
        return [
            {"id": c.id, "name": c.name, "created_at": c.created_at}
            for c in resp.chats
        ]

    async def join_chat(self, chat_id: str, user_id: str) -> str:
        resp = await self.stub.JoinChat(
            chat_pb2.JoinChatRequest(chat_id=chat_id, user_id=user_id)
        )
        return resp.detail

    async def send_message(self, chat_id: str, sender_id: str, body: str) -> dict:
        resp = await self.stub.SendMessage(
            chat_pb2.SendMessageRequest(
                chat_id=chat_id, sender_id=sender_id, body=body
            )
        )
        return {"message_id": resp.message_id, "status": resp.status}

    async def get_history(self, chat_id: str, user_id: str, limit: int = 50) -> list[dict]:
        resp = await self.stub.GetHistory(
            chat_pb2.HistoryRequest(chat_id=chat_id, user_id=user_id, limit=limit)
        )
        return [
            {
                "id": m.id,
                "chat_id": m.chat_id,
                "sender_id": m.sender_id,
                "sender_username": m.sender_username,
                "body": m.body,
                "sent_at": m.sent_at,
            }
            for m in resp.messages
        ]
