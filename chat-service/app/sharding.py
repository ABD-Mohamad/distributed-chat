import hashlib
import logging

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from .config import settings

logger = logging.getLogger(__name__)

NUM_SHARDS = 2


class ShardConnection:
    def __init__(self, primary_url: str, replica_url: str):
        self.primary_engine = create_async_engine(primary_url, echo=False, pool_size=5, max_overflow=10)
        self.replica_engine = create_async_engine(replica_url, echo=False, pool_size=5, max_overflow=10)
        self.primary_session = async_sessionmaker(self.primary_engine, expire_on_commit=False)
        self.replica_session = async_sessionmaker(self.replica_engine, expire_on_commit=False)

    async def dispose(self):
        await self.primary_engine.dispose()
        await self.replica_engine.dispose()


_shards: list[ShardConnection] = []


def init_shards():
    global _shards
    _shards = [
        ShardConnection(settings.shard0_primary_url, settings.shard0_replica_url),
        ShardConnection(settings.shard1_primary_url, settings.shard1_replica_url),
    ]
    logger.info(f"Initialized {len(_shards)} shards")


async def dispose_shards():
    for shard in _shards:
        await shard.dispose()


def get_shard_id(chat_id: str) -> int:
    h = int(hashlib.md5(chat_id.encode()).hexdigest(), 16)
    return h % NUM_SHARDS


def get_shard(chat_id: str) -> ShardConnection:
    sid = get_shard_id(chat_id)
    return _shards[sid]


def get_primary_session(chat_id: str):
    return _shards[get_shard_id(chat_id)].primary_session()


def get_replica_session(chat_id: str):
    return _shards[get_shard_id(chat_id)].replica_session()


def get_all_primary_sessions():
    for shard in _shards:
        yield shard.primary_session()
