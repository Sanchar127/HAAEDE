import asyncpg
from app.core.config import POSTGRES_DSN

async def get_connection():
    return await asyncpg.connect(POSTGRES_DSN)