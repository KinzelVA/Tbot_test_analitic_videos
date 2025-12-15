import asyncio
import os

import asyncpg


def _build_dsn_from_env() -> str | None:
    """
    1) Если DSN задан явно (любым из популярных имён) — используем его.
    2) Иначе собираем DSN из POSTGRES_* переменных (которые обычно есть в docker-compose/.env).
    """
    for key in ("DATABASE_DSN", "POSTGRES_DSN", "DATABASE_URL"):
        v = os.getenv(key)
        if v:
            return v

    host = os.getenv("POSTGRES_HOST") or os.getenv("DB_HOST") or "db"
    port = os.getenv("POSTGRES_PORT") or os.getenv("DB_PORT") or "5432"
    user = os.getenv("POSTGRES_USER") or "postgres"
    password = os.getenv("POSTGRES_PASSWORD") or "postgres"
    dbname = os.getenv("POSTGRES_DB") or "video_analytics"

    return f"postgresql://{user}:{password}@{host}:{port}/{dbname}"


class Database:
    def __init__(self):
        self._pool: asyncpg.Pool | None = None
        self._dsn = _build_dsn_from_env()

    async def connect(self):
        if not self._dsn:
            raise RuntimeError("DB DSN is empty. Check .env/docker-compose env vars.")

        last_err = None
        for _ in range(30):
            try:
                self._pool = await asyncpg.create_pool(dsn=self._dsn, min_size=1, max_size=10)
                return
            except Exception as e:
                last_err = e
                await asyncio.sleep(1)

        raise last_err

    async def close(self):
        if self._pool:
            await self._pool.close()
            self._pool = None

    async def fetchval(self, sql: str, *args):
        if not self._pool:
            raise RuntimeError("DB pool is not connected")
        async with self._pool.acquire() as conn:
            return await conn.fetchval(sql, *args)


db = Database()
