import asyncpg

class DB:
    def __init__(self, dsn: str):
        self._dsn = dsn
        self._pool: asyncpg.Pool | None = None

    async def connect(self) -> None:
        self._pool = await asyncpg.create_pool(dsn=self._dsn, min_size=1, max_size=10)

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()

    async def fetchval(self, sql: str, *args):
        if self._pool is None:
            raise RuntimeError("DB pool is not initialized. Call connect() first.")
        async with self._pool.acquire() as conn:
            return await conn.fetchval(sql, *args)
