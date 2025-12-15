import asyncio
import os

from aiogram import Bot, Dispatcher
from aiogram.types import Message

from .db import db
from .query_engine import build_sql


dp = Dispatcher()


@dp.message()
async def on_message(message: Message):
    sql, args = build_sql(message.text or "")
    val = await db.fetchval(sql, *args)

    # НИКАКОГО int() — ответы бывают строками (топ, даты и т.д.)
    await message.answer(str(val if val is not None else 0))


async def main():
    token = os.getenv("BOT_TOKEN")
    if not token:
        raise RuntimeError("BOT_TOKEN is empty. Set BOT_TOKEN in .env")

    await db.connect()
    bot = Bot(token=token)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
