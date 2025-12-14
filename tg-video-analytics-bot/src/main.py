import asyncio
from aiogram import Bot, Dispatcher, Router
from aiogram.types import Message

from .config import load_config
from .db import DB
from .query_engine import build_sql

router = Router()

@router.message()
async def on_message(message: Message, db: DB):
    text = (message.text or "").strip()
    sql, args = build_sql(text)
    val = await db.fetchval(sql, *args)
    await message.answer(str(int(val or 0)))

async def main():
    cfg = load_config()

    db = DB(cfg.db_dsn)
    await db.connect()

    bot = Bot(token=cfg.bot_token)
    dp = Dispatcher()
    dp.include_router(router)

    try:
        await dp.start_polling(bot, db=db)
    finally:
        await db.close()

if __name__ == "__main__":
    asyncio.run(main())
