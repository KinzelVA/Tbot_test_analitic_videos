import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

@dataclass(frozen=True)
class Config:
    bot_token: str
    db_dsn: str

def load_config() -> Config:
    bot_token = os.getenv("BOT_TOKEN", "").strip()
    if not bot_token:
        raise RuntimeError("BOT_TOKEN is empty. Put it into .env")

    db_host = os.getenv("DB_HOST", "db").strip()
    db_port = int(os.getenv("DB_PORT", "5432"))
    db_name = os.getenv("POSTGRES_DB", "video_analytics").strip()
    db_user = os.getenv("POSTGRES_USER", "postgres").strip()
    db_password = os.getenv("POSTGRES_PASSWORD", "postgres").strip()

    db_dsn = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    return Config(bot_token=bot_token, db_dsn=db_dsn)
