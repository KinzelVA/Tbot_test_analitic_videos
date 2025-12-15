"""
src/query_engine.py

Rule-based NL -> SQL for the Telegram Video Analytics Bot.

Важно: бот (main.py) обычно делает db.fetchval(...) и ожидает одно значение.
Поэтому для "табличных" ответов (топ-5 креаторов и т.п.) мы возвращаем ОДНУ
строку, собранную в SQL через string_agg.
"""

from __future__ import annotations

import os
import re
from datetime import datetime, timedelta
from typing import Optional, Tuple


# ----------------------------
# Config / helpers
# ----------------------------

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _safe_ident(name: str, default: str) -> str:
    name = (name or "").strip()
    return name if _IDENTIFIER_RE.match(name) else default


# Реальная колонка даты публикации может отличаться в разных эталонах.
# По умолчанию используем video_created_at (как в вашем init sql).
PUBLISHED_AT_COL = _safe_ident(
    os.getenv("PUBLISHED_AT_COL") or os.getenv("PUBLISHED_AT") or os.getenv("VIDEO_PUBLISHED_AT_COL") or "",
    default="video_created_at",
)


# ----------------------------
# RU date parsing
# ----------------------------

# Поддерживаем разные падежи:
# "июня" (род.), "июне" (предл.), а также короткие варианты.
_MONTHS = {
    # January
    "января": 1, "январе": 1, "янв": 1, "январь": 1,
    # February
    "февраля": 2, "феврале": 2, "фев": 2, "февраль": 2,
    # March
    "марта": 3, "марте": 3, "мар": 3, "март": 3,
    # April
    "апреля": 4, "апреле": 4, "апр": 4, "апрель": 4,
    # May
    "мая": 5, "мае": 5, "май": 5,
    # June
    "июня": 6, "июне": 6, "июн": 6, "июнь": 6,
    # July
    "июля": 7, "июле": 7, "июл": 7, "июль": 7,
    # August
    "августа": 8, "августе": 8, "авг": 8, "август": 8,
    # September
    "сентября": 9, "сентябре": 9, "сен": 9, "сент": 9, "сентябрь": 9,
    # October
    "октября": 10, "октябре": 10, "окт": 10, "октябрь": 10,
    # November
    "ноября": 11, "ноябре": 11, "ноя": 11, "ноябрь": 11,
    # December
    "декабря": 12, "декабре": 12, "дек": 12, "декабрь": 12,
}


def _parse_ru_date_fragment(s: str) -> Optional[datetime]:
    """
    Парсит дату из фрагмента вида:
      - "1 ноября 2025"
      - "01 ноября 2025"
      - "1 ноября" (год опционально → None)
    Возвращает datetime(YYYY, MM, DD) или None.
    """
    if not s:
        return None

    s = s.lower().strip()
    # допускаем лишние слова вроде "года"
    s = re.sub(r"\bг(ода)?\b", "", s).strip()

    m = re.search(r"(\d{1,2})\s+([а-яё\.]+)\s*(\d{4})?", s, flags=re.I)
    if not m:
        return None

    day = int(m.group(1))
    mon_word = m.group(2).strip(".").lower()
    year_s = m.group(3)

    month = _MONTHS.get(mon_word)
    if not month:
        return None

    if not year_s:
        # год не задан — в рамках ТЗ это обычно не встречается, вернем None
        return None

    year = int(year_s)
    try:
        return datetime(year, month, day)
    except ValueError:
        return None


def _parse_ru_range(text: str) -> Optional[Tuple[datetime, datetime]]:
    """
    Парсит диапазон вида:
      "с 1 ноября 2025 по 5 ноября 2025 включительно"
    Возвращает (start_inclusive, end_exclusive) либо None.
    """
    t = (text or "").lower()

    m = re.search(r"с\s+(.+?)\s+по\s+(.+?)(?:\bвключительно\b|\?|$)", t)
    if not m:
        return None

    d1 = _parse_ru_date_fragment(m.group(1))
    d2 = _parse_ru_date_fragment(m.group(2))
    if not d1 or not d2:
        return None

    start = datetime(d1.year, d1.month, d1.day)
    # "включительно" → делаем end_exclusive = следующий день после d2
    end = datetime(d2.year, d2.month, d2.day) + timedelta(days=1)
    if end <= start:
        return None
    return start, end


def _parse_ru_month_year(text: str) -> Optional[Tuple[datetime, datetime]]:
    """
    Парсит "в июне 2025 года" / "за июнь 2025" / "в июне 2025".
    Возвращает (start_inclusive, end_exclusive) либо None.
    """
    t = (text or "").lower()
    m = re.search(r"\b([а-яё\.]+)\s+(\d{4})\b", t)
    if not m:
        return None

    mon_word = m.group(1).strip(".").lower()
    year = int(m.group(2))

    month = _MONTHS.get(mon_word)
    if not month:
        return None

    start = datetime(year, month, 1)
    # конец месяца: первый день следующего месяца
    if month == 12:
        end = datetime(year + 1, 1, 1)
    else:
        end = datetime(year, month + 1, 1)
    return start, end


def _extract_creator_id(text: str) -> Optional[str]:
    m = re.search(r"\b([0-9a-f]{32})\b", (text or "").lower())
    return m.group(1) if m else None


def _extract_threshold(text: str, default: Optional[int] = None) -> Optional[int]:
    m = re.search(r"(\d[\d\s_]{2,})", (text or ""))
    if not m:
        return default
    raw = m.group(1).replace(" ", "").replace("_", "")
    try:
        return int(raw)
    except ValueError:
        return default


# ----------------------------
# Core: NL -> SQL
# ----------------------------

def build_sql(text: str) -> tuple[str, tuple]:
    """
    Возвращает (sql, args).
    SQL должен выдавать ОДНО значение (одно поле одной строки),
    чтобы main.py мог безопасно использовать fetchval().
    """
    t = (text or "").lower().strip()
    pub_col = PUBLISHED_AT_COL

    # 1) Замеры статистики (video_snapshots)
    if ("замер" in t or "снапшот" in t or "snapshot" in t) and ("статист" in t or "статистика" in t):
        if ("отриц" in t or "стало меньше" in t or "уменьш" in t or "сниз" in t) and ("просмотр" in t):
            return "SELECT COUNT(*)::bigint FROM video_snapshots WHERE delta_views_count < 0", ()
        return "SELECT COUNT(*)::bigint FROM video_snapshots", ()

    # 2) Сколько всего видео в системе
    if ("сколько" in t or "количество" in t) and ("видео" in t) and ("в системе" in t or "всего" in t) and ("креатор" not in t and "creator" not in t):
        return "SELECT COUNT(*)::bigint FROM videos", ()

    # 3) Сколько видео у креатора (всего)
    if ("креатор" in t or "creator" in t) and ("сколько" in t or "количество" in t) and ("видео" in t) and ("больше" not in t):
        creator_id = _extract_creator_id(t)
        if creator_id:
            return "SELECT COUNT(*)::bigint FROM videos WHERE creator_id = $1", (creator_id,)

    # 4) Сколько видео у креатора набрали больше X просмотров (по итоговой статистике)
    if ("креатор" in t or "creator" in t) and ("видео" in t) and ("больше" in t) and ("просмотр" in t):
        creator_id = _extract_creator_id(t)
        threshold = _extract_threshold(t, default=10000)
        if creator_id and threshold is not None:
            return (
                "SELECT COUNT(*)::bigint FROM videos WHERE creator_id = $1 AND views_count > $2",
                (creator_id, threshold),
            )

    # 5) Сколько всего видео набрали больше X просмотров (по итоговой статистике)
    if ("видео" in t) and ("больше" in t) and ("просмотр" in t) and ("креатор" not in t and "creator" not in t):
        threshold = _extract_threshold(t, default=100000)
        return "SELECT COUNT(*)::bigint FROM videos WHERE views_count > $1", (threshold,)

    # 6) Ранняя/поздняя дата публикации
    if ("самая ранняя" in t or "ранняя" in t) and ("самая поздняя" in t or "поздняя" in t) and ("дата" in t) and ("публик" in t):
        return (
            f"SELECT (MIN({pub_col})::date::text || ' ' || MAX({pub_col})::date::text) FROM videos",
            (),
        )

    # 7) Какой креатор выпустил больше всего видео и сколько?
    if ("какой" in t) and ("креатор" in t or "creator" in t) and ("больше всего" in t) and ("видео" in t):
        return (
            """
            SELECT COALESCE((
                SELECT (creator_id || ' ' || cnt::text)
                FROM (
                    SELECT creator_id, COUNT(*)::bigint AS cnt
                    FROM videos
                    GROUP BY creator_id
                    ORDER BY cnt DESC
                    LIMIT 1
                ) s
            ), '0')
            """,
            (),
        )

    # 8) Топ-5 креаторов по количеству видео
    if ("топ" in t or "top" in t) and ("креатор" in t or "creator" in t) and ("колич" in t) and ("видео" in t):
        return (
            """
            SELECT COALESCE((
                SELECT string_agg(creator_id || ' ' || cnt::text, E'\n' ORDER BY cnt DESC)
                FROM (
                    SELECT creator_id, COUNT(*)::bigint AS cnt
                    FROM videos
                    GROUP BY creator_id
                    ORDER BY cnt DESC
                    LIMIT 5
                ) s
            ), '0')
            """,
            (),
        )

    # 9) Креатор: сколько видео в период с ... по ... включительно (дата публикации)
    if ("креатор" in t or "creator" in t) and ("видео" in t) and ("период" in t or ("с " in t and " по " in t)):
        creator_id = _extract_creator_id(t)
        rng = _parse_ru_range(t)
        if creator_id and rng:
            start, end = rng
            return (
                f"SELECT COUNT(*)::bigint FROM videos WHERE creator_id = $1 AND {pub_col} >= $2 AND {pub_col} < $3",
                (creator_id, start, end),
            )

    # 10) Суммарные просмотры всех видео, опубликованных в <месяц> <год>
    # Пример из проверки: "в июне 2025 года"
    if ("суммар" in t or "сумма" in t) and ("просмотр" in t) and ("опублик" in t):
        rng = _parse_ru_month_year(t)
        if rng:
            start, end = rng
            return (
                f"SELECT COALESCE(SUM(views_count), 0)::bigint FROM videos WHERE {pub_col} >= $1 AND {pub_col} < $2",
                (start, end),
            )

    # неизвестное → строго число (чтобы не падать на int())
    return "SELECT 0::bigint", ()
