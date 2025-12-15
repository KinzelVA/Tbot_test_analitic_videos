from __future__ import annotations

import re
from datetime import date, datetime
from typing import Tuple, Optional

_CREATOR_ID_RE = re.compile(r"\b[0-9a-f]{32}\b", re.IGNORECASE)

_RU_MONTHS = {
    "января": 1, "февраля": 2, "марта": 3, "апреля": 4, "мая": 5, "июня": 6,
    "июля": 7, "августа": 8, "сентября": 9, "октября": 10, "ноября": 11, "декабря": 12,
}


def _norm(text: str) -> str:
    return (text or "").lower().strip()


def _extract_creator_id(t: str) -> Optional[str]:
    m = _CREATOR_ID_RE.search(t)
    return m.group(0).lower() if m else None


def _extract_first_int(t: str) -> Optional[int]:
    # supports "10 000", "100000", "1_000" etc.
    m = re.search(r"(\d[\d\s_]{0,20})", t)
    if not m:
        return None
    digits = re.sub(r"\D", "", m.group(1))
    return int(digits) if digits else None


def _parse_iso_date(s: str) -> Optional[date]:
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return None


def _parse_ru_date_fragment(fragment: str) -> Optional[date]:
    m = re.search(
        r"\b(\d{1,2})\s+"
        r"(января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря)\s+"
        r"(\d{4})\b",
        fragment,
    )
    if not m:
        return None
    d = int(m.group(1))
    month = _RU_MONTHS[m.group(2)]
    y = int(m.group(3))
    try:
        return date(y, month, d)
    except Exception:
        return None


def _extract_two_dates(t: str) -> Optional[Tuple[date, date]]:
    iso = re.findall(r"\b\d{4}-\d{2}-\d{2}\b", t)
    dates: list[date] = []
    for s in iso:
        d = _parse_iso_date(s)
        if d:
            dates.append(d)
    if len(dates) >= 2:
        return dates[0], dates[1]

    ru_matches = re.findall(
        r"\b\d{1,2}\s+(?:января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря)\s+\d{4}\b",
        t,
    )
    for s in ru_matches:
        d = _parse_ru_date_fragment(s)
        if d:
            dates.append(d)
    if len(dates) >= 2:
        return dates[0], dates[1]

    return None


def build_sql(text: str) -> Tuple[str, tuple]:
    t = _norm(text)
    if not t:
        return ("SELECT 0::bigint", ())

    creator_id = _extract_creator_id(t)

    # Ключевой фикс: чтобы "1061" из creator_id не считалось порогом просмотров
    t_no_id = t
    if creator_id:
        t_no_id = t_no_id.replace(creator_id, " ")

    # -----------------------------
    # A) Video snapshots (замеры / снимки)
    # -----------------------------
    if ("замер" in t or "снимок" in t or "snapshot" in t) and ("просмотр" in t or "views" in t) and (
        "отриц" in t or "меньше" in t or "уменьш" in t or "стало меньше" in t
    ):
        return ("SELECT COUNT(*)::bigint FROM video_snapshots WHERE delta_views_count < 0", ())

    if ("замер" in t or "замеров" in t or "снимок" in t or "снимков" in t or "snapshot" in t) and "статистик" in t:
        return ("SELECT COUNT(*)::bigint FROM video_snapshots", ())

    # -----------------------------
    # B) Non-count answers (text responses)
    # -----------------------------
    if ("самая ранняя" in t or "ранняя" in t) and ("самая поздняя" in t or "поздняя" in t) and (
        "дата" in t or "число" in t
    ):
        return (
            """
            SELECT
              to_char(MIN(video_created_at)::date, 'YYYY-MM-DD') || ' ' ||
              to_char(MAX(video_created_at)::date, 'YYYY-MM-DD')
            FROM videos
            """,
            (),
        )

    if ("топ" in t) and ("креатор" in t or "креаторов" in t or "авторов" in t) and ("количеству" in t or "числу" in t) and "видео" in t:
        return (
            """
            SELECT COALESCE(string_agg(creator_id || ' ' || cnt::bigint, E'\n'), '')
            FROM (
              SELECT creator_id, COUNT(*) AS cnt
              FROM videos
              GROUP BY creator_id
              ORDER BY cnt DESC
              LIMIT 5
            ) t
            """,
            (),
        )

    if ("креатор" in t or "креатора" in t or "автор" in t) and ("больше всего" in t or "больше всех" in t) and "видео" in t:
        return (
            """
            SELECT creator_id || ' ' || COUNT(*)::bigint
            FROM videos
            GROUP BY creator_id
            ORDER BY COUNT(*) DESC
            LIMIT 1
            """,
            (),
        )

    # -----------------------------
    # C) Videos — counts
    # -----------------------------
    if creator_id and "видео" in t and ("просмотр" in t) and ("больше" in t or "свыше" in t or ">" in t):
        threshold = _extract_first_int(t_no_id) or 0
        return (
            "SELECT COUNT(*)::bigint FROM videos WHERE creator_id = $1 AND views_count > $2",
            (creator_id, threshold),
        )

    if "видео" in t and ("просмотр" in t) and ("больше" in t or "свыше" in t or ">" in t):
        threshold = _extract_first_int(t) or 0
        return ("SELECT COUNT(*)::bigint FROM videos WHERE views_count > $1", (threshold,))

    if creator_id and "видео" in t and ("у креатора" in t or "креатора" in t or "креатор" in t or "автор" in t):
        return ("SELECT COUNT(*)::bigint FROM videos WHERE creator_id = $1", (creator_id,))

    if ("сколько" in t or "всего" in t) and "видео" in t:
        return ("SELECT COUNT(*)::bigint FROM videos", ())

    # -----------------------------
    # D) Date-range queries (опционально)
    # -----------------------------
    if creator_id and ("вышло" in t or "опублик" in t) and "с" in t and "по" in t:
        rng = _extract_two_dates(t)
        if rng:
            d1, d2 = rng
            return (
                """
                SELECT COUNT(*)::bigint
                FROM videos
                WHERE creator_id = $1
                  AND video_created_at::date >= $2
                  AND video_created_at::date <= $3
                """,
                (creator_id, d1, d2),
            )

    return ("SELECT 0::bigint", ())
