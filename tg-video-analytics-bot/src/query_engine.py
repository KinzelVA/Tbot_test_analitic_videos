import re
from datetime import datetime, timedelta, date


_MONTHS = {
    "января": 1, "февраля": 2, "марта": 3, "апреля": 4, "мая": 5, "июня": 6,
    "июля": 7, "августа": 8, "сентября": 9, "октября": 10, "ноября": 11, "декабря": 12
}


def _parse_int(s: str) -> int:
    return int(re.sub(r"[^\d]", "", s))


def _parse_ru_date(text: str) -> date | None:
    m = re.search(r"(\d{1,2})\s+([а-я]+)\s+(\d{4})", text.lower())
    if not m:
        return None
    d = int(m.group(1))
    mon = _MONTHS.get(m.group(2))
    y = int(m.group(3))
    if not mon:
        return None
    return date(y, mon, d)


def _parse_ru_range(text: str) -> tuple[date, date] | None:
    t = text.lower()

    # "с 1 ноября 2025 по 5 ноября 2025"
    m = re.search(r"с\s+(\d{1,2})\s+([а-я]+)\s+(\d{4})\s+по\s+(\d{1,2})\s+([а-я]+)\s+(\d{4})", t)
    if m:
        d1, m1, y1, d2, m2, y2 = m.groups()
        mon1 = _MONTHS.get(m1)
        mon2 = _MONTHS.get(m2)
        if not mon1 or not mon2:
            return None
        return date(int(y1), mon1, int(d1)), date(int(y2), mon2, int(d2))

    # "с 1 по 5 ноября 2025"
    m = re.search(r"с\s+(\d{1,2})\s+по\s+(\d{1,2})\s+([а-я]+)\s+(\d{4})", t)
    if m:
        d1, d2, mw, y = m.groups()
        mon = _MONTHS.get(mw)
        if not mon:
            return None
        return date(int(y), mon, int(d1)), date(int(y), mon, int(d2))

    return None


def build_sql(text: str) -> tuple[str, tuple]:
    t = (text or "").lower().strip()

    # 1) "Сколько всего видео есть в системе?"
    if "сколько" in t and "видео" in t and ("всего" in t or "в системе" in t):
        return "SELECT COUNT(*)::bigint FROM videos", ()

    # 2a) "Сколько видео у креатора с id ... набрали больше 10 000 просмотров по итоговой статистике?"
    # Важно: считаем по итоговой таблице videos + фильтр creator_id
    if (
        "сколько" in t
        and "видео" in t
        and ("креатора" in t or "креатор" in t)
        and "id" in t
        and ("просмотр" in t or "просмотров" in t)
        and re.search(r"(больше|более|свыше|превысил|превысило|выше)", t)
    ):
        mid = re.search(r"(?:id\s*[:=]?\s*)([0-9a-fA-F\-]{8,64})", t)
        mthr = re.search(r"(больше|более|свыше|превысил[ао]?|выше)\s*([\d\s]+)", t)
        if mid and mthr:
            creator_raw = mid.group(1)
            creator_id = creator_raw.replace("-", "").lower()  # на случай, если попадётся UUID-формат
            thr = _parse_int(mthr.group(2))
            return (
                "SELECT COUNT(*)::bigint FROM videos WHERE creator_id = $1 AND views_count > $2",
                (creator_id, thr),
            )


    # 2) "Сколько видео набрало больше 100 000 просмотров за всё время?"
    if "видео" in t and ("просмотр" in t or "просмотров" in t) and re.search(r"(больше|более|свыше|превысил|превысило|выше)", t):
        m = re.search(r"(больше|более|свыше|превысил[ао]?|выше)\s*([\d\s]+)", t)
        if m:
            thr = _parse_int(m.group(2))
            return "SELECT COUNT(*)::bigint FROM videos WHERE views_count > $1", (thr,)

    # 3) "Сколько видео у креатора с id ... вышло с 1 ноября 2025 по 5 ноября 2025 включительно?"
    if "сколько" in t and "видео" in t and ("креатора" in t or "креатор" in t) and "id" in t:
        mid = re.search(r"(?:id\s*[:=]?\s*)([0-9a-fA-F\-]{8,64})", t)
        dr = _parse_ru_range(t)
        if mid and dr:
            d1, d2 = dr
            start = datetime(d1.year, d1.month, d1.day)
            end = datetime(d2.year, d2.month, d2.day) + timedelta(days=1)
            return (
                """
                SELECT COUNT(*)::bigint
                FROM videos
                WHERE creator_id = $1
                  AND video_created_at >= $2
                  AND video_created_at <  $3
                """,
                (mid.group(1), start, end),
            )

    # 4) "На сколько просмотров в сумме выросли все видео 28 ноября 2025?"
    if ("на сколько" in t or "прирост" in t) and ("просмотр" in t) and ("в сумме" in t or "всего" in t) and ("вырос" in t or "выросли" in t or "прирост" in t):
        d = _parse_ru_date(t)
        if d:
            start = datetime(d.year, d.month, d.day)
            end = start + timedelta(days=1)
            return (
                """
                SELECT COALESCE(SUM(delta_views_count), 0)::bigint
                FROM video_snapshots
                WHERE created_at >= $1 AND created_at < $2
                """,
                (start, end),
            )

    # 5) "Сколько разных видео получали новые просмотры 27 ноября 2025?"
    if "сколько" in t and "разных" in t and "видео" in t and ("новые просмотры" in t or "получали" in t or "получили" in t):
        d = _parse_ru_date(t)
        if d:
            start = datetime(d.year, d.month, d.day)
            end = start + timedelta(days=1)
            return (
                """
                SELECT COUNT(DISTINCT video_id)::bigint
                FROM video_snapshots
                WHERE created_at >= $1 AND created_at < $2
                  AND delta_views_count > 0
                """,
                (start, end),
            )

    # неизвестное → строго число
    return "SELECT 0::bigint", ()
