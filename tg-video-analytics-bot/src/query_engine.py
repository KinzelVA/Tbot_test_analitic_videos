import re
import datetime as dt
from typing import Any, Tuple, Optional
from datetime import date, time

# В БД у тебя дата публикации = video_created_at (ты сам показал \d videos)
PUBLISHED_COL = "video_created_at"

_RU_MONTHS_GEN = {
    "января": 1, "февраля": 2, "марта": 3, "апреля": 4, "мая": 5, "июня": 6,
    "июля": 7, "августа": 8, "сентября": 9, "октября": 10, "ноября": 11, "декабря": 12,
}

# Поддерживаем разные падежи русских месяцев: "июня", "июне", "июнь" и т.п.
_MONTH_ALIASES = {
    1: ["январь", "января", "январе"],
    2: ["февраль", "февраля", "феврале"],
    3: ["март", "марта", "марте"],
    4: ["апрель", "апреля", "апреле"],
    5: ["май", "мая", "мае"],
    6: ["июнь", "июня", "июне"],
    7: ["июль", "июля", "июле"],
    8: ["август", "августа", "августе"],
    9: ["сентябрь", "сентября", "сентябре"],
    10: ["октябрь", "октября", "октябре"],
    11: ["ноябрь", "ноября", "ноябре"],
    12: ["декабрь", "декабря", "декабре"],
}

_MONTH_WORD_TO_NUM = {}
for m, forms in _MONTH_ALIASES.items():
    for f in forms:
        _MONTH_WORD_TO_NUM[f] = m


def _norm(text: str) -> str:
    return (text or "").lower().strip()


def _parse_int(s: str) -> int:
    s = re.sub(r"[^\d]", "", s or "")
    return int(s) if s else 0


def _parse_creator_id(text: str) -> Optional[str]:
    """
    Достаём id после 'id' (uuid/hex). В БД creator_id хранится как text,
    обычно 32 hex без дефисов — приводим к lower и убираем дефисы.
    """
    m = re.search(r"\bid\s*[:=]?\s*([0-9a-fA-F\-]{8,64})\b", text)
    if not m:
        return None
    return m.group(1).replace("-", "").lower()


def _parse_date_any(fragment: str) -> Optional[dt.date]:
    fragment = fragment.strip()

    # ISO: 2025-11-05
    m = re.search(r"\b(20\d{2})-(\d{2})-(\d{2})\b", fragment)
    if m:
        y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        return dt.date(y, mo, d)

    # dd.mm.yyyy
    m = re.search(r"\b(\d{1,2})\.(\d{1,2})\.(20\d{2})\b", fragment)
    if m:
        d, mo, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        return dt.date(y, mo, d)

    # "1 ноября 2025" / "1 ноябре 2025" etc
    m = re.search(r"\b(\d{1,2})\s+([а-яё]+)\s+(20\d{2})\b", fragment)
    if m:
        d = int(m.group(1))
        mon_word = m.group(2)
        y = int(m.group(3))
        mo = _MONTH_WORD_TO_NUM.get(mon_word)
        if mo:
            return dt.date(y, mo, d)

    return None


def _extract_dates_in_order(text: str) -> list[dt.date]:
    """
    Вытаскиваем ВСЕ даты в порядке появления: ISO, dd.mm.yyyy, "d <month> yyyy"
    """
    out: list[tuple[int, dt.date]] = []

    for m in re.finditer(r"\b(20\d{2})-(\d{2})-(\d{2})\b", text):
        y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        out.append((m.start(), dt.date(y, mo, d)))

    for m in re.finditer(r"\b(\d{1,2})\.(\d{1,2})\.(20\d{2})\b", text):
        d, mo, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        out.append((m.start(), dt.date(y, mo, d)))

    for m in re.finditer(r"\b(\d{1,2})\s+([а-яё]+)\s+(20\d{2})\b", text):
        d = int(m.group(1))
        mon_word = m.group(2)
        y = int(m.group(3))
        mo = _MONTH_WORD_TO_NUM.get(mon_word)
        if mo:
            out.append((m.start(), dt.date(y, mo, d)))

    out.sort(key=lambda x: x[0])
    return [d for _, d in out]


def _parse_ru_date_range_inclusive(text: str) -> Optional[tuple[dt.date, dt.date]]:
    """
    Понимает:
      - "с 1 ноября 2025 по 5 ноября 2025"
      - "с 01.11.2025 по 05.11.2025"
      - "с 2025-11-01 по 2025-11-05"
      - "с 1 по 5 ноября 2025"  (месяц/год указаны один раз)
    """
    t = _norm(text)

    # 1) Если в тексте явно есть две полные даты — берём первые две
    dates = _extract_dates_in_order(t)
    if len(dates) >= 2:
        start, end = dates[0], dates[1]
        if start <= end:
            return start, end
        return end, start

    # 2) Случай "с 1 по 5 ноября 2025"
    m = re.search(r"\bс\s*(\d{1,2})\s*по\s*(\d{1,2})\s*([а-яё]+)\s*(20\d{2})\b", t)
    if m:
        d1 = int(m.group(1))
        d2 = int(m.group(2))
        mon_word = m.group(3)
        y = int(m.group(4))
        mo = _MONTH_WORD_TO_NUM.get(mon_word)
        if mo:
            start = dt.date(y, mo, d1)
            end = dt.date(y, mo, d2)
            if start <= end:
                return start, end
            return end, start

    return None


def _parse_ru_month_and_year(text: str) -> Optional[tuple[int, int]]:
    """
    Понимает "в июне 2025", "за июнь 2025", "в июне 2025 года" и т.д.
    """
    t = _norm(text)
    m = re.search(r"\b(январ[ьяе]|феврал[ьяе]|март[аеи]?|апрел[ьяе]|ма[йяе]|июн[ьяе]|июл[ьяе]|август[ае]?|сентябр[ьяе]|октябр[ьяе]|ноябр[ьяе]|декабр[ьяе])\s+(20\d{2})\b", t)
    if not m:
        return None

    mon_word = m.group(1)
    year = int(m.group(2))

    # нормализуем слово месяца к нашему словарю
    # (например "марте" мы ловим как "марта/марте" — тут достаточно прямого lookup по _MONTH_WORD_TO_NUM)
    month = _MONTH_WORD_TO_NUM.get(mon_word)
    if not month:
        # запасной вариант: попробовать "срезать" окончание, но обычно не понадобится
        for k, v in _MONTH_WORD_TO_NUM.items():
            if mon_word.startswith(k[:4]):
                month = v
                break

    if not month:
        return None

    return month, year

def _extract_creator_id_token(s: str) -> str | None:
    s = (s or "").lower()
    # uuid без дефисов (как в ТЗ) или с дефисами
    m = re.search(r"\bid\s*([0-9a-f]{32})\b", s)
    if m:
        return m.group(1)
    m = re.search(r"\bid\s*([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\b", s)
    if m:
        return m.group(1).replace("-", "")
    return None

def _parse_ru_date_dmy_gen(s: str) -> date | None:
    s = (s or "").lower()
    m = re.search(r"\b(\d{1,2})\s+(января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря)\s+(\d{4})\b", s)
    if not m:
        return None
    d = int(m.group(1))
    mon = _RU_MONTHS_GEN[m.group(2)]
    y = int(m.group(3))
    return date(y, mon, d)

def _parse_hhmm(s: str) -> time | None:
    m = re.search(r"\b(\d{1,2})\s*[:.]\s*(\d{2})\b", (s or ""))
    if not m:
        return None
    hh = int(m.group(1))
    mm = int(m.group(2))
    if not (0 <= hh <= 23 and 0 <= mm <= 59):
        return None
    return time(hh, mm)

def _parse_time_range(s: str) -> tuple[time, time] | None:
    s2 = (s or "").lower()
    m = re.search(r"с\s*(\d{1,2}\s*[:.]\s*\d{2})\s*до\s*(\d{1,2}\s*[:.]\s*\d{2})", s2)
    if not m:
        return None
    t1 = _parse_hhmm(m.group(1))
    t2 = _parse_hhmm(m.group(2))
    if not t1 or not t2:
        return None
    return (t1, t2)


def build_sql(text: str) -> Tuple[str, Tuple[Any, ...]]:
    """
    Возвращает (sql, args). Если не поняли вопрос — возвращаем ("", ()).
    Все ответы стараемся вернуть одним значением (fetchval), даже топы/списки.
    """
    t = _norm(text)
    # 0) Суммарный рост просмотров креатора за интервал времени в конкретную дату (сумма delta_views_count)
    creator_id = _extract_creator_id_token(text)
    d = _parse_ru_date_dmy_gen(text)
    tr = _parse_time_range(text)

    if creator_id and d and tr and ("просмотр" in t) and (
            ("вырос" in t) or ("суммар" in t) or ("на сколько" in t) or ("насколько" in t)) and ("с " in t) and (
            " до " in t):
        t_from, t_to = tr
        return (
            """
            SELECT COALESCE(SUM(s.delta_views_count), 0)::bigint
            FROM video_snapshots s
            JOIN videos v ON v.id = s.video_id
            WHERE v.creator_id = $1
              AND s.created_at::date = $2::date
              AND s.created_at::time >= $3::time
              AND s.created_at::time <= $4::time
            """,
            (creator_id, d, t_from, t_to),
        )

    # 1) Сколько всего видео в системе?
    if (
            ("видео" in t)
            and ("в системе" in t or "всего" in t)
            and ("просмотр" not in t)  # <- ВАЖНО: чтобы не перехватывать вопросы про просмотры
            and ("замер" not in t)
            and ("креатор" not in t)
            and ("креатора" not in t)
    ):
        return "SELECT COUNT(*)::bigint FROM videos", ()

    # 2) Сколько всего замеров статистики (по всем видео)?
    if ("замер" in t or "снимк" in t or "snapshot" in t) and ("всего" in t or "сколько" in t) and ("отриц" not in t) and ("меньше" not in t):
        return "SELECT COUNT(*)::bigint FROM video_snapshots", ()

    # 3) Сколько замеров, где просмотры за час стали меньше (delta < 0)?
    if ("замер" in t or "снимк" in t or "snapshot" in t) and ("просмотр" in t) and ("час" in t) and ("меньше" in t or "отриц" in t):
        return "SELECT COUNT(*)::bigint FROM video_snapshots WHERE delta_views_count < 0", ()

    # 4) Сколько всего видео у креатора с id ...
    if ("сколько" in t) and ("видео" in t) and ("креатор" in t or "креатора" in t) and ("id" in t) and ("период" not in t):
        # но чтобы не перехватить вопросы про просмотры/порог:
        if "просмотр" not in t:
            cid = _parse_creator_id(t)
            if cid:
                return "SELECT COUNT(*)::bigint FROM videos WHERE creator_id = $1", (cid,)

    # 5) Сколько видео у креатора ... набрали больше N просмотров по итоговой статистике?
    if ("сколько" in t) and ("видео" in t) and ("креатор" in t or "креатора" in t) and ("id" in t) and ("просмотр" in t) and re.search(r"\b(больше|более|свыше|превысил[ао]?|выше)\b", t):
        cid = _parse_creator_id(t)
        mthr = re.search(r"\b(?:больше|более|свыше|превысил[ао]?|выше)\s*([\d\s]+)\b", t)
        thr = _parse_int(mthr.group(1)) if mthr else 0
        if cid and thr > 0:
            return (
                "SELECT COUNT(*)::bigint FROM videos WHERE creator_id = $1 AND views_count > $2",
                (cid, thr),
            )

    # 6) Сколько всего видео в системе набрали больше N просмотров по итоговой статистике?
    if ("сколько" in t) and ("видео" in t) and ("в системе" in t) and ("просмотр" in t) and re.search(r"\b(больше|более|свыше|превысил[ао]?|выше)\b", t):
        mthr = re.search(r"\b(?:больше|более|свыше|превысил[ао]?|выше)\s*([\d\s]+)\b", t)
        thr = _parse_int(mthr.group(1)) if mthr else 0
        if thr > 0:
            return "SELECT COUNT(*)::bigint FROM videos WHERE views_count > $1", (thr,)

    # 7) Ранняя и поздняя дата публикации
    if ("самая ранняя" in t or "ранняя" in t) and ("самая поздняя" in t or "поздняя" in t) and ("дата" in t) and ("публикац" in t or "опублик" in t):
        return (
            f"""
            SELECT
              to_char(MIN({PUBLISHED_COL}), 'YYYY-MM-DD') || ' ' || to_char(MAX({PUBLISHED_COL}), 'YYYY-MM-DD')
            FROM videos
            """,
            (),
        )

    # 8) Какой креатор выпустил больше всего видео и сколько?
    if ("какой" in t) and ("креатор" in t or "креатора" in t) and ("больше всего" in t) and ("видео" in t) and ("сколько" in t or "и сколько" in t):
        return (
            """
            SELECT creator_id || ' ' || COUNT(*)::bigint
            FROM videos
            GROUP BY creator_id
            ORDER BY COUNT(*) DESC, creator_id ASC
            LIMIT 1
            """,
            (),
        )

    # 9) Топ-5 креаторов по количеству видео
    if (("топ-5" in t) or ("топ 5" in t)) and ("креатор" in t or "креатора" in t) and ("видео" in t):
        return (
            """
            WITH top5 AS (
              SELECT creator_id, COUNT(*)::bigint AS cnt
              FROM videos
              GROUP BY creator_id
              ORDER BY cnt DESC, creator_id ASC
              LIMIT 5
            )
            SELECT COALESCE(string_agg(creator_id || ' ' || cnt::text, E'\n' ORDER BY cnt DESC, creator_id ASC), '')
            FROM top5
            """,
            (),
        )

    # 10) Сколько видео опубликовал креатор ... в период с ... по ... включительно
    if ("сколько" in t) and ("видео" in t) and ("креатор" in t or "креатора" in t) and ("id" in t) and ("период" in t) and ("с" in t) and ("по" in t):
        cid = _parse_creator_id(t)
        dr = _parse_ru_date_range_inclusive(t)
        if cid and dr:
            start, end = dr
            return (
                f"SELECT COUNT(*)::bigint FROM videos WHERE creator_id=$1 AND {PUBLISHED_COL}::date BETWEEN $2 AND $3",
                (cid, start, end),
            )
        # если не смогли распарсить даты — лучше честно сказать "не понял", чем дать неверный ответ
        return "", ()

    # 11) Суммарные просмотры всех видео, опубликованных в <месяце> <год>
    if ("суммар" in t or "сумм" in t) and ("просмотр" in t) and ("опублик" in t or "публикац" in t):
        my = _parse_ru_month_and_year(t)
        if my:
            month, year = my
            start = dt.date(year, month, 1)
            # next month:
            if month == 12:
                next_month = dt.date(year + 1, 1, 1)
            else:
                next_month = dt.date(year, month + 1, 1)

            return (
                f"""
                SELECT COALESCE(SUM(views_count), 0)::bigint
                FROM videos
                WHERE {PUBLISHED_COL}::date >= $1
                  AND {PUBLISHED_COL}::date <  $2
                """,
                (start, next_month),
            )

    return "", ()
