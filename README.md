# Telegram Bot: Video Analytics (NL → SQL)

Telegram-бот для аналитики по видео: принимает вопросы на естественном русском языке и возвращает **одно число** (count/sum/growth), считая метрики по данным в PostgreSQL.

Бот: https://t.me/tbot_analytics_videos_viktor_bot

---

## Стек

- Python 3.12
- Telegram Bot: **aiogram 3.x**
- База данных: **PostgreSQL 16**
- Импорт данных: Python + psycopg2
- Запуск: **Docker Compose**

---

## Структура данных

Данные загружаются из JSON-файла с массивом `videos`, где каждый объект — одно видео с вложенными почасовыми снапшотами.

### Таблица `videos` (итоговая статистика по ролику)

- `id` (UUID, PK) — идентификатор видео
- `creator_id` (TEXT) — идентификатор креатора
- `video_created_at` (timestamptz) — дата/время публикации
- `views_count`, `likes_count`, `comments_count`, `reports_count` (BIGINT) — финальные значения
- `created_at`, `updated_at` (timestamptz) — служебные поля

### Таблица `video_snapshots` (почасовые замеры по ролику)

- `id` (TEXT, PK) — идентификатор снапшота
- `video_id` (UUID, FK → videos.id)
- текущие значения: `views_count`, `likes_count`, `comments_count`, `reports_count`
- приращения: `delta_views_count`, `delta_likes_count`, `delta_comments_count`, `delta_reports_count`
- `created_at` (timestamptz) — время замера (раз в час)
- `updated_at` — служебное поле

DDL таблиц находится в: `sql/001_init.sql`

---

## Как работает распознавание запроса (NL → SQL)

Используется детерминированный rule-based парсер (регулярные выражения + парсинг дат на русском) для преобразования текстового запроса в заранее определённый SQL-шаблон.

Почему такой подход:
- **не галлюцинирует** на малом датасете;
- выдаёт воспроизводимые ответы;
- безопасен: бот выполняет только заранее подготовленные SQL-шаблоны.

Точка входа логики: `src/query_engine.py`  
Бот всегда возвращает **одно число**. Неизвестный/неподдержанный запрос возвращает `0`.

---

## Поддержанные типы запросов

Примеры (в духе ТЗ):

1) **Сколько всего видео есть в системе?**  
   → `COUNT(*) FROM videos`

2) **Сколько видео у креатора с id <creator_id> вышло с 19 августа 2025 по 17 ноября 2025 включительно?**  
   → `COUNT(*) FROM videos WHERE creator_id = ... AND video_created_at in range`

3) **Сколько видео набрало больше 100000 просмотров за всё время?**  
   → `COUNT(*) FROM videos WHERE views_count > threshold`

4) **На сколько просмотров в сумме выросли все видео 28 ноября 2025?**  
   → `SUM(delta_views_count) FROM video_snapshots WHERE created_at in day`

5) **Сколько разных видео получали новые просмотры 27 ноября 2025?**  
   → `COUNT(DISTINCT video_id) FROM video_snapshots WHERE created_at in day AND delta_views_count > 0`

---

## Запуск (Docker)

### 1) Создать файл `.env`

В папке проекта:

```bash
cp .env.example .env


