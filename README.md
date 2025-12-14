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

Запуск (Docker)
Шаг 1. Перейти в папку проекта

Открой терминал и перейди в папку:

tg-video-analytics-bot

(если ты в корне репозитория, то просто открой папку tg-video-analytics-bot в IDE или перейди в неё командой cd)

Шаг 2. Создать файл .env

В папке tg-video-analytics-bot рядом с .env.example нужно создать файл .env.

Самый простой способ:

скопируй файл .env.example

вставь рядом

переименуй копию в .env

Дальше открой .env и обязательно укажи минимум:

BOT_TOKEN=... (токен от @BotFather)

Остальные переменные можно оставить как в .env.example.

Важно: файл .env не должен попадать в публичный репозиторий.

Шаг 3. Поднять контейнеры (PostgreSQL + бот)

Запусти сборку и старт контейнеров командой:

docker compose up -d --build

Проверить, что контейнеры поднялись:

docker compose ps

Шаг 4. Проверить, что таблицы созданы

Схема БД создаётся автоматически при старте Postgres (см. файл sql/001_init.sql).

Проверка таблиц:

docker compose exec db psql -U postgres -d video_analytics -c "\dt"

Должны быть таблицы: videos, video_snapshots.

Импорт данных из JSON
Шаг 1. Скачать JSON

Скачай файл videos.json (из задания) и положи его, например, сюда:

C:\data\videos.json

(путь может быть любой, главное — чтобы ты его знал)

Шаг 2. Запустить импорт

Команда запускается на хосте (на твоём компьютере), из папки tg-video-analytics-bot.

Пример для Windows:

python .\scripts\load_json.py --dsn "postgresql://postgres:postgres@localhost:5432/video_analytics" --file "C:\data\videos.json"

После выполнения скрипт выведет что-то вроде:

Loaded videos from JSON: ...
OK: import finished. Attempted insert: videos=..., snapshots=...

Шаг 3. Проверить количество видео в базе

docker compose exec db psql -U postgres -d video_analytics -c "SELECT COUNT(*) FROM videos;"

Проверка работы бота

Открой бота в Telegram и отправь запрос, например:

Сколько всего видео есть в системе?

Бот должен вернуть одно число.

Автопроверка (служебный бот)

Для автоматической проверки через @rlt_test_checker_bot отправь команду:

/check @tbot_analytics_videos_viktor_bot https://github.com/KinzelVA/Tbot_test_analitic_videos

Важно: во время проверки твой бот должен быть запущен и доступен.



