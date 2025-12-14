import argparse
import json
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--dsn", required=True, help="postgresql://user:pass@host:port/db")
    p.add_argument("--file", required=True, help="path to videos.json")
    p.add_argument("--batch", type=int, default=500, help="how many videos per flush")
    return p.parse_args()


def read_json(path: Path):
    # Иногда при сохранении/копировании может появиться BOM или странные пробелы.
    text = path.read_text(encoding="utf-8", errors="strict").lstrip("\ufeff")
    data = json.loads(text)
    if isinstance(data, dict) and "videos" in data:
        return data["videos"]
    if isinstance(data, list):
        return data
    raise ValueError("Unexpected JSON format. Expected {'videos': [...]} or [...]")


def flush(cur, videos_rows, snapshots_rows):
    if videos_rows:
        execute_values(
            cur,
            """
            INSERT INTO videos
              (id, creator_id, video_created_at, views_count, likes_count, comments_count, reports_count, created_at, updated_at)
            VALUES %s
            ON CONFLICT (id) DO NOTHING
            """,
            videos_rows,
            page_size=2000,
        )

    if snapshots_rows:
        execute_values(
            cur,
            """
            INSERT INTO video_snapshots
              (id, video_id, views_count, likes_count, comments_count, reports_count,
               delta_views_count, delta_likes_count, delta_comments_count, delta_reports_count,
               created_at, updated_at)
            VALUES %s
            ON CONFLICT (id) DO NOTHING
            """,
            snapshots_rows,
            page_size=5000,
        )


def main():
    args = parse_args()
    file_path = Path(args.file)

    if not file_path.exists():
        raise FileNotFoundError(file_path)

    videos = read_json(file_path)
    print(f"Loaded videos from JSON: {len(videos)}")

    conn = psycopg2.connect(args.dsn)
    conn.autocommit = False

    total_videos = 0
    total_snapshots = 0

    with conn.cursor() as cur:
        videos_rows = []
        snapshots_rows = []

        for v in videos:
            videos_rows.append((
                v["id"],
                v["creator_id"],
                v["video_created_at"],
                int(v["views_count"]),
                int(v["likes_count"]),
                int(v["comments_count"]),
                int(v["reports_count"]),
                v["created_at"],
                v["updated_at"],
            ))

            for s in v.get("snapshots", []):
                snapshots_rows.append((
                    s["id"],
                    s["video_id"],
                    int(s["views_count"]),
                    int(s["likes_count"]),
                    int(s["comments_count"]),
                    int(s["reports_count"]),
                    int(s["delta_views_count"]),
                    int(s["delta_likes_count"]),
                    int(s["delta_comments_count"]),
                    int(s["delta_reports_count"]),
                    s["created_at"],
                    s["updated_at"],
                ))

            if len(videos_rows) >= args.batch:
                flush(cur, videos_rows, snapshots_rows)
                total_videos += len(videos_rows)
                total_snapshots += len(snapshots_rows)
                print(f"Inserted so far (attempted): videos={total_videos}, snapshots={total_snapshots}")
                videos_rows.clear()
                snapshots_rows.clear()

        if videos_rows or snapshots_rows:
            flush(cur, videos_rows, snapshots_rows)
            total_videos += len(videos_rows)
            total_snapshots += len(snapshots_rows)

    conn.commit()
    conn.close()
    print(f"OK: import finished. Attempted insert: videos={total_videos}, snapshots={total_snapshots}")


if __name__ == "__main__":
    main()
