CREATE TABLE IF NOT EXISTS videos (
  id UUID PRIMARY KEY,
  creator_id TEXT NOT NULL,
  video_created_at TIMESTAMPTZ NOT NULL,
  views_count BIGINT NOT NULL,
  likes_count BIGINT NOT NULL,
  comments_count BIGINT NOT NULL,
  reports_count BIGINT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS video_snapshots (
  id TEXT PRIMARY KEY,
  video_id UUID NOT NULL REFERENCES videos(id) ON DELETE CASCADE,
  views_count BIGINT NOT NULL,
  likes_count BIGINT NOT NULL,
  comments_count BIGINT NOT NULL,
  reports_count BIGINT NOT NULL,
  delta_views_count BIGINT NOT NULL,
  delta_likes_count BIGINT NOT NULL,
  delta_comments_count BIGINT NOT NULL,
  delta_reports_count BIGINT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_videos_creator_created
  ON videos (creator_id, video_created_at);

CREATE INDEX IF NOT EXISTS idx_videos_created
  ON videos (video_created_at);

CREATE INDEX IF NOT EXISTS idx_snapshots_created
  ON video_snapshots (created_at);

CREATE INDEX IF NOT EXISTS idx_snapshots_video_created
  ON video_snapshots (video_id, created_at);

CREATE INDEX IF NOT EXISTS idx_snapshots_delta_views
  ON video_snapshots (delta_views_count);
