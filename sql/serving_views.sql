-- ===================================================================
-- TikTok Toxicity - Serving Layer Views
-- Merge Batch + Speed Layer data cho Power BI
-- ===================================================================

-- ===================================================================
-- 1. SPEED LAYER TABLES (Real-time data)
-- ===================================================================

CREATE TABLE IF NOT EXISTS speed_video_stats (
    video_id VARCHAR(100),
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    total_comments INT,
    toxic_comments INT,
    hate_comments INT,
    offensive_comments INT,
    clean_comments INT,
    toxic_ratio FLOAT,
    hashtags TEXT[],
    PRIMARY KEY (video_id, window_end)
);

CREATE TABLE IF NOT EXISTS speed_alerts (
    id SERIAL PRIMARY KEY,
    video_id VARCHAR(100),
    window_end TIMESTAMP,
    toxic_ratio FLOAT,
    total_comments INT,
    toxic_comments INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_speed_video_stats_video_id 
    ON speed_video_stats(video_id);
CREATE INDEX IF NOT EXISTS idx_speed_video_stats_window_end 
    ON speed_video_stats(window_end);
CREATE INDEX IF NOT EXISTS idx_speed_alerts_video_id 
    ON speed_alerts(video_id);

CREATE TABLE IF NOT EXISTS speed_hashtag_stats (
    hashtag VARCHAR(200),
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    total_comments INT,
    toxic_comments INT,
    toxic_ratio FLOAT,
    PRIMARY KEY (hashtag, window_end)
);

CREATE TABLE IF NOT EXISTS speed_user_stats (
    user_id VARCHAR(100),
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    total_comments INT,
    toxic_comments INT,
    toxic_ratio FLOAT,
    PRIMARY KEY (user_id, window_end)
);

CREATE INDEX IF NOT EXISTS idx_speed_hashtag_window_end 
    ON speed_hashtag_stats(window_end);
CREATE INDEX IF NOT EXISTS idx_speed_user_window_end 
    ON speed_user_stats(window_end);

-- ===================================================================
-- 2. BATCH LAYER TABLES (Historical data)
-- ===================================================================

CREATE TABLE IF NOT EXISTS batch_video_stats (
    video_id VARCHAR(100) PRIMARY KEY,
    total_comments INT,
    toxic_comments INT,
    hate_comments INT,
    offensive_comments INT,
    clean_comments INT,
    toxic_ratio FLOAT,
    computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS batch_hashtag_stats (
    hashtag VARCHAR(200) PRIMARY KEY,
    total_videos INT,
    total_comments INT,
    toxic_comments INT,
    toxic_ratio FLOAT,
    computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS batch_user_ranking (
    user_id VARCHAR(100) PRIMARY KEY,
    total_comments INT,
    toxic_comments INT,
    toxic_ratio FLOAT,
    computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS batch_user_groups (
    user_id VARCHAR(100) PRIMARY KEY,
    group_topic VARCHAR(200),
    toxic_comment_count INT,
    computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_batch_hashtag_toxic_ratio 
    ON batch_hashtag_stats(toxic_ratio DESC);
CREATE INDEX IF NOT EXISTS idx_batch_user_toxic_ratio 
    ON batch_user_ranking(toxic_ratio DESC);

-- ===================================================================
-- 3. SERVING VIEWS (Lambda Architecture - Merge Batch + Speed)
-- ===================================================================

-- -------------------------------------------------------------------
-- 3.1. Video Statistics View
-- Merge batch + speed, ưu tiên speed layer (dữ liệu mới nhất)
-- -------------------------------------------------------------------
DROP VIEW IF EXISTS serving_video_stats CASCADE;
CREATE OR REPLACE VIEW serving_video_stats AS
SELECT 
    COALESCE(s.video_id, b.video_id) AS video_id,
    COALESCE(s.total_comments, b.total_comments, 0) AS total_comments,
    COALESCE(s.toxic_comments, b.toxic_comments, 0) AS toxic_comments,
    COALESCE(s.hate_comments, b.hate_comments, 0) AS hate_comments,
    COALESCE(s.offensive_comments, b.offensive_comments, 0) AS offensive_comments,
    COALESCE(s.clean_comments, b.clean_comments, 0) AS clean_comments,
    COALESCE(s.toxic_ratio, b.toxic_ratio, 0.0) AS toxic_ratio,
    COALESCE(s.hashtags, ARRAY[]::TEXT[]) AS hashtags,
    GREATEST(
        COALESCE(s.window_end, '1970-01-01'::TIMESTAMP), 
        COALESCE(b.computed_at, '1970-01-01'::TIMESTAMP)
    ) AS last_updated,
    CASE 
        WHEN s.video_id IS NOT NULL THEN 'speed'
        ELSE 'batch'
    END AS data_source
FROM (
    -- Get latest window for each video from speed layer
    SELECT DISTINCT ON (video_id) 
        video_id,
        window_end,
        total_comments,
        toxic_comments,
        hate_comments,
        offensive_comments,
        clean_comments,
        toxic_ratio,
        hashtags
    FROM speed_video_stats
    ORDER BY video_id, window_end DESC
) s
FULL OUTER JOIN batch_video_stats b USING (video_id);

-- -------------------------------------------------------------------
-- 3.2. Hashtag Statistics View
-- Chỉ từ batch layer (vì speed layer không aggregate hashtags)
-- -------------------------------------------------------------------
DROP VIEW IF EXISTS serving_hashtag_stats CASCADE;
CREATE OR REPLACE VIEW serving_hashtag_stats AS
SELECT 
    COALESCE(s.hashtag, b.hashtag) AS hashtag,
    COALESCE(b.total_videos, 0) AS total_videos, -- Speed layer doesn't count videos per hashtag yet
    COALESCE(s.total_comments, b.total_comments, 0) AS total_comments,
    COALESCE(s.toxic_comments, b.toxic_comments, 0) AS toxic_comments,
    COALESCE(s.toxic_ratio, b.toxic_ratio, 0.0) AS toxic_ratio,
    GREATEST(
        COALESCE(s.window_end, '1970-01-01'::TIMESTAMP), 
        COALESCE(b.computed_at, '1970-01-01'::TIMESTAMP)
    ) AS last_updated
FROM (
    SELECT DISTINCT ON (hashtag)
        hashtag,
        window_end,
        total_comments,
        toxic_comments,
        toxic_ratio
    FROM speed_hashtag_stats
    ORDER BY hashtag, window_end DESC
) s
FULL OUTER JOIN batch_hashtag_stats b USING (hashtag)
ORDER BY toxic_ratio DESC;

-- -------------------------------------------------------------------
-- 3.3. User Ranking View
-- Chỉ từ batch layer
-- -------------------------------------------------------------------
DROP VIEW IF EXISTS serving_user_ranking CASCADE;
CREATE OR REPLACE VIEW serving_user_ranking AS
SELECT 
    COALESCE(s.user_id, b.user_id) AS user_id,
    COALESCE(s.total_comments, b.total_comments, 0) AS total_comments,
    COALESCE(s.toxic_comments, b.toxic_comments, 0) AS toxic_comments,
    COALESCE(s.toxic_ratio, b.toxic_ratio, 0.0) AS toxic_ratio,
    GREATEST(
        COALESCE(s.window_end, '1970-01-01'::TIMESTAMP), 
        COALESCE(b.computed_at, '1970-01-01'::TIMESTAMP)
    ) AS last_updated
FROM (
    SELECT DISTINCT ON (user_id)
        user_id,
        window_end,
        total_comments,
        toxic_comments,
        toxic_ratio
    FROM speed_user_stats
    ORDER BY user_id, window_end DESC
) s
FULL OUTER JOIN batch_user_ranking b USING (user_id)
ORDER BY toxic_ratio DESC;

-- -------------------------------------------------------------------
-- 3.3.1. Toxic User Groups View
-- Chỉ từ batch layer
-- -------------------------------------------------------------------
DROP VIEW IF EXISTS serving_toxic_user_groups CASCADE;
CREATE OR REPLACE VIEW serving_toxic_user_groups AS
SELECT 
    group_topic,
    COUNT(user_id) as user_count,
    SUM(toxic_comment_count) as total_toxic_comments,
    string_agg(user_id, ', ') as member_sample
FROM batch_user_groups
GROUP BY group_topic
ORDER BY total_toxic_comments DESC;

-- -------------------------------------------------------------------
-- 3.4. Real-time Alerts View
-- Chỉ từ speed layer (real-time alerts)
-- -------------------------------------------------------------------
CREATE OR REPLACE VIEW serving_alerts AS
SELECT 
    id,
    video_id,
    window_end,
    toxic_ratio,
    total_comments,
    toxic_comments,
    created_at
FROM speed_alerts
ORDER BY created_at DESC;

-- -------------------------------------------------------------------
-- 3.5. Top Toxic Videos View (for dashboard)
-- -------------------------------------------------------------------
CREATE OR REPLACE VIEW serving_top_toxic_videos AS
SELECT 
    video_id,
    total_comments,
    toxic_comments,
    toxic_ratio,
    last_updated,
    data_source
FROM serving_video_stats
WHERE total_comments >= 10  -- Filter videos with significant comments
ORDER BY toxic_ratio DESC, total_comments DESC
LIMIT 100;

-- -------------------------------------------------------------------
-- 3.6. Recent Activity Summary (Last 24h from speed layer)
-- -------------------------------------------------------------------
CREATE OR REPLACE VIEW serving_recent_activity AS
SELECT 
    COUNT(DISTINCT video_id) AS total_videos_processed,
    SUM(total_comments) AS total_comments,
    SUM(toxic_comments) AS total_toxic_comments,
    ROUND(AVG(toxic_ratio)::NUMERIC, 3) AS avg_toxic_ratio,
    MAX(window_end) AS last_update_time
FROM speed_video_stats
WHERE window_end >= NOW() - INTERVAL '24 hours';

-- ===================================================================
-- 4. MATERIALIZED VIEWS (Optional - for better performance)
-- ===================================================================

-- Materialized view for dashboard aggregations
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_daily_toxicity_summary AS
SELECT 
    DATE(last_updated) AS report_date,
    COUNT(*) AS total_videos,
    SUM(total_comments) AS total_comments,
    SUM(toxic_comments) AS total_toxic_comments,
    ROUND(AVG(toxic_ratio)::NUMERIC, 3) AS avg_toxic_ratio,
    MAX(toxic_ratio) AS max_toxic_ratio
FROM serving_video_stats
GROUP BY DATE(last_updated)
ORDER BY report_date DESC;

-- Create index on materialized view
CREATE INDEX IF NOT EXISTS idx_mv_daily_summary_date 
    ON mv_daily_toxicity_summary(report_date DESC);

-- ===================================================================
-- 5. HELPER FUNCTIONS
-- ===================================================================

-- Function to refresh materialized views
CREATE OR REPLACE FUNCTION refresh_serving_views()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW mv_daily_toxicity_summary;
    RAISE NOTICE 'Materialized views refreshed successfully';
END;
$$ LANGUAGE plpgsql;

-- ===================================================================
-- 6. GRANTS (for Power BI read-only user)
-- ===================================================================

-- Create read-only user for Power BI
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_user WHERE usename = 'powerbi_reader') THEN
        CREATE USER powerbi_reader WITH PASSWORD 'powerbi_read123';
    END IF;
END
$$;

-- Grant permissions
GRANT CONNECT ON DATABASE tiktok_toxicity TO powerbi_reader;
GRANT USAGE ON SCHEMA public TO powerbi_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO powerbi_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA public 
    GRANT SELECT ON TABLES TO powerbi_reader;

-- ===================================================================
-- DONE
-- ===================================================================

