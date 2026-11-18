CREATE TABLE events.clicks_raw (
    ts DateTime, user_id UInt64
) ENGINE = MergeTree() ORDER BY ts
