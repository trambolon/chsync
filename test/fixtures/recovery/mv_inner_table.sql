CREATE MATERIALIZED VIEW events.sessions_mv
ENGINE = AggregatingMergeTree() ORDER BY d
AS SELECT toDate(ts) AS d, uniqState(user_id) AS u
FROM events.raw_sessions
GROUP BY d
