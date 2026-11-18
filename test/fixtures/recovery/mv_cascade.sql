CREATE MATERIALIZED VIEW events.daily_rollup_mv
TO events.daily_rollup
AS SELECT d, sum(c) AS total
FROM events.clicks_agg
GROUP BY d
