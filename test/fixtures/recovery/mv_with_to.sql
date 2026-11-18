CREATE MATERIALIZED VIEW events.clicks_mv ON CLUSTER '{cluster}'
TO events.clicks_agg
AS SELECT toDate(ts) AS d, count() AS c
FROM events.clicks_raw
GROUP BY d
