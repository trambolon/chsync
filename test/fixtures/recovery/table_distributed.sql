CREATE TABLE events.clicks_dist ON CLUSTER '{cluster}'
AS events.clicks_local
ENGINE = Distributed('{cluster}', 'events', 'clicks_local', rand())
