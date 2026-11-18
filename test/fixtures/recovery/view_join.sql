CREATE VIEW events.enriched AS
SELECT c.d, c.total, u.name
FROM events.clicks_agg AS c
LEFT JOIN dicts_db.users AS u ON u.id = c.user_id
ARRAY JOIN tags
