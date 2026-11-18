CREATE VIEW events.recent AS
SELECT * FROM clicks_raw WHERE ts > now() - INTERVAL 1 DAY
