CREATE MATERIALIZED VIEW events.import_mv
TO events.imported
AS SELECT * FROM remote('ch-old:9000', legacy, clicks)
