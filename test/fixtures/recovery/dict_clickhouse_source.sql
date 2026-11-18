CREATE DICTIONARY events.geo_dict (
    id UInt64, country String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 DB 'refdata' TABLE 'geo' PASSWORD 'p(ss)word'))
LIFETIME(MIN 300 MAX 600)
LAYOUT(HASHED())
