CREATE TABLE events.orders (
    id UInt64,
    geo_id UInt64,
    country String DEFAULT dictGetString('events.geo_dict', 'country', geo_id),
    -- FROM inside a comment must not create a dep: FROM fake.table
    note String DEFAULT 'copied FROM fake.other'
) ENGINE = MergeTree() ORDER BY id
