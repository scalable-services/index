CREATE KEYSPACE history WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true;

CREATE TABLE history.databases (
    id text PRIMARY KEY,
    buf blob,
    num_leaf_entries int,
    num_meta_entries int
) WITH default_time_to_live = 0
    AND transactions = {'enabled': 'false'};

CREATE TABLE history.blocks (
    id text PRIMARY KEY,
    bin blob,
    size int
) WITH default_time_to_live = 0
    AND transactions = {'enabled': 'false'};