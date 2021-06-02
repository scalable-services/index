CREATE KEYSPACE indexes WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true;

CREATE TABLE indexes.meta (
    id text PRIMARY KEY,
    root text,
    num_leaf_entries int,
    num_meta_entries int
) WITH default_time_to_live = 0
    AND transactions = {'enabled': 'false'};

CREATE TABLE indexes.blocks (
    id text PRIMARY KEY,
    bin blob,
    leaf boolean,
    size bigint
) WITH default_time_to_live = 0
    AND transactions = {'enabled': 'false'};