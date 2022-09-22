CREATE KEYSPACE history WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true;

CREATE TABLE history.databases (
    id text PRIMARY KEY,
    buf blob,
    num_leaf_entries int,
    num_meta_entries int
) WITH default_time_to_live = 0
    AND transactions = {'enabled': 'false'};

CREATE TABLE history.indexes (
    id text PRIMARY KEY,
    buf blob,
    num_leaf_entries int,
    num_meta_entries int
) WITH default_time_to_live = 0
    AND transactions = {'enabled': 'false'};

CREATE TABLE history.blocks (
    partition text,
    id text,
    bin blob,
    size int,
    PRIMARY KEY(partition, id)
) WITH default_time_to_live = 0
    AND transactions = {'enabled': 'false'};