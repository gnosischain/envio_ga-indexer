-- Shared raw audit log: append-only, full mutation history, NO TTL.
-- No dedup by id -> captures every observed version of a row (the one thing the
-- mutable upstream API discards on in-place update). Monthly-partitioned so it
-- stays prunable, with retention intentionally unbounded (user decision).

CREATE TABLE IF NOT EXISTS raw_entities (
    entity        LowCardinality(String),
    id            String,
    payload       String,
    payload_hash  String,
    block_number  UInt64 DEFAULT 0,
    observed_at   DateTime DEFAULT now(),
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = MergeTree
ORDER BY (entity, id, insert_version)
PARTITION BY toStartOfMonth(observed_at);
