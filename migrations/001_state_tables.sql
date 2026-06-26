-- Single append-only state table (read via FINAL / argMax).
-- The page-level backfill progress AND the per-entity incremental watermark are
-- both derived from this one table (cryo pattern). page_seq is metrics-only;
-- the resume key is max(cursor_end) over completed rows per (entity, partition_key).

CREATE TABLE IF NOT EXISTS ga_index_state (
    entity        String,
    partition_key String DEFAULT '',
    strategy      Enum8('block_cursor' = 1, 'field_cursor' = 2, 'full_rescan' = 3, 'reconcile' = 4),
    page_seq      UInt64 DEFAULT 0,
    cursor_start  String DEFAULT '',
    cursor_end    String DEFAULT '',
    status        Enum8('pending' = 1, 'claimed' = 2, 'completed' = 3, 'failed' = 4, 'dead' = 5),
    backfill_complete UInt8 DEFAULT 0,
    worker_id     String DEFAULT '',
    rows_indexed  UInt32 DEFAULT 0,
    attempt_count UInt16 DEFAULT 0,
    error_message String DEFAULT '',
    created_at    DateTime64(3) DEFAULT now64(3),
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
ORDER BY (entity, partition_key, cursor_start)
PARTITION BY entity;
