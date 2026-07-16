-- 003_fix_strategy_enum.sql
-- One-time recovery for ga_index_state.
--
-- The strategy column was Enum8('block_cursor'=1,'field_cursor'=2,'full_rescan'=3,
-- 'reconcile'=4) -- it declared an unused 'reconcile' and OMITTED 'dual', the strategy
-- of the DUAL-tier entities (avatar, token, trust_relation, guardian_module). The
-- clickhouse-connect driver maps an unknown enum string to raw byte 0
-- (_name_map.get('dual', 0)), so every 'dual' write silently stored byte 0 -- a value
-- with no enum name. Reads that materialize the name throw Code 691 UNKNOWN_ELEMENT_OF_ENUM.
--
-- Fix (ORDER MATTERS): extend the enum so byte 0 is nameable and 'dual' is valid, REPAIR
-- the byte-0 rows to 'dual' WHILE STILL AN ENUM, and only then drop the enum to
-- LowCardinality(String). Converting to String before repairing fails: MODIFY COLUMN
-- does not rewrite parts eagerly, so the lazy Enum8->String cast still hits the unnamed
-- byte 0. Repairing first rewrites those parts (byte 0 -> 'dual') so the later conversion
-- has no unnamed byte left to choke on.
--
-- OPERATIONAL NOTE: long-running writers (the envio-ga-realtime Deployment) cache the
-- table's column types in their clickhouse-connect client at startup. After these type
-- changes they MUST be restarted (the standard deploy rolls the image, which does this)
-- or they will keep encoding strategy as the old Enum8 and re-inject byte-0 rows. Running
-- this migration WITHOUT rolling/restarting the writers will re-corrupt the table.
--
-- Idempotent in outcome (re-running reconverges to LowCardinality(String)). Once every
-- deployed DB is migrated this file can be deleted.

-- 1. Name byte 0 and add 'dual'. Metadata-only Enum8 extension (bytes 1-4 unchanged):
--    reads recover immediately and 'dual' becomes a valid member.
ALTER TABLE ga_index_state
    MODIFY COLUMN strategy Enum8('unknown' = 0, 'block_cursor' = 1, 'field_cursor' = 2, 'full_rescan' = 3, 'reconcile' = 4, 'dual' = 5);

-- 2. Repair byte-0 rows to 'dual' WHILE the column is still an enum. toInt8() reads the
--    raw stored int without name resolution, so byte 0 is safe to match. This rewrites the
--    affected parts with a valid member, eliminating byte 0 before the type conversion.
--    (Mutation ordering guarantees this applies before step 3 on every part.)
ALTER TABLE ga_index_state
    UPDATE strategy = 'dual' WHERE toInt8(strategy) = 0;

-- 3. Drop the enum for strategy. No byte 0 remains, so the Enum8 -> String conversion
--    succeeds for every part.
ALTER TABLE ga_index_state
    MODIFY COLUMN strategy LowCardinality(String);

-- 4. status has no invalid bytes; convert for consistency and to remove the same latent
--    coercion foot-gun.
ALTER TABLE ga_index_state
    MODIFY COLUMN status LowCardinality(String);
