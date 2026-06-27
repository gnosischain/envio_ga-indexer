# envio_ga-indexer

Mirrors a Hasura/Envio-HyperIndex GraphQL API (Gnosis Chain — Circles, Metri, Metri Pay,
Gnosis Pay/Cashback, Investment) into an existing managed **ClickHouse** so its entities are
queryable in our analytics stack.

The source is **mutable in place and can delete rows**, and exposes **no change cursor and no
server-side counts**, so the design is built around three correctness invariants (below). The
pipeline writes a **raw JSON audit log** (full mutation history) plus **typed tables** (the
queryable mirror), structured like `beacon-indexer` and reusing `cryo-indexer`'s fix-mode patterns.

## Source reality (probed live)
- Gnosis Chain, `chain_id=100`. Reads use a **Bearer token** (`Authorization: Bearer <token>`).
- **28 entities.** Hard **1000-row cap** on `limit` → pagination is mandatory.
- **Keyset by `id`** works (`where:{id:{_gt:$last}}, order_by:{id:asc}`); ids are opaque, not time-ordered.
- **No `*_aggregate`** (no counts) and **no global changed-since** field.
- Many entities expose a filterable `blockNumber`; some mutable entities expose a monotonic update
  field (`lastStatusUpdate`, `lastCalculated`, …); some have neither.
- BigInt values arrive as `numeric` and **can be negative** → stored as `Int256` (the uint256-max
  sentinel field `TrustRelation.expiryTime` is `UInt256`). Enums → `LowCardinality(String)`.

## Correctness invariants
- **INV-1 (deletes):** an id-keyed append-only mirror can't remove rows. A periodic **reconcile**
  (id-only full keyset walk + id-set diff) tombstones rows that disappeared upstream (`_deleted=1`).
- **INV-2 (cursor choice):** `block_cursor` is used **only** for entities asserted immutable. Mutable
  entities use `field_cursor` (monotonic field) or `full_rescan` / `dual` (periodic full body re-walk),
  because an id-only reconcile catches deletes/inserts but not in-place field updates.
- **INV-3 (partitioning):** typed tables partition only by **immutable** columns — `intDiv(block_number,1e6)`
  for pure block_cursor, else unpartitioned. Never by a mutable field (else an id's versions split
  across partitions and never collapse).

## Sync tiers (28 entities)
- **block_cursor** (immutable, append-only): `transfer`, `transaction`
- **dual** (block discovery + periodic rescan): `avatar`, `token`, `trust_relation`, `guardian_module`
- **field_cursor** (monotonic update field): `avatar_balance`, `avatar_total_balance_v2`, `cashback`,
  `cashback_status_history`, `notification`, `metri_pay_delay_module_owner`, `profile`, `transaction_action`
- **full_rescan** (no usable cursor): `auto_topup`, `circles_backing`, `coordinator_state`,
  `earned_from_invite`, `gnosis_app_user`, `historical_gno_balance`, `investment_account`,
  `metri_balance`, `metri_order`, `metri_pay_delay_module`, `metri_pay_roles_module`,
  `pending_recovery`, `swap`, `v1_token_pending_stop`

Tiers are encoded in `config/entities.yaml` and baked into the registry by `introspect`. Every
deletable entity is also covered by `reconcile`.

## Querying the mirror
Typed tables are `ReplacingMergeTree` keyed by `id` with a tombstone flag.
**Never use `FINAL`** — it forces a heavy merge-on-read and OOMs constrained
instances. Dedup to the latest version per id with `argMax(col, insert_version)
GROUP BY id` and drop tombstones via `HAVING`:
```sql
-- live rows, deduped, no FINAL
SELECT id,
       argMax(value, insert_version)        AS value,
       argMax(block_number, insert_version) AS block_number
       -- ... one argMax per column you need ...
FROM <entity>
GROUP BY id
HAVING argMax(_deleted, insert_version) = 0;

-- live row count
SELECT count() FROM (
  SELECT id FROM <entity> GROUP BY id HAVING argMax(_deleted, insert_version) = 0
);
```
(Background merges still collapse old versions over time, but reads must not rely
on or trigger that with `FINAL`.)

## Layout
```
config/entities.yaml          per-entity tier/cursor/mutable/partition overrides
migrations/                   000_settings, 001_state_tables, 002_raw_entities, 100_typed_entities (GENERATED)
scripts/                      migrate.py, introspect.py (codegen), status.py
src/registry/                 schema.py, generated.py (GENERATED), overrides.py (runtime accessor)
src/services/                 graphql_client.py, clickhouse.py, state.py, loader.py, maintenance.py
src/loaders/                  base.py, graphql_entity.py
src/parsers/generic.py        GraphQL dict -> typed row
src/utils/                    types.py (type map + coercion), logger.py
```

## Setup
```bash
make install                  # python venv + deps
cp .env.example .env          # fill GRAPHQL_API_KEY (Bearer) + CLICKHOUSE_* 
make migrate                  # create state + raw + (generated) typed tables
make introspect               # regenerate registry + typed DDL from live schema, then:
make migrate                  # apply any DDL changes
```

## Commands
```bash
make backfill                          # historical backfill (ENTITIES=a,b CONCURRENCY=4)
make realtime                          # continuous, strategy-routed ingestion
make reconcile                         # INV-1 delete detection (schedule daily)
make status                            # progress overview
make check    ENTITIES=transfer        # report state / gaps / failed pages
make fix      ENTITIES=transfer BLOCK_RANGE=20000000:21000000   # delete-then-reinsert a window
make reset    ENTITIES=swap STATUS=failed                       # requeue failed pages
make reprocess ENTITIES=cashback       # re-derive typed from raw (no API calls)
make test                              # unit tests
```
Direct CLI: `python -m src.main <migrate|introspect|load backfill|load realtime|reconcile|maintain ...|status>`.

## Docker
All profiles target the external managed ClickHouse (no bundled CH):
```bash
docker compose --profile migration  up
docker compose --profile introspect up
docker compose --profile backfill   up
docker compose --profile realtime   up -d
docker compose --profile reconcile  up
```

## How sync works
- **Backfill**: work items (one per entity; pure block_cursor entities are split into block
  sub-chunks) are sharded disjointly across N async workers and walked by id-keyset, resumable from
  the furthest completed cursor. Re-runs are idempotent (changed-only writes via payload hash).
- **Realtime**: each tick reads the head, then per entity — block_cursor/dual walk a
  `[watermark-overlap, head]` block window; field_cursor does a compound `(field,id)` keyset from the
  watermark; full_rescan/dual re-walk on a cadence. Watermarks advance only after durable writes.
- **Reconcile**: id-only full walk → stage live ids → tombstone the disappeared.
- **State**: a single append-only `ga_index_state` table, read via `argMax(col, insert_version)`
  dedup (never `FINAL`).

## Notes
- `raw_entities` has **no TTL** (full mutation history kept forever); set `RAW_TTL_DAYS>0` to add one.
- ClickHouse Cloud is eventually consistent across replicas; count checks use
  `SETTINGS select_sequential_consistency=1` to read the latest state.
- Adding/retiring an entity or field: edit `config/entities.yaml`, run `introspect` (it diffs and
  regenerates), then `migrate`. New entities are not auto-enabled.
