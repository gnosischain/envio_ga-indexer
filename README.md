# envio_ga-indexer

Mirrors a Hasura / Envio-HyperIndex GraphQL API (Gnosis Chain — Circles, Metri, Metri Pay,
Gnosis Pay / Cashback, Investment) into a managed **ClickHouse** database so its 28 entities are
queryable in our analytics stack alongside the cerebro/dbt models.

The source is the only place this data lives, it is **mutable in place and can delete rows**, and it
exposes **no change cursor, no row counts, and no delete signal**. Those constraints shape the entire
design (see [Why it works this way](#why-it-works-this-way)). The indexer keeps ClickHouse in sync
through four modes — **backfill**, **realtime**, **reconcile**, and **maintain** — and keeps a raw
JSON audit log so typed tables can be re-derived without re-hitting the API.

---

## Table of contents
- [Quick start](#quick-start)
- [Architecture](#architecture)
- [Repository structure](#repository-structure)
- [The source (verified reality)](#the-source-verified-reality)
- [Why it works this way (correctness invariants)](#why-it-works-this-way)
- [Entities & sync tiers](#entities--sync-tiers)
- [ClickHouse schema](#clickhouse-schema)
- [Type mapping](#type-mapping)
- [Running modes (CLI)](#running-modes-cli)
- [Configuration](#configuration)
- [Querying the mirror](#querying-the-mirror)
- [Deployment & operations](#deployment--operations)
- [Recovery & troubleshooting](#recovery--troubleshooting)
- [Observability](#observability)
- [Development](#development)

---

## Quick start

```bash
make install                 # create .venv and install requirements
cp .env.example .env         # fill GRAPHQL_API_KEY (Bearer) + CLICKHOUSE_* (see Configuration)

make migrate                 # create state + raw + typed tables in ClickHouse
make introspect              # regenerate registry + typed DDL from the live GraphQL schema
make migrate                 # apply any newly generated/changed typed tables

make backfill                # one-time historical load of all enabled entities (resumable)
make realtime                # continuous ingestion (run as a long-lived service)
make reconcile               # delete-detection sweep (or `reconcile --loop` as a service)
make status                  # progress overview
```

Direct CLI form: `python -m src.main <command>` (see [Running modes](#running-modes-cli)).

---

## Architecture

```
            ┌──────────────────────────── GraphQL source (Hasura / Envio) ────────────────────────────┐
            │  28 entity list-queries · keyset by id · 1000-row cap · Bearer auth · no counts/deletes   │
            └───────────────────────────────────────────┬──────────────────────────────────────────────┘
                                                         │  GraphQLClient (aiohttp, token-bucket RPS, retry)
                                  ┌──────────────────────┼───────────────────────┐
                                  │                      │                       │
                            LoaderService          LoaderService          MaintenanceService
                            .backfill()            .realtime()            .reconcile()/.fix()/...
                                  │                      │                       │
                                  ▼                      ▼                       ▼
                  GraphQLEntityLoader.write_rows()  (parse + hash-skip unchanged)
                                  │
                 ┌────────────────┴───────────────────────────────┐
                 ▼                                                 ▼
        raw_entities (append-only audit log)           <entity> typed tables (ReplacingMergeTree)
                 │                                                 ▲
                 └──────────────  maintain reprocess  ─────────────┘   (re-derive typed from raw, no API)

   Progress/cursors/watermarks  →  ga_index_state (one append-only table, argMax dedup, never FINAL)
   Codegen: scripts/introspect.py  →  src/registry/generated.py  +  migrations/100_typed_entities.sql
```

- One **generic** loader + parser drive all 28 entities from an `EntitySpec` registry — adding an
  entity is a config edit + regenerated migration, never new Python.
- Every fetched row is written to a permanent **raw JSON audit log** (the one thing the mutable
  source destroys on in-place update) and parsed into a **typed table** in the same pass.
- All progress (backfill pages, realtime watermarks, rescan markers) lives in a single
  `ga_index_state` table read with `argMax` dedup.

---

## Repository structure

```
envio_ga-indexer/
├── README.md  Makefile  requirements.txt  requirements-dev.txt
├── Dockerfile  docker-compose.yml  docker-entrypoint.sh  .env.example
│
├── config/
│   └── entities.yaml            # per-entity overrides: tier, cursor_field, mutable, deletable,
│                                #   partition/order_by/indexes, rescan_interval, enabled
├── migrations/
│   ├── 000_settings.sql         # placeholder (no special server settings needed)
│   ├── 001_state_tables.sql     # ga_index_state (single append-only progress table)
│   ├── 002_raw_entities.sql     # raw_entities (append-only JSON audit log, no TTL)
│   └── 100_typed_entities.sql   # GENERATED — one ReplacingMergeTree table per entity
├── scripts/
│   ├── migrate.py               # run *.sql migrations (split on ';', strip comments)
│   ├── introspect.py            # GraphQL __schema -> generated.py + 100_typed_entities.sql (+ drift diff)
│   └── status.py                # progress overview (wraps cli.print_status)
└── src/
    ├── main.py                  # entry point: `python -m src.main ...`
    ├── cli.py                   # argparse verbs + dispatch + status/check printers
    ├── config.py                # all env vars (python-dotenv)
    ├── observability.py         # Prometheus metrics + /metrics /health server (envio_ga_*)
    ├── registry/
    │   ├── schema.py            # EntitySpec / FieldSpec dataclasses + SyncStrategy enum
    │   ├── generated.py         # GENERATED registry (list[EntitySpec]) — do not hand-edit
    │   └── overrides.py         # runtime accessor: all_specs() / get_spec() + ENABLED_ENTITIES filter
    ├── services/
    │   ├── graphql_client.py    # async client: Bearer auth, retry/backoff, RPS limiter, keyset builders
    │   ├── clickhouse.py        # connection + pooled inserts (retry on mem/parts errors), reads
    │   ├── state.py             # GaStateManager: page lifecycle, watermarks, completeness (argMax)
    │   ├── loader.py            # LoaderService: backfill + realtime (strategy-routed)
    │   └── maintenance.py       # MaintenanceService: reconcile / check / fix / reset / reprocess
    ├── loaders/
    │   ├── base.py              # calculate_payload_hash + canonical_json
    │   └── graphql_entity.py    # GraphQLEntityLoader: write raw audit + typed in one pass
    ├── parsers/
    │   └── generic.py           # GenericEntityParser: GraphQL dict -> typed row (coercion, sentinels)
    └── utils/
        ├── types.py             # GraphQL scalar -> ClickHouse type map + value coercion + clamping
        └── logger.py            # structlog setup
```

---

## The source (verified reality)

Probed live; these facts drive the design:

- **Gnosis Chain**, `chain_id = 100`, head ≈ 46.9M blocks. Reads require a **Bearer token**
  (`Authorization: Bearer <token>`).
- **28 entities** exposed as list queries (plus `*_by_pk`, `_meta`, `chain_metadata`, empty
  `raw_events` — all skipped by codegen).
- **Hard 1000-row cap** on `limit` → pagination is mandatory.
- **Keyset by `id` works**: `where:{id:{_gt:$last}}, order_by:{id:asc}, limit:1000`. Ids are opaque,
  case-sensitive strings, **not** time/block-ordered.
- **No `*_aggregate`** (no server-side counts), **no `db_write_timestamp`/changed-since cursor**, and
  **no delete signal** (no audit log, `raw_events` empty).
- Many entities expose a filterable `blockNumber` (`where:{blockNumber:{_gte,_lt}}`); block range
  ≈ 11.2M → head. Enables block sub-chunking for the big tables.
- `numeric` (BigInt) values can be **negative** and occasionally hit the uint256-max sentinel.
- Enums are scalar custom types; `Transfer.participants`/`extraData` are scalar JSON strings.

---

## Why it works this way

Three non-negotiable invariants for mirroring a **mutable + deletable + cursorless** source:

- **INV-1 (deletes).** An id-keyed append-only ReplacingMergeTree can insert and update but never
  *remove* a row. Deletes are detected only by enumerating the live id set and diffing it against
  ClickHouse, then writing a soft-delete tombstone (`_deleted=1`). Because the source gives no delete
  signal, that id-walk is the *only* mechanism — so it is **opt-in** (`deletable`, default false) and
  runs as the separate `reconcile` job over the small mutable entities only.
- **INV-2 (cursor choice).** `where blockNumber > watermark` only re-fetches a row whose blockNumber
  *increases*; a mutable row whose fields change in place but whose creation block is fixed is never
  re-fetched. So `block_cursor` is correct **only** for entities asserted immutable; mutable entities
  use `field_cursor` (a monotonic update field) or `full_rescan`/`dual` (periodic full re-walk).
- **INV-3 (partitioning).** ReplacingMergeTree dedups only **within a partition**, so `PARTITION BY`
  must be a pure function of **immutable** fields, else two versions of an id split across partitions
  and never collapse. Pure block_cursor partitions by `intDiv(block_number,1e6)` (immutable creation
  block); everything else is unpartitioned (id-keyed dedup is partition-safe).

And one hard rule learned operationally: **never use `FINAL`** — it forces a heavy merge-on-read and
OOMs the instance. All dedup is done with `argMax(col, insert_version) GROUP BY id`.

---

## Entities & sync tiers

Each entity is assigned a **sync tier** that determines how realtime keeps it current. Tiers (and all
other per-entity settings) live in `config/entities.yaml` and are baked into `src/registry/generated.py`
by `introspect`.

| Tier | Meaning | Realtime behavior |
|---|---|---|
| `block_cursor` | immutable, append-only, block-stamped | page `blockNumber >= watermark - overlap`, advance after durable write |
| `field_cursor` | mutable/append-only with a monotonic field set at creation | compound `(field, id)` keyset from `watermark - overlap` |
| `dual` | block-bearing but mutable | block_cursor discovery **+** periodic full rescan (rescan is the correctness guarantee) |
| `full_rescan` | mutable, no usable cursor | periodic full keyset re-walk every `rescan_interval_s` |

The 28 entities (`del` = delete-checked by reconcile; see [INV-1](#why-it-works-this-way)):

| Entity | Tier | Cursor field | del | Partition (typed table) |
|---|---|---|---|---|
| transfer | block_cursor | blockNumber | – | `intDiv(block_number, 1000000)` |
| transaction | block_cursor | blockNumber | – | `intDiv(block_number, 1000000)` |
| avatar | dual | blockNumber | – | unpartitioned |
| token | dual | blockNumber | – | unpartitioned |
| trust_relation | dual | blockNumber | – | unpartitioned |
| guardian_module | dual | blockNumber | – | unpartitioned |
| avatar_balance | field_cursor | lastCalculated | ✓ | unpartitioned |
| avatar_total_balance_v2 | field_cursor | lastPendingUpdate | ✓ | unpartitioned |
| cashback | field_cursor | lastStatusUpdate | ✓ | unpartitioned |
| profile | field_cursor | lastUpdatedBlockNumber | ✓ | unpartitioned |
| cashback_status_history | field_cursor | timestamp | – | unpartitioned |
| notification | field_cursor | timestamp | – | unpartitioned |
| metri_pay_delay_module_owner | field_cursor | timestamp | – | unpartitioned |
| transaction_action | field_cursor | timestamp | – | `toStartOfMonth(toDateTime(timestamp))`, `ORDER BY (avatar_id, timestamp, id)` + skip-indexes |
| auto_topup | full_rescan | – | ✓ | unpartitioned |
| circles_backing | full_rescan | – | ✓ | unpartitioned |
| coordinator_state | full_rescan | – | ✓ | unpartitioned |
| earned_from_invite | full_rescan | – | ✓ | unpartitioned |
| gnosis_app_user | full_rescan | – | ✓ | unpartitioned |
| historical_gno_balance | full_rescan | – | ✓ | unpartitioned |
| investment_account | full_rescan | – | ✓ | unpartitioned |
| metri_balance | full_rescan | – | ✓ | unpartitioned |
| metri_order | full_rescan | – | ✓ | unpartitioned |
| metri_pay_delay_module | full_rescan | – | ✓ | unpartitioned |
| metri_pay_roles_module | full_rescan | – | ✓ | unpartitioned |
| pending_recovery | full_rescan | – | ✓ | unpartitioned |
| swap | full_rescan | – | ✓ | unpartitioned |
| v1_token_pending_stop | full_rescan | – | ✓ | unpartitioned |

Approximate scale (high-volume tables): `transaction_action` ~200M, `transfer` ~108M,
`transaction` ~36M, `trust_relation` ~1.4M; everything else is small-to-mid (≤ ~250k).

`del` is left blank (deletable=false) for the huge append-only logs and the large dual entities —
they don't delete (chain history / update-in-place), so reconcile never walks them.

---

## ClickHouse schema

Everything lives in the `envio_ga` database (the connection's grants are scoped to `envio_ga.*`).
Three kinds of table: one **state** table, one **raw audit** table, and 28 **typed** tables.

### 1. State — `ga_index_state` (`migrations/001_state_tables.sql`)

A single append-only `ReplacingMergeTree(insert_version)` table holding ALL progress: backfill page
status, realtime watermarks (`partition_key='realtime'`), and rescan markers (`partition_key='rescan'`).
Reads dedup the latest row per `(entity, partition_key, cursor_start)` with `argMax` (never FINAL).

```sql
CREATE TABLE ga_index_state (
    entity        String,
    partition_key String DEFAULT '',          -- '' keyset walk | 'block:lo-hi' sub-chunk | realtime | rescan
    strategy      Enum8('block_cursor'=1,'field_cursor'=2,'full_rescan'=3,'reconcile'=4),
    page_seq      UInt64 DEFAULT 0,            -- metrics only; not the resume key
    cursor_start  String DEFAULT '',           -- the id/block we paginate from
    cursor_end    String DEFAULT '',           -- last cursor of the page (resume key = max completed cursor_end)
    status        Enum8('pending'=1,'claimed'=2,'completed'=3,'failed'=4,'dead'=5),
    backfill_complete UInt8 DEFAULT 0,          -- 1 on a chunk's terminal page
    worker_id, rows_indexed, attempt_count, error_message, created_at ...,
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
ORDER BY (entity, partition_key, cursor_start)
PARTITION BY entity;
```

Derived facts: `watermark(entity)` = `argMax(cursor_end, page_seq)` over completed rows;
`is_backfill_complete(entity)` = **min over chunks** of (max `backfill_complete` per chunk) — so an
entity is complete only when *every* block sub-chunk finished.

### 2. Raw audit log — `raw_entities` (`migrations/002_raw_entities.sql`)

One shared, **append-only** table (no dedup, **no TTL**) that captures every observed version of every
row — the mutation history the API discards on in-place update. Writes are skipped when the
`payload_hash` is unchanged, so it only grows on real changes. Used by `maintain reprocess` to
re-derive typed tables without API calls.

```sql
CREATE TABLE raw_entities (
    entity        LowCardinality(String),
    id            String,
    payload       String,                       -- full JSON row from GraphQL
    payload_hash  String,                        -- sha256[:16]
    block_number  UInt64 DEFAULT 0,
    observed_at   DateTime DEFAULT now(),
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = MergeTree
ORDER BY (entity, id, insert_version)
PARTITION BY toStartOfMonth(observed_at);
```

### 3. Typed tables — `migrations/100_typed_entities.sql` (GENERATED)

One `ReplacingMergeTree` per entity, keyed by `id`, with a soft-delete tombstone. Every table ends
with the same trailer:

```sql
CREATE TABLE <entity> (
    `id` String,
    ... one column per scalar/FK field (snake_case, sentinel DEFAULTs) ...,
    `_deleted`       UInt8  DEFAULT 0,           -- INV-1 tombstone (reads filter this out)
    `_seen_version`  UInt64 DEFAULT 0,           -- reserved: last reconcile that observed the id
    `ingested_at`    DateTime DEFAULT now(),
    `_synced_block`  UInt64 DEFAULT 0,           -- chain block at sync time (if known)
    `insert_version` UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))   -- ReplacingMergeTree version
) ENGINE = ReplacingMergeTree(insert_version)
ORDER BY (id)                                    -- or a tuned key, e.g. transaction_action
PARTITION BY <immutable expr or none>;
```

A re-ingested `id` writes a new row with a larger `insert_version`; `argMax` reads keep the newest.
Re-runs are therefore idempotent. `transaction_action` is tuned for read performance:
`PARTITION BY toStartOfMonth(toDateTime(timestamp))`, `ORDER BY (avatar_id, timestamp, id)`, plus a
`bloom_filter` on `transaction_id` and a `minmax` on `timestamp` — set per-entity in `entities.yaml`
(`order_by` / `indexes` / `ts_immutable`).

---

## Type mapping

`src/utils/types.py` maps GraphQL scalars to ClickHouse columns (sentinels, not `Nullable`):

| GraphQL | ClickHouse | Notes |
|---|---|---|
| `ID` / `String` / text | `String` `DEFAULT ''` | the ORDER BY key |
| `Int` (block field) | `UInt64` | block numbers |
| `Int` (other) | `Int64` | timestamps (unix secs), counts, indices |
| `numeric` / `bigint` | `Int256` `DEFAULT 0` | balances/values **can be negative**; covers all real magnitudes |
| (uint256-max sentinel, e.g. `expiryTime`) | `UInt256` | overrides via `field_types` in `entities.yaml` |
| `Boolean` | `Bool` `DEFAULT false` | |
| `Float` | `Float64` | |
| enum scalar | `LowCardinality(String)` `DEFAULT ''` | new upstream enum values need no migration |
| `jsonb` / `json` | `String` | stored as JSON text |
| FK relation `foo: Bar` | `foo_id String` | scalar FK captured; reverse list relations not stored |

Values are coerced and **range-clamped** on insert so a stray out-of-range value can never abort a
batch. Negatives default to `Int256`; only fields that use the uint256-max sentinel are `UInt256`.

---

## Running modes (CLI)

`python -m src.main <command>` (Makefile targets wrap the common ones).

### `migrate`
Runs `migrations/*.sql` in order against `envio_ga` (`CREATE TABLE IF NOT EXISTS`, idempotent).

### `introspect`
Runs GraphQL `__schema`, regenerates `src/registry/generated.py` and `migrations/100_typed_entities.sql`,
applies `config/entities.yaml` overrides, enforces codegen guardrails (INV-2/INV-3), and prints a
**drift diff** vs the committed registry (new/removed entities & fields). Re-run `migrate` afterward
to apply DDL changes (additive only; new entities are not auto-enabled).

### `load backfill [--entities a,b] [--page-size N] [--concurrency K] [--restart]`
One-time historical load. Builds work items (one per entity; pure `block_cursor` entities are split
into `block:lo-hi` sub-chunks of `BACKFILL_BLOCK_CHUNK`), shards them disjointly across `K` async
workers, and walks each by id-keyset. Writes raw + typed in batches (~25k rows/insert). **Resumable**
from the furthest completed `cursor_end`; idempotent. `--restart` clears an entity's backfill state to
re-walk from scratch.

### `load realtime [--entities a,b] [--poll-interval S]`
Continuous, strategy-routed sync (one long-lived process). Each tick reads the head, then per entity:
`block_cursor`/`dual` walk a `[watermark - overlap, head]` block window; `field_cursor` does a
compound `(field,id)` keyset from the watermark; `full_rescan`/`dual` re-walk fully every
`rescan_interval_s`. Watermarks advance only after durable writes; unchanged rows are hash-skipped.

### `reconcile [--entities a,b] [--loop]`
Delete-detection (INV-1) for `deletable` entities only. Per entity: full **id-only** keyset walk into
a staging table, then tombstone (`_deleted=1`) live CH ids absent from the source. **Recovery-safe**
(tombstones only after a provably complete walk) and **race-safe** (an `insert_version` fence exempts
rows a concurrent `realtime` touched mid-walk). `--loop` runs it repeatedly every `RECONCILE_INTERVAL_S`
as a standalone service. Non-deletable entities (the huge append-only tables) are skipped.

### `maintain check [--entities a,b]`
Read-only health report: source head, per-entity `COMPLETE/INCOMPLETE` (min-over-chunks), the `[del]`
flag, per-block-chunk coverage for block entities (lists incomplete/missing chunks), keyset gaps, and
failed/dead/stuck pages. This is the honest source of truth — never trust a single flag.

### `maintain fix [--entities a,b] [--id-range F:T | --block-range LO:HI] [--dry-run]`
Recovers stuck pages, re-queues failed/dead pages, and re-drives incomplete backfills. With
`--block-range`, does **delete-then-reinsert** of that window (the reorg-repair path for append-only
block tables).

### `maintain reset [--entities a,b] [--status failed|claimed]`
Requeues pages of a given status, or (no status) clears an entity's backfill state for a full re-walk.

### `maintain reprocess [--entities a,b]`
Re-derives typed tables from `raw_entities` (parse fix / schema change) with **no API calls**.

### `status`
Compact progress table: per-entity backfill complete/partial, pages, rows, and live row count
(`argMax` dedup, FINAL-free).

---

## Configuration

All via environment / `.env` (`src/config.py`). See `.env.example`.

| Variable | Default | Purpose |
|---|---|---|
| `GRAPHQL_ENDPOINT` | — | source URL |
| `GRAPHQL_API_KEY` | — | Bearer token |
| `GRAPHQL_AUTH_HEADER` / `GRAPHQL_AUTH_SCHEME` | `Authorization` / `Bearer` | header `Authorization: Bearer <token>` |
| `PAGE_SIZE` | `1000` | rows per request (hard server cap = 1000) |
| `BACKFILL_CONCURRENCY` | `4` | disjoint backfill workers (use 1–2 on a small CH instance) |
| `BACKFILL_BLOCK_CHUNK` | `5000000` | block-window size for block_cursor sub-chunking |
| `GQL_MAX_RPS` | `10` | shared token-bucket rate limit to the Hasura |
| `GQL_MAX_RETRIES` | `5` | retry attempts (429/5xx/timeout, exponential + jitter) |
| `GQL_TIMEOUT` | `120` | per-request timeout (s) |
| `ENABLED_ENTITIES` | (all) | comma list to restrict which entities run |
| `POLL_INTERVAL` | `30` | realtime tick (s) |
| `REALTIME_OVERLAP_BLOCKS` | `5` | re-read boundary blocks (reorg/tie safety) |
| `RESCAN_INTERVAL_S` | `300` | full_rescan / dual rescan cadence (overridable per entity) |
| `RECONCILE_INTERVAL_S` | `86400` | `reconcile --loop` cadence |
| `CLICKHOUSE_HOST/PORT/USER/PASSWORD/DATABASE/SECURE` | — | managed instance (HTTP 8443/443 secure) |
| `CLICKHOUSE_TIMEOUT` | `120` | send/receive timeout (s); short so hangs fail fast and retry |
| `RAW_TTL_DAYS` | `0` | `0` = keep raw forever; positive adds a partition TTL |
| `METRICS_ENABLED` / `METRICS_PORT` | `true` / `9090` | Prometheus `/metrics` + `/health` |
| `LOG_LEVEL` | `INFO` | (`FORCE_JSON_LOGS=true` for JSON logs) |

Per-entity overrides live in `config/entities.yaml`: `tier`, `cursor_field`, `mutable`, `deletable`,
`ts_field`/`ts_immutable`, `partition`, `order_by`, `indexes`, `field_types`, `rescan_interval_s`,
`enabled`. Edit it, run `introspect`, then `migrate`.

---

## Querying the mirror

Typed tables are `ReplacingMergeTree` keyed by `id` with a tombstone. **Never use `FINAL`** (it forces
a heavy merge-on-read). Dedup to the latest version per id with `argMax(col, insert_version)` and drop
tombstones via `HAVING`:

```sql
-- live rows, deduped (one argMax per column you need)
SELECT id,
       argMax(value, insert_version)        AS value,
       argMax(block_number, insert_version) AS block_number,
       argMax(transfer_type, insert_version) AS transfer_type
FROM transfer
WHERE transfer_type = 'PayTopUp' AND block_number >= 46000000   -- partition pruning
GROUP BY id
HAVING argMax(_deleted, insert_version) = 0;

-- live row count
SELECT count() FROM (
  SELECT id FROM <entity> GROUP BY id HAVING argMax(_deleted, insert_version) = 0
);
```

On the huge tables (`transfer`, `transaction`, `transaction_action`) always **scope by time/avatar/
block** so partition pruning kicks in; unbounded full-table aggregates are memory-heavy on a small
instance. (Background merges also collapse old versions over time, but reads must not depend on or
trigger that via `FINAL`.) For ClickHouse Cloud read-after-write consistency, append
`SETTINGS select_sequential_consistency = 1` to verification counts.

---

## Deployment & operations

All Docker profiles target the **external** managed ClickHouse (no bundled CH container):

```bash
docker compose --profile migration  up      # migrate
docker compose --profile introspect up      # regenerate registry + DDL
docker compose --profile backfill   up      # one-time historical load
docker compose --profile realtime   up -d   # continuous sync (restart: unless-stopped)
docker compose --profile reconcile  up -d   # reconcile --loop (restart: unless-stopped)
docker compose --profile maintenance run --rm maintenance maintain check
```

Steady-state runs **two long-lived services**: `realtime` (sync) and `reconcile --loop` (deletes).
Both are resumable and safe to restart.

Operational notes:
- Run long jobs under a process supervisor (Docker `restart: unless-stopped`, k8s Deployment/CronJob,
  systemd, or `tmux`/`nohup`). Do **not** rely on a session-tied shell — a suspended session reaps it.
  (`setsid` is unavailable on macOS; use `nohup` or a container.)
- On a small ClickHouse instance keep `BACKFILL_CONCURRENCY` low (1–2); inserts batch ~25k rows with
  `optimize_on_insert=0` and retry on memory/too-many-parts errors.
- Deciding `deletable` for an entity: **measure then flip** — run `reconcile --entities X` a few times;
  if it tombstones `>0` rows it deletes upstream → set `deletable: true` and re-run `introspect`;
  otherwise leave it `false`. `maintain check` shows the `[del]` flag.

---

## Recovery & troubleshooting

- **Resume after a crash/kill:** just re-run the same command. Backfill resumes from the furthest
  completed `cursor_end`; realtime resumes from its watermark; reconcile rebuilds its stage. Idempotent
  throughout (id-keyed ReplacingMergeTree).
- **"Less data than expected":** run `maintain check` — it shows per-chunk coverage and flags
  incomplete/missing block chunks. Re-run `load backfill --entities X` to finish them.
- **Memory errors (code 241 / too-many-parts):** inserts already retry with backoff; lower
  `BACKFILL_CONCURRENCY`, or size up the ClickHouse tier for the 100M+ tables.
- **A reorged-out tx on an append-only table:** `maintain fix --entities transfer --block-range LO:HI`
  (delete-then-reinsert); widen `REALTIME_OVERLAP_BLOCKS` if reorgs are deeper than the overlap.
- **Schema drift (upstream added/removed a field/entity):** `introspect` prints the diff and emits
  additive DDL; review, then `migrate`. New entities require an explicit `enabled` before they run.
- **Re-derive a typed table without re-hitting the API:** `maintain reprocess --entities X`.

---

## Observability

`src/observability.py` serves Prometheus metrics on `:9090/metrics` and a `/health` endpoint, all
namespaced `envio_ga_*` and labelled by `entity`:

- `envio_ga_graphql_requests_total{operation,status}`, `envio_ga_graphql_request_duration_seconds`
- `envio_ga_chain_head_block`, `envio_ga_entity_watermark{entity}`, `envio_ga_entity_staleness_seconds{entity}`
- `envio_ga_pages_total{entity,status}`, `envio_ga_page_duration_seconds{entity}`,
  `envio_ga_rows_written_total{table}`, `envio_ga_entity_rows{entity}`
- `envio_ga_reconcile_added_total{entity}`, `envio_ga_reconcile_tombstoned_total{entity}`,
  `envio_ga_unknown_fields_total{entity}`

Long backfills also emit `Backfill progress` log lines (~every 250k rows) with the current chunk,
rows, and cursor.

---

## Development

```bash
make test                    # unit tests (pure: type map, registry invariants, deletable policy)
.venv/bin/python -m pytest -q
```

**Adding or changing an entity / field:** edit `config/entities.yaml`, run `make introspect` (diffs +
regenerates `generated.py` and `100_typed_entities.sql`), then `make migrate`. No Python changes — the
generic loader/parser handle any entity from its `EntitySpec`.

Dependencies: `aiohttp`, `clickhouse-connect`, `python-dotenv`, `structlog`, `prometheus_client`,
`pyyaml` (`requirements.txt`); `pytest` for dev (`requirements-dev.txt`). Python 3.11 in Docker.
