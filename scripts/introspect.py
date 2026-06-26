#!/usr/bin/env python3
"""GraphQL introspection -> entity registry + typed-table DDL (codegen).

Runs the GraphQL `__schema` query, discovers entity root fields, collects scalar
leaf fields, applies config/entities.yaml overrides (tier / cursor / mutable /
deletable / partition / version), enforces the correctness guardrails (INV-2,
INV-3), and emits:

  - src/registry/generated.py        (list[EntitySpec])
  - migrations/100_typed_entities.sql (one ReplacingMergeTree table per entity)

Also performs a drift diff against the previously generated registry and prints
a summary. Reads the endpoint + Bearer token from .env via src.config.
"""
import json
import os
import sys
import urllib.request

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

import yaml  # noqa: E402

from src.config import config  # noqa: E402
from src.registry.schema import EntitySpec, FieldSpec, SyncStrategy  # noqa: E402
from src.utils.types import gql_scalar_to_ch, is_block_field, to_snake, ddl_default  # noqa: E402

# Root fields / types that are NOT data entities.
SKIP_NAMES = {
    "chain_metadata", "event_sync_state", "dynamic_contract_registry",
    "raw_events", "_meta",
}
SKIP_SUFFIXES = ("_aggregate", "_by_pk", "_stream")

# Candidate creation-timestamp field names (used only as a hint; partition by ts
# requires an explicit yaml `partition` or `ts_immutable: true`).
TS_CANDIDATES = ["timestamp", "blockTimestamp", "createdAt", "mintedAt",
                 "completedAt", "acceptedInviteTimestamp"]

INTROSPECTION_QUERY = """
query IntrospectAll {
  __schema {
    queryType { name }
    types {
      kind
      name
      fields(includeDeprecated: true) {
        name
        type { ...TypeRef }
      }
    }
    queryType {
      fields {
        name
        type { ...TypeRef }
      }
    }
  }
}
fragment TypeRef on __Type {
  kind name
  ofType { kind name
    ofType { kind name
      ofType { kind name
        ofType { kind name } } } }
}
"""


def fetch_schema() -> dict:
    headers = {"Content-Type": "application/json"}
    if config.GRAPHQL_API_KEY:
        scheme = (config.GRAPHQL_AUTH_SCHEME + " ") if config.GRAPHQL_AUTH_SCHEME else ""
        headers[config.GRAPHQL_AUTH_HEADER] = f"{scheme}{config.GRAPHQL_API_KEY}"
    body = json.dumps({"query": INTROSPECTION_QUERY}).encode("utf-8")
    req = urllib.request.Request(config.GRAPHQL_ENDPOINT, data=body, headers=headers, method="POST")
    with urllib.request.urlopen(req, timeout=60) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    if "errors" in payload:
        raise RuntimeError(f"Introspection errors: {payload['errors']}")
    return payload["data"]["__schema"]


def unwrap(type_ref: dict):
    """Unwrap NON_NULL/LIST wrappers. Returns (base_kind, base_name, is_list, non_null_top)."""
    is_list = False
    non_null_top = type_ref.get("kind") == "NON_NULL"
    cur = type_ref
    while cur and cur.get("kind") in ("NON_NULL", "LIST"):
        if cur["kind"] == "LIST":
            is_list = True
        cur = cur.get("ofType")
    if not cur:
        return None, None, is_list, non_null_top
    return cur.get("kind"), cur.get("name"), is_list, non_null_top


def discover_entities(schema: dict):
    """Return {gql_type: root_field} for entity list queries."""
    out = {}
    for f in schema["queryType"]["fields"]:
        name = f["name"]
        if name in SKIP_NAMES or name.startswith("_"):
            continue
        if any(name.endswith(suf) for suf in SKIP_SUFFIXES):
            continue
        base_kind, base_name, is_list, _ = unwrap(f["type"])
        if base_kind == "OBJECT" and is_list and base_name and base_name not in SKIP_NAMES:
            out[base_name] = name
    return out


def collect_fields(type_def: dict, field_types: dict = None):
    """Collect scalar/enum leaf FieldSpecs from a GraphQL type definition."""
    field_types = field_types or {}
    fields = []
    block_field = None
    ts_field = None
    for f in type_def.get("fields") or []:
        gql_name = f["name"]
        base_kind, base_name, is_list, non_null = unwrap(f["type"])
        if base_kind not in ("SCALAR", "ENUM"):
            continue          # skip object/interface relations
        if is_list:
            continue          # skip scalar arrays / reverse relations
        if gql_name in field_types:           # explicit per-field type override
            ch_type = field_types[gql_name]
        elif base_kind == "ENUM":
            ch_type = "LowCardinality(String)"
        else:
            ch_type = gql_scalar_to_ch(base_name, gql_name)
        ch_name = to_snake(gql_name)
        fields.append(FieldSpec(gql_name=gql_name, ch_name=ch_name,
                                ch_type=ch_type, nullable=not non_null))
        if is_block_field(gql_name):
            block_field = gql_name
        if ts_field is None and gql_name in TS_CANDIDATES:
            ts_field = gql_name
    return fields, block_field, ts_field


def load_overrides() -> dict:
    path = os.path.join(_ROOT, "config", "entities.yaml")
    if not os.path.exists(path):
        return {"entities": {}, "defaults": {}}
    with open(path) as fh:
        data = yaml.safe_load(fh) or {}
    data.setdefault("entities", {})
    data.setdefault("defaults", {})
    return data


def build_specs(schema: dict, overrides: dict):
    types_by_name = {t["name"]: t for t in schema["types"] if t.get("name")}
    entities = discover_entities(schema)
    defaults = overrides.get("defaults", {})
    ov_entities = overrides.get("entities", {})

    specs = []
    for gql_type, root_field in sorted(entities.items()):
        type_def = types_by_name.get(gql_type)
        if not type_def:
            continue
        ov = ov_entities.get(gql_type, {}) or {}
        fields, block_field, ts_field = collect_fields(type_def, ov.get("field_types"))
        if not any(f.gql_name == "id" for f in fields):
            # every entity must have an id; skip otherwise (not a real entity)
            continue

        strategy = SyncStrategy(ov.get("tier", "full_rescan"))
        cursor_field = ov.get("cursor_field")
        mutable = bool(ov.get("mutable", True))
        deletable = bool(ov.get("deletable", True))
        version_field = ov.get("version_field")
        ts_field = ov.get("ts_field", ts_field)
        block_field = ov.get("block_field", block_field)
        rescan = int(ov.get("rescan_interval_s", defaults.get("rescan_interval_s", 300)))
        enabled = bool(ov.get("enabled", defaults.get("enabled", True)))

        spec = EntitySpec(
            name=to_snake(gql_type),
            gql_type=gql_type,
            root_field=root_field,
            fields=fields,
            strategy=strategy,
            cursor_field=cursor_field,
            block_field=block_field,
            ts_field=ts_field,
            version_field=version_field,
            mutable=mutable,
            deletable=deletable,
            page_size=int(ov.get("page_size", config.PAGE_SIZE)),
            rescan_interval_s=rescan,
            enabled=enabled,
        )
        spec.partition_expr = _partition_expr(spec, ov)
        specs.append(spec)

    _enforce_guardrails(specs)
    return specs


def _partition_expr(spec: EntitySpec, ov: dict) -> str:
    """INV-3: partition only by immutable columns; avoid high-cardinality keys.

    Block-partition only for pure block_cursor entities (asserted immutable, so
    blockNumber is the immutable creation block); their inserts are block-windowed
    so each insert touches few partitions. Everything else is UNPARTITIONED ("")
    so each insert produces a single part — partitioning by cityHash64(id) would
    scatter every insert across many parts and trigger merge-storm OOMs on a small
    ClickHouse instance. Unpartitioned is still immutable + dedup-safe (id is the
    ORDER BY key). An explicit yaml `partition` (or `ts_immutable`) overrides.
    """
    if ov.get("partition"):
        return ov["partition"]
    block_ch = spec.block_ch_name
    if block_ch and spec.strategy == SyncStrategy.BLOCK_CURSOR:
        return f"intDiv({block_ch}, 1000000)"
    if ov.get("ts_immutable") and spec.ts_field:
        ts_ch = spec.ch_name_for(spec.ts_field)
        if ts_ch:
            return f"toStartOfMonth(toDateTime({ts_ch}))"
    return ""  # unpartitioned


def _enforce_guardrails(specs):
    errors = []
    for s in specs:
        if s.strategy == SyncStrategy.BLOCK_CURSOR and s.mutable:
            errors.append(f"{s.gql_type}: block_cursor requires mutable:false (INV-2). "
                          f"Use 'dual' or 'field_cursor', or assert mutable:false in entities.yaml.")
        if s.strategy == SyncStrategy.FIELD_CURSOR and not s.cursor_field:
            errors.append(f"{s.gql_type}: field_cursor requires a cursor_field (INV-2).")
        if s.strategy in (SyncStrategy.BLOCK_CURSOR, SyncStrategy.DUAL) and not s.block_field:
            errors.append(f"{s.gql_type}: {s.strategy.value} requires a block_field.")
        if s.cursor_field and not s.field_by_gql(s.cursor_field):
            errors.append(f"{s.gql_type}: cursor_field '{s.cursor_field}' is not a scalar field.")
    if errors:
        raise ValueError("entities.yaml guardrail violations:\n  - " + "\n  - ".join(errors))


# ── code emission ─────────────────────────────────────────────────────────────
def generate_registry_py(specs) -> str:
    lines = [
        '"""GENERATED by scripts/introspect.py — do not edit by hand."""',
        "from src.registry.schema import EntitySpec, FieldSpec, SyncStrategy",
        "",
        "ENTITIES = [",
    ]
    for s in specs:
        fld = ", ".join(
            f"FieldSpec({f.gql_name!r}, {f.ch_name!r}, {f.ch_type!r}, {f.nullable})"
            for f in s.fields
        )
        lines.append("    EntitySpec(")
        lines.append(f"        name={s.name!r}, gql_type={s.gql_type!r}, root_field={s.root_field!r},")
        lines.append(f"        strategy=SyncStrategy.{s.strategy.name}, cursor_field={s.cursor_field!r},")
        lines.append(f"        block_field={s.block_field!r}, ts_field={s.ts_field!r}, version_field={s.version_field!r},")
        lines.append(f"        mutable={s.mutable}, deletable={s.deletable}, partition_expr={s.partition_expr!r},")
        lines.append(f"        page_size={s.page_size}, rescan_interval_s={s.rescan_interval_s}, enabled={s.enabled},")
        lines.append(f"        fields=[{fld}],")
        lines.append("    ),")
    lines.append("]")
    lines.append("")
    return "\n".join(lines)


def generate_typed_sql(specs) -> str:
    out = ["-- GENERATED by scripts/introspect.py — do not edit by hand.",
           "-- One ReplacingMergeTree table per entity, keyed by id, tombstone-aware.", ""]
    for s in specs:
        out.append(f"-- entity: {s.gql_type}  strategy: {s.strategy.value}  mutable: {s.mutable}")
        out.append(f"CREATE TABLE IF NOT EXISTS {s.name} (")
        cols = ["    `id` String"]
        for f in s.fields:
            if f.gql_name == "id":
                continue
            cols.append(f"    `{f.ch_name}` {f.ch_type} {ddl_default(f.ch_type)}")
        cols.append("    `_deleted` UInt8 DEFAULT 0")
        cols.append("    `_seen_version` UInt64 DEFAULT 0")
        cols.append("    `ingested_at` DateTime DEFAULT now()")
        cols.append("    `_synced_block` UInt64 DEFAULT 0")
        cols.append("    `insert_version` UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))")
        out.append(",\n".join(cols))
        part = f" PARTITION BY {s.partition_expr}" if s.partition_expr else ""
        out.append(f") ENGINE = ReplacingMergeTree({s.version_column}) "
                   f"ORDER BY (id){part};")
        out.append("")
    return "\n".join(out)


def drift_report(specs):
    """Compare new specs to the existing generated.py (names + field sets)."""
    try:
        from src.registry.generated import ENTITIES as old
    except Exception:
        return ["(no previous registry — first generation)"]
    old_by = {s.gql_type: s for s in old}
    new_by = {s.gql_type: s for s in specs}
    msgs = []
    for name in sorted(set(new_by) - set(old_by)):
        msgs.append(f"NEW entity: {name} (review tier/cursor/partition; not auto-enabled if disabled)")
    for name in sorted(set(old_by) - set(new_by)):
        msgs.append(f"REMOVED entity: {name}")
    for name in sorted(set(old_by) & set(new_by)):
        of = {f.gql_name for f in old_by[name].fields}
        nf = {f.gql_name for f in new_by[name].fields}
        for added in sorted(nf - of):
            msgs.append(f"{name}: NEW field {added} (ALTER TABLE {new_by[name].name} ADD COLUMN ...)")
        for removed in sorted(of - nf):
            msgs.append(f"{name}: field removed upstream: {removed}")
    return msgs or ["no drift"]


def main():
    print(f"Introspecting {config.GRAPHQL_ENDPOINT} ...")
    schema = fetch_schema()
    overrides = load_overrides()
    drift = drift_report  # capture before overwrite
    specs = build_specs(schema, overrides)

    print(f"Discovered {len(specs)} entities.")
    msgs = drift(specs)

    reg_path = os.path.join(_ROOT, "src", "registry", "generated.py")
    sql_path = os.path.join(_ROOT, "migrations", "100_typed_entities.sql")
    with open(reg_path, "w") as fh:
        fh.write(generate_registry_py(specs))
    with open(sql_path, "w") as fh:
        fh.write(generate_typed_sql(specs))

    print(f"Wrote {reg_path}")
    print(f"Wrote {sql_path}")
    print("Drift:")
    for m in msgs:
        print(f"  - {m}")

    by_tier = {}
    for s in specs:
        by_tier.setdefault(s.strategy.value, []).append(s.name)
    print("Tiers:")
    for tier, names in sorted(by_tier.items()):
        print(f"  {tier} ({len(names)}): {', '.join(sorted(names))}")


if __name__ == "__main__":
    main()
