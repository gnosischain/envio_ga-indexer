"""GraphQLEntityLoader: write a page of GraphQL rows to raw audit + typed table.

For each page we hash every row, compare against the latest stored hash per id
(argMax over the append-only raw log), and only write rows whose payload changed.
This makes re-runs idempotent, keeps the raw audit log free of identical
consecutive versions, and makes the realtime OVERLAP re-reads free.
"""
from typing import Any, Dict, List

from src.loaders.base import calculate_payload_hash, canonical_json
from src.parsers.generic import GenericEntityParser
from src.registry.schema import EntitySpec
from src.services.clickhouse import ClickHouse
from src import observability as obs


class GraphQLEntityLoader:
    def __init__(self, spec: EntitySpec, clickhouse: ClickHouse):
        self.spec = spec
        self.ch = clickhouse
        self.parser = GenericEntityParser(spec)

    def _existing_hashes(self, ids: List[str]) -> Dict[str, str]:
        if not ids:
            return {}
        rows = self.ch.execute(
            "SELECT id, argMax(payload_hash, insert_version) AS h "
            "FROM raw_entities WHERE entity = {e:String} AND id IN {ids:Array(String)} "
            "GROUP BY id",
            {"e": self.spec.name, "ids": list(ids)},
        )
        return {r["id"]: r["h"] for r in rows}

    async def write_rows(self, gql_rows: List[Dict[str, Any]], synced_block: int = 0,
                         check_existing: bool = True) -> int:
        """Write changed rows; return the number of changed (written) rows.

        check_existing=True (realtime/rescan/reconcile): compare each row's hash to
        the latest stored hash and write only what changed (free overlap re-reads,
        no duplicate raw versions). check_existing=False (backfill): skip that heavy
        per-page read entirely — on a clean backfill every row is new, and resume
        skips completed pages, so the read is pure overhead (and an OOM risk on a
        small ClickHouse instance).
        """
        if not gql_rows:
            return 0

        hashes = {r["id"]: calculate_payload_hash(r) for r in gql_rows}
        if check_existing:
            existing = self._existing_hashes([r["id"] for r in gql_rows])
            changed = [r for r in gql_rows if hashes[r["id"]] != existing.get(r["id"])]
        else:
            changed = gql_rows
        if not changed:
            return 0

        block_field = self.spec.block_field
        raw_rows = []
        for r in changed:
            bn = 0
            if block_field and r.get(block_field) not in (None, ""):
                try:
                    bn = int(r[block_field])
                except (TypeError, ValueError):
                    bn = 0
            raw_rows.append({
                "entity": self.spec.name,
                "id": r["id"],
                "payload": canonical_json(r),
                "payload_hash": hashes[r["id"]],
                "block_number": bn,
            })

        typed_rows = [self.parser.to_typed_row(r, synced_block) for r in changed]

        await self.ch.insert_batch_concurrent("raw_entities", raw_rows)
        await self.ch.insert_batch_concurrent(self.spec.name, typed_rows)

        obs.rows_written_total.labels(table=self.spec.name).inc(len(typed_rows))
        obs.rows_written_total.labels(table="raw_entities").inc(len(raw_rows))
        return len(changed)
