"""MaintenanceService — reconcile (delete detection), check, fix, reset, reprocess.

reconcile  : id-only full keyset walk -> stage live ids -> tombstone disappeared
             ids (INV-1). The only mechanism that handles upstream deletions.
check      : report state summary, page gaps, failed/dead/stuck pages.
fix        : recover stuck pages, re-queue failed/dead, optional targeted
             re-ingest of an --id-range / --block-range (delete-then-reinsert).
reset      : requeue pages of a given status, or clear backfill state for a
             full re-walk.
reprocess  : re-derive typed tables from the raw audit log (no API calls).
"""
import json
import time
from typing import List, Optional

from src.config import config
from src.registry.overrides import all_specs
from src.registry.schema import EntitySpec, SyncStrategy
from src.services.clickhouse import ClickHouse
from src.services.graphql_client import GraphQLClient
from src.services.state import GaStateManager
from src.loaders.graphql_entity import GraphQLEntityLoader
from src.parsers.generic import GenericEntityParser
from src.utils.logger import logger
from src import observability as obs

_TRAILER_COLS = ["_seen_version", "ingested_at", "_synced_block"]


class MaintenanceService:
    def __init__(self):
        self.ch = ClickHouse()
        self.state = GaStateManager(self.ch)

    def _select(self, entities: Optional[List[str]]) -> List[EntitySpec]:
        specs = all_specs()
        if entities:
            wanted = {e.lower() for e in entities}
            specs = [s for s in specs if s.name.lower() in wanted or s.gql_type.lower() in wanted]
        return specs

    def close(self):
        self.ch.close_all_connections()

    # ── reconcile (INV-1 delete detection) ──────────────────────────────────────
    async def reconcile(self, entities: Optional[List[str]] = None):
        specs = [s for s in self._select(entities) if s.deletable]
        skipped = [s.name for s in self._select(entities) if not s.deletable]
        if skipped:
            logger.info("Reconcile skipping non-deletable entities", entities=skipped)
        async with GraphQLClient() as gc:
            for spec in specs:
                await self._reconcile_entity(spec, gc)

    async def _reconcile_entity(self, spec: EntitySpec, gc):
        stage = f"_reconcile_{spec.name}"
        self.ch.command(f"DROP TABLE IF EXISTS {stage}")
        self.ch.command(f"CREATE TABLE {stage} (id String) ENGINE = MergeTree ORDER BY id")
        live = 0
        after = ""
        try:
            while True:
                rows = await gc.fetch_keyset(spec, after_id=after, ids_only=True, limit=spec.page_size)
                if not rows:
                    break
                self.ch.insert_batch(stage, [{"id": r["id"]} for r in rows])
                live += len(rows)
                after = rows[-1]["id"]
                if len(rows) < spec.page_size:
                    break

            added = int(self.ch.query_value(
                f"SELECT count() FROM {stage} WHERE id NOT IN "
                f"(SELECT id FROM {spec.name} FINAL WHERE _deleted=0)", default=0) or 0)
            to_delete = int(self.ch.query_value(
                f"SELECT count() FROM {spec.name} FINAL WHERE _deleted=0 "
                f"AND id NOT IN (SELECT id FROM {stage})", default=0) or 0)

            if to_delete:
                cols = ["id"] + [f.ch_name for f in spec.fields if f.gql_name != "id"] + _TRAILER_COLS
                col_list = ", ".join(f"`{c}`" for c in cols)
                self.ch.command(
                    f"INSERT INTO {spec.name} ({col_list}, _deleted) "
                    f"SELECT {col_list}, 1 FROM {spec.name} FINAL "
                    f"WHERE _deleted=0 AND id NOT IN (SELECT id FROM {stage})")

            obs.reconcile_added_total.labels(entity=spec.name).inc(added)
            obs.reconcile_tombstoned_total.labels(entity=spec.name).inc(to_delete)
            logger.info("Reconciled", entity=spec.name, live=live, new_ids=added, tombstoned=to_delete)
        finally:
            self.ch.command(f"DROP TABLE IF EXISTS {stage}")

    # ── check ───────────────────────────────────────────────────────────────────
    def check(self, entities: Optional[List[str]] = None):
        specs = self._select(entities)
        names = {s.name for s in specs}
        summary = [r for r in self.state.summary() if r["entity"] in names]
        report = {"summary": summary, "gaps": {}, "failed": [], "dead": [], "stuck": []}
        for s in specs:
            gaps = self.state.find_page_gaps(s.name)
            if gaps:
                report["gaps"][s.name] = gaps
        report["failed"] = self.state.find_pages_by_status("failed")
        report["dead"] = self.state.find_pages_by_status("dead")
        report["stuck"] = self.state.find_pages_by_status("claimed")
        report["failed"] = [r for r in report["failed"] if r["entity"] in names]
        report["dead"] = [r for r in report["dead"] if r["entity"] in names]
        report["stuck"] = [r for r in report["stuck"] if r["entity"] in names]
        return report

    # ── fix ───────────────────────────────────────────────────────────────────
    async def fix(self, entities: Optional[List[str]] = None, id_range: Optional[str] = None,
                  block_range: Optional[str] = None, dry_run: bool = False, stuck_timeout_s: int = 3600):
        specs = self._select(entities)
        actions = []

        recovered = 0 if dry_run else self.state.recover_stuck_pages(stuck_timeout_s)
        actions.append(f"recovered_stuck_pages={recovered}")

        # Re-queue failed/dead pages for the selected entities.
        names = {s.name for s in specs}
        broken = [r for r in (self.state.find_pages_by_status("failed")
                              + self.state.find_pages_by_status("dead")) if r["entity"] in names]
        for r in broken:
            if not dry_run:
                self.state.requeue_page(r["entity"], r["partition_key"], r["cursor_start"],
                                        r["page_seq"], r["strategy"])
        actions.append(f"requeued_pages={len(broken)}")

        # Targeted re-ingest (one entity expected when a range is given).
        if (id_range or block_range) and not dry_run:
            async with GraphQLClient() as gc:
                head = await gc.head_block()
                for spec in specs:
                    if block_range and spec.block_field:
                        lo, hi = (int(x) for x in block_range.split(":"))
                        await self._reingest_block_range(spec, gc, lo, hi, head)
                        actions.append(f"{spec.name}: reingested blocks {lo}-{hi}")
                    if id_range:
                        lo, hi = id_range.split(":")
                        await self._reingest_id_range(spec, gc, lo, hi, head)
                        actions.append(f"{spec.name}: reingested ids {lo}..{hi}")

        # Re-drive incomplete backfills + requeued pages.
        if not dry_run and (broken or recovered):
            from src.services.loader import LoaderService
            loader = LoaderService()
            try:
                await loader.backfill(entities=[s.name for s in specs])
            finally:
                loader.close()
            actions.append("re-drove backfill for affected entities")

        return actions

    async def _reingest_block_range(self, spec, gc, lo, hi, head):
        loader = GraphQLEntityLoader(spec, self.ch)
        bch = spec.block_ch_name
        # delete-then-reinsert (cryo pattern): physically remove the window first.
        self.ch.command(f"DELETE FROM {spec.name} WHERE {bch} >= {int(lo)} AND {bch} < {int(hi)}")
        after = ""
        while True:
            rows = await gc.fetch_keyset(spec, after_id=after, block_lo=lo, block_hi=hi,
                                         limit=spec.page_size)
            if not rows:
                break
            await loader.write_rows(rows, synced_block=head)
            after = rows[-1]["id"]
            if len(rows) < spec.page_size:
                break

    async def _reingest_id_range(self, spec, gc, lo, hi, head):
        loader = GraphQLEntityLoader(spec, self.ch)
        after = lo  # exclusive lower bound via keyset _gt; pass the bound just below if inclusive needed
        while True:
            rows = await gc.fetch_keyset(spec, after_id=after, limit=spec.page_size)
            if not rows:
                break
            keep = [r for r in rows if r["id"] <= hi]
            if keep:
                await loader.write_rows(keep, synced_block=head)
            after = rows[-1]["id"]
            if after > hi or len(rows) < spec.page_size:
                break

    # ── reset ───────────────────────────────────────────────────────────────────
    def reset(self, entities: Optional[List[str]] = None, status: Optional[str] = None):
        specs = self._select(entities)
        if status:
            pages = [r for r in self.state.find_pages_by_status(status)
                     if r["entity"] in {s.name for s in specs}]
            for r in pages:
                self.state.requeue_page(r["entity"], r["partition_key"], r["cursor_start"],
                                        r["page_seq"], r["strategy"])
            return f"requeued {len(pages)} '{status}' pages"
        # No status: clear backfill state -> full re-walk on next backfill.
        for s in specs:
            self.ch.command("ALTER TABLE ga_index_state DELETE WHERE entity={e:String} "
                            "AND partition_key NOT IN ('realtime','rescan')", {"e": s.name})
        return f"cleared backfill state for {len(specs)} entities"

    # ── reprocess (re-derive typed from raw, no API) ────────────────────────────
    def reprocess(self, entities: Optional[List[str]] = None, batch: int = 5000):
        specs = self._select(entities)
        total = {}
        for spec in specs:
            parser = GenericEntityParser(spec)
            after = ""
            n = 0
            while True:
                rows = self.ch.execute(
                    "SELECT id, argMax(payload, insert_version) AS payload FROM raw_entities "
                    "WHERE entity={e:String} AND id > {a:String} GROUP BY id ORDER BY id LIMIT {n:UInt32}",
                    {"e": spec.name, "a": after, "n": batch})
                if not rows:
                    break
                typed = []
                for r in rows:
                    try:
                        gql_row = json.loads(r["payload"])
                    except (json.JSONDecodeError, TypeError):
                        continue
                    typed.append(parser.to_typed_row(gql_row))
                if typed:
                    self.ch.insert_batch(spec.name, typed)
                n += len(rows)
                after = rows[-1]["id"]
                if len(rows) < batch:
                    break
            total[spec.name] = n
            logger.info("Reprocessed from raw", entity=spec.name, rows=n)
        return total
