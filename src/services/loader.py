"""LoaderService — historical backfill and strategy-routed continuous ingestion.

Backfill: build a flat list of work items (one per entity, plus block sub-chunks
for pure block_cursor entities), shard them deterministically and disjointly
across N async workers (no claim race, INV-4), and walk each by id-keyset,
resumable from the furthest completed cursor.

Realtime: each tick reads the head, then per entity routes by tier —
block_cursor/dual walk a [watermark-overlap, head] block window; field_cursor
does a compound (cursor_field, id) walk from the watermark; full_rescan/dual
re-walk all rows on a cadence. Watermarks advance only after durable writes.
"""
import asyncio
import time
from typing import List, Optional

from src.config import config
from src.registry.overrides import all_specs
from src.registry.schema import EntitySpec, SyncStrategy
from src.services.clickhouse import ClickHouse
from src.services.graphql_client import GraphQLClient, GraphQLError
from src.services.state import GaStateManager
from src.loaders.graphql_entity import GraphQLEntityLoader
from src.utils.logger import logger
from src import observability as obs

BACKFILL_BLOCK_CHUNK = 5_000_000   # block-window size for block_cursor sub-chunking
FLUSH_ROWS = 25_000                # accumulate ~this many rows per insert (fewer parts/merges)


class _WorkItem:
    __slots__ = ("spec", "partition_key", "block_lo", "block_hi")

    def __init__(self, spec, partition_key="", block_lo=None, block_hi=None):
        self.spec = spec
        self.partition_key = partition_key
        self.block_lo = block_lo
        self.block_hi = block_hi


class LoaderService:
    def __init__(self):
        self.ch = ClickHouse()
        self.state = GaStateManager(self.ch)
        self._loaders = {}

    def loader_for(self, spec: EntitySpec) -> GraphQLEntityLoader:
        if spec.name not in self._loaders:
            self._loaders[spec.name] = GraphQLEntityLoader(spec, self.ch)
        return self._loaders[spec.name]

    # ── backfill ────────────────────────────────────────────────────────────────
    async def backfill(self, entities: Optional[List[str]] = None, concurrency: Optional[int] = None,
                       page_size: Optional[int] = None, restart: bool = False):
        specs = self._select(entities)
        concurrency = concurrency or config.BACKFILL_CONCURRENCY
        page_size = page_size or config.PAGE_SIZE

        async with GraphQLClient() as gc:
            head = await gc.head_block()
            obs.chain_head_block.set(head)
            logger.info("Backfill starting", entities=[s.name for s in specs],
                        concurrency=concurrency, head=head)

            if restart:
                for s in specs:
                    self.ch.command("ALTER TABLE ga_index_state DELETE WHERE entity={e:String} "
                                    "AND partition_key NOT IN ('realtime','rescan')", {"e": s.name})

            items = self._build_work_items(specs, head)
            logger.info("Backfill work items", total=len(items))

            async def worker(wid: int):
                for item in items[wid::concurrency]:
                    await self._backfill_item(item, gc, head, f"w{wid}", page_size)

            await asyncio.gather(*(worker(i) for i in range(concurrency)))
            logger.info("Backfill complete")

    def _build_work_items(self, specs, head) -> List[_WorkItem]:
        items: List[_WorkItem] = []
        for s in specs:
            if s.strategy == SyncStrategy.BLOCK_CURSOR and s.block_field:
                lo = 0
                while lo <= head:
                    hi = lo + BACKFILL_BLOCK_CHUNK
                    items.append(_WorkItem(s, f"block:{lo}-{hi}", lo, hi))
                    lo = hi
            else:
                items.append(_WorkItem(s, ""))
        return items

    async def _backfill_item(self, item: _WorkItem, gc, head, worker_id, page_size):
        spec, pk = item.spec, item.partition_key
        pos = self.state.resume_position(spec.name, pk)
        if pos["complete"]:
            return
        loader = self.loader_for(spec)
        after_id = pos["cursor"]
        page_seq = pos["page_seq"] + 1
        flush_rows = max(page_size, FLUSH_ROWS)
        buf = []
        batch_start = after_id
        last_id = after_id
        t0 = time.monotonic()
        while True:
            try:
                rows = await gc.fetch_keyset(spec, after_id=after_id, block_lo=item.block_lo,
                                             block_hi=item.block_hi, limit=page_size)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.state.fail_page(spec.name, spec.strategy, batch_start, page_seq, str(e),
                                     partition_key=pk, worker_id=worker_id)
                logger.error("Backfill fetch failed", entity=spec.name, partition_key=pk,
                             cursor_start=batch_start, error=str(e)[:200])
                return
            if rows:
                buf.extend(rows)
                last_id = rows[-1]["id"]
                after_id = last_id
            complete = len(rows) < page_size
            # Flush a batch of accumulated pages -> fewer, larger inserts (fewer parts).
            if buf and (len(buf) >= flush_rows or complete):
                try:
                    written = await loader.write_rows(buf, synced_block=head, check_existing=False)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    self.state.fail_page(spec.name, spec.strategy, batch_start, page_seq, str(e),
                                         partition_key=pk, worker_id=worker_id)
                    obs.pages_total.labels(entity=spec.name, status="failed").inc()
                    logger.error("Backfill batch failed", entity=spec.name, partition_key=pk,
                                 cursor_start=batch_start, error=str(e)[:200])
                    return
                self.state.complete_page(spec.name, spec.strategy, batch_start, last_id, page_seq,
                                         written, complete, partition_key=pk, worker_id=worker_id)
                obs.pages_total.labels(entity=spec.name, status="completed").inc()
                obs.page_duration_seconds.labels(entity=spec.name).observe(time.monotonic() - t0)
                buf = []
                batch_start = last_id
                page_seq += 1
                t0 = time.monotonic()
            if complete:
                break

    # ── realtime ──────────────────────────────────────────────────────────────────
    async def realtime(self, entities: Optional[List[str]] = None, poll_interval: Optional[int] = None):
        specs = self._select(entities)
        poll_interval = poll_interval or config.POLL_INTERVAL
        logger.info("Realtime starting", entities=[s.name for s in specs], poll_interval=poll_interval)
        async with GraphQLClient() as gc:
            while True:
                tick = time.monotonic()
                try:
                    head = await gc.head_block()
                    obs.chain_head_block.set(head)
                    # Sequential per-entity: gentler on a small ClickHouse instance
                    # than running every entity's reads/writes concurrently.
                    for s in specs:
                        await self._realtime_entity(s, gc, head)
                except Exception as e:  # keep the loop alive
                    logger.error("Realtime tick error", error=str(e))
                elapsed = time.monotonic() - tick
                await asyncio.sleep(max(0.0, poll_interval - elapsed))

    async def _realtime_entity(self, spec: EntitySpec, gc, head):
        try:
            if spec.strategy == SyncStrategy.BLOCK_CURSOR:
                await self._rt_block(spec, gc, head)
            elif spec.strategy == SyncStrategy.FIELD_CURSOR:
                await self._rt_field(spec, gc, head)
            elif spec.strategy == SyncStrategy.DUAL:
                await self._rt_block(spec, gc, head)
                await self._maybe_rescan(spec, gc, head)
            else:  # FULL_RESCAN
                await self._maybe_rescan(spec, gc, head)
            obs.entity_staleness_seconds.labels(entity=spec.name).set(0)
        except Exception as e:
            logger.error("Realtime entity error", entity=spec.name, error=str(e))

    async def _rt_block(self, spec, gc, head):
        wm = self.state.get_watermark(spec.name)
        if wm == 0:
            wm = self._bootstrap_block(spec)
        lo = max(0, wm - config.REALTIME_OVERLAP_BLOCKS)
        loader = self.loader_for(spec)
        after_id = ""
        total = 0
        while True:
            rows = await gc.fetch_keyset(spec, after_id=after_id, block_lo=lo, block_hi=head + 1,
                                         limit=spec.page_size)
            if not rows:
                break
            total += await loader.write_rows(rows, synced_block=head)
            after_id = rows[-1]["id"]
            if len(rows) < spec.page_size:
                break
        self.state.set_watermark(spec.name, spec.strategy, head, rows=total)
        obs.entity_watermark.labels(entity=spec.name).set(head)
        if total:
            logger.info("Realtime block sync", entity=spec.name, rows=total, cursor_end=str(head))

    async def _rt_field(self, spec, gc, head):
        cf_ch = spec.cursor_ch_name
        wm = self.state.get_watermark(spec.name)
        if wm == 0:
            wm = self._bootstrap_field(spec)
        loader = self.loader_for(spec)
        after_val = wm
        after_id = ""
        max_seen = wm
        first = True
        total = 0
        while True:
            rows = await gc.fetch_cursor(spec, after_value=after_val,
                                         after_id=("" if first else after_id), limit=spec.page_size)
            if not rows:
                break
            total += await loader.write_rows(rows, synced_block=head)
            last = rows[-1]
            after_val = _as_int(last.get(spec.cursor_field))
            after_id = last["id"]
            max_seen = max(max_seen, after_val)
            first = False
            if len(rows) < spec.page_size:
                break
        self.state.set_watermark(spec.name, spec.strategy, max_seen, rows=total)
        obs.entity_watermark.labels(entity=spec.name).set(max_seen)
        if total:
            logger.info("Realtime field sync", entity=spec.name, rows=total, cursor_end=str(max_seen))

    async def _maybe_rescan(self, spec, gc, head):
        now = int(time.time())
        last = self.state.get_last_rescan(spec.name)
        if now - last < spec.rescan_interval_s:
            return
        loader = self.loader_for(spec)
        after_id = ""
        total = 0
        while True:
            rows = await gc.fetch_keyset(spec, after_id=after_id, limit=spec.page_size)
            if not rows:
                break
            total += await loader.write_rows(rows, synced_block=head)
            after_id = rows[-1]["id"]
            if len(rows) < spec.page_size:
                break
        self.state.set_last_rescan(spec.name, spec.strategy, now, rows=total)
        logger.info("Realtime rescan", entity=spec.name, rows=total)

    def _bootstrap_block(self, spec) -> int:
        bch = spec.block_ch_name or "block_number"
        return int(self.ch.query_value(f"SELECT max({bch}) FROM {spec.name}", default=0) or 0)

    def _bootstrap_field(self, spec) -> int:
        cf = spec.cursor_ch_name
        if not cf:
            return 0
        return int(self.ch.query_value(f"SELECT max({cf}) FROM {spec.name}", default=0) or 0)

    # ── helpers ────────────────────────────────────────────────────────────────
    def _select(self, entities: Optional[List[str]]) -> List[EntitySpec]:
        specs = all_specs()
        if entities:
            wanted = {e.lower() for e in entities}
            specs = [s for s in specs if s.name.lower() in wanted or s.gql_type.lower() in wanted]
        return specs

    def close(self):
        self.ch.close_all_connections()


def _as_int(v) -> int:
    try:
        return int(v)
    except (TypeError, ValueError):
        return 0
