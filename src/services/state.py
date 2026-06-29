"""GaStateManager — append-only state over ga_index_state.

A page is uniquely identified by (entity, partition_key, cursor_start). Every
status transition is an INSERT; reads dedup to the latest version per key with
argMax(col, insert_version) GROUP BY ... — never FINAL (FINAL forces a heavy
merge-on-read). page_seq is metrics-only; the backfill resume key is the furthest
completed cursor_end. The realtime watermark lives in a reserved
partition_key='realtime' row, the rescan marker in partition_key='rescan'.
"""
from typing import Dict, List, Optional

from src.services.clickhouse import ClickHouse
from src.registry.schema import SyncStrategy

TABLE = "ga_index_state"
REALTIME_PK = "realtime"
RESCAN_PK = "rescan"

# Latest version of each (entity, partition_key, cursor_start) row — the FINAL-free
# equivalent of `FROM ga_index_state FINAL`. Pass an inner WHERE to push the entity
# filter into the aggregation.
def _latest(inner_where: str = "") -> str:
    w = (" WHERE " + inner_where) if inner_where else ""
    return (
        "SELECT entity, partition_key, cursor_start, "
        "argMax(status, insert_version) AS status, "
        "argMax(cursor_end, insert_version) AS cursor_end, "
        "argMax(page_seq, insert_version) AS page_seq, "
        "argMax(backfill_complete, insert_version) AS backfill_complete, "
        "argMax(attempt_count, insert_version) AS attempt_count, "
        "argMax(strategy, insert_version) AS strategy, "
        "argMax(worker_id, insert_version) AS worker_id, "
        "argMax(created_at, insert_version) AS created_at, "
        "argMax(rows_indexed, insert_version) AS rows_indexed "
        "FROM " + TABLE + w + " GROUP BY entity, partition_key, cursor_start"
    )


class GaStateManager:
    def __init__(self, clickhouse: ClickHouse):
        self.ch = clickhouse

    # ── generic record ─────────────────────────────────────────────────────────
    def _record(self, entity: str, status: str, strategy: str, *, partition_key: str = "",
                page_seq: int = 0, cursor_start: str = "", cursor_end: str = "",
                backfill_complete: int = 0, worker_id: str = "", rows_indexed: int = 0,
                attempt_count: int = 0, error_message: str = ""):
        self.ch.insert_batch(TABLE, [{
            "entity": entity, "partition_key": partition_key, "strategy": strategy,
            "page_seq": page_seq, "cursor_start": cursor_start, "cursor_end": cursor_end,
            "status": status, "backfill_complete": backfill_complete, "worker_id": worker_id,
            "rows_indexed": rows_indexed, "attempt_count": attempt_count,
            "error_message": error_message[:500],
        }])

    @staticmethod
    def _strategy_str(strategy) -> str:
        return strategy.value if isinstance(strategy, SyncStrategy) else str(strategy)

    # ── backfill page lifecycle ────────────────────────────────────────────────
    def claim_page(self, entity, strategy, cursor_start, page_seq, worker_id, partition_key="", attempt=0):
        self._record(entity, "claimed", self._strategy_str(strategy), partition_key=partition_key,
                     page_seq=page_seq, cursor_start=cursor_start, worker_id=worker_id,
                     attempt_count=attempt)

    def complete_page(self, entity, strategy, cursor_start, cursor_end, page_seq, rows,
                      backfill_complete, partition_key="", worker_id=""):
        self._record(entity, "completed", self._strategy_str(strategy), partition_key=partition_key,
                     page_seq=page_seq, cursor_start=cursor_start, cursor_end=cursor_end,
                     rows_indexed=rows, backfill_complete=1 if backfill_complete else 0,
                     worker_id=worker_id)

    def fail_page(self, entity, strategy, cursor_start, page_seq, error, partition_key="",
                  worker_id="", dead_after=5):
        attempts = self._attempts(entity, partition_key, cursor_start) + 1
        status = "dead" if attempts >= dead_after else "failed"
        self._record(entity, status, self._strategy_str(strategy), partition_key=partition_key,
                     page_seq=page_seq, cursor_start=cursor_start, worker_id=worker_id,
                     attempt_count=attempts, error_message=str(error))
        return status

    def _attempts(self, entity, partition_key, cursor_start) -> int:
        sub = _latest("entity={e:String} AND partition_key={pk:String} AND cursor_start={cs:String}")
        return int(self.ch.query_value(
            f"SELECT max(attempt_count) FROM ({sub})",
            {"e": entity, "pk": partition_key, "cs": cursor_start}, default=0) or 0)

    # ── resume ──────────────────────────────────────────────────────────────────
    def resume_position(self, entity, partition_key="") -> Dict:
        """Furthest completed cursor for a (entity, partition_key) keyset walk."""
        sub = _latest("entity={e:String} AND partition_key={pk:String}")
        rows = self.ch.execute(
            "SELECT argMax(cursor_end, page_seq) AS c, max(page_seq) AS ps, "
            f"max(backfill_complete) AS done FROM ({sub}) WHERE status='completed'",
            {"e": entity, "pk": partition_key})
        if not rows or rows[0]["c"] is None:
            return {"cursor": "", "page_seq": -1, "complete": False}
        r = rows[0]
        return {"cursor": r["c"] or "", "page_seq": int(r["ps"] or 0),
                "complete": bool(r["done"])}

    def is_backfill_complete(self, entity) -> bool:
        """Complete only when EVERY chunk is complete. backfill_complete is per-page
        (last page of a chunk = 1), so: chunk_complete = max over pages; entity = min over chunks."""
        sub = _latest("entity={e:String} AND partition_key NOT IN ('realtime','rescan')")
        rows = self.ch.execute(
            "SELECT min(chunk_complete) AS done, count() AS n FROM "
            f"(SELECT partition_key, max(backfill_complete) AS chunk_complete FROM ({sub}) "
            "GROUP BY partition_key)", {"e": entity})
        if not rows or not rows[0]["n"]:
            return False
        return bool(rows[0]["done"])

    # ── realtime watermark / rescan marker (argMax already dedups; no FINAL) ─────
    def get_watermark(self, entity) -> int:
        return self._marker(entity, REALTIME_PK)

    def set_watermark(self, entity, strategy, value: int, rows: int = 0):
        self._record(entity, "completed", self._strategy_str(strategy),
                     partition_key=REALTIME_PK, cursor_end=str(int(value)), rows_indexed=rows)

    def get_last_rescan(self, entity) -> int:
        return self._marker(entity, RESCAN_PK)

    def set_last_rescan(self, entity, strategy, epoch: int, rows: int = 0):
        self._record(entity, "completed", self._strategy_str(strategy),
                     partition_key=RESCAN_PK, cursor_end=str(int(epoch)), rows_indexed=rows)

    def _marker(self, entity, pk) -> int:
        v = self.ch.query_value(
            "SELECT argMax(cursor_end, insert_version) FROM " + TABLE +
            " WHERE entity={e:String} AND partition_key={pk:String}",
            {"e": entity, "pk": pk}, default="")
        try:
            return int(v) if v not in (None, "") else 0
        except (TypeError, ValueError):
            return 0

    # ── fix / repair ────────────────────────────────────────────────────────────
    def find_pages_by_status(self, status: str, entity: Optional[str] = None) -> List[Dict]:
        inner = "entity={e:String}" if entity else ""
        params = {"s": status}
        if entity:
            params["e"] = entity
        return self.ch.execute(
            "SELECT entity, partition_key, page_seq, cursor_start, strategy, attempt_count "
            f"FROM ({_latest(inner)}) WHERE status={{s:String}} ORDER BY entity, page_seq", params)

    def recover_stuck_pages(self, timeout_s: int, entity: Optional[str] = None) -> int:
        inner = "entity={e:String}" if entity else ""
        params = {"t": timeout_s}
        if entity:
            params["e"] = entity
        stuck = self.ch.execute(
            "SELECT entity, partition_key, page_seq, cursor_start, strategy "
            f"FROM ({_latest(inner)}) "
            "WHERE status='claimed' AND created_at < now64(3) - {t:UInt32}", params)
        for s in stuck:
            self._record(s["entity"], "pending", str(s["strategy"]),
                         partition_key=s["partition_key"], page_seq=s["page_seq"],
                         cursor_start=s["cursor_start"])
        return len(stuck)

    def requeue_page(self, entity, partition_key, cursor_start, page_seq, strategy):
        self._record(entity, "pending", self._strategy_str(strategy),
                     partition_key=partition_key, page_seq=page_seq, cursor_start=cursor_start)

    def find_page_gaps(self, entity, partition_key="") -> List[str]:
        """cursor_start values where the keyset chain is broken (cursor_end[k] != cursor_start[k+1])."""
        sub = _latest("entity={e:String} AND partition_key={pk:String}")
        pages = self.ch.execute(
            f"SELECT page_seq, cursor_start, cursor_end FROM ({sub}) "
            "WHERE status='completed' ORDER BY page_seq", {"e": entity, "pk": partition_key})
        gaps = []
        prev_end = None
        for p in pages:
            if prev_end is not None and p["cursor_start"] != prev_end:
                gaps.append(prev_end)
            prev_end = p["cursor_end"]
        return gaps

    # ── summary ─────────────────────────────────────────────────────────────────
    def summary(self) -> List[Dict]:
        # Two-level: per chunk, chunk_complete = max(backfill_complete) over its pages;
        # per entity, backfill_complete = min(chunk_complete) over chunks. So a
        # block-sub-chunked entity is "complete" only when EVERY chunk finished.
        sub = _latest()
        return self.ch.execute(
            "SELECT entity, sum(c_pages) AS completed, sum(c_failed) AS failed, "
            "sum(c_dead) AS dead, sum(c_claimed) AS claimed, count() AS chunks, "
            "min(chunk_complete) AS backfill_complete, sum(c_rows) AS rows_indexed FROM ("
            "  SELECT entity, partition_key, max(backfill_complete) AS chunk_complete, "
            "  countIf(status='completed') AS c_pages, countIf(status='failed') AS c_failed, "
            "  countIf(status='dead') AS c_dead, countIf(status='claimed') AS c_claimed, "
            "  sumIf(rows_indexed, status='completed') AS c_rows "
            f"  FROM ({sub}) WHERE partition_key NOT IN ('realtime','rescan') "
            "  GROUP BY entity, partition_key"
            ") GROUP BY entity ORDER BY entity")
