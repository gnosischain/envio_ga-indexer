"""GaStateManager — append-only state over ga_index_state (read via FINAL/argMax).

A page is uniquely identified by (entity, partition_key, cursor_start) — the
ReplacingMergeTree ORDER BY key — so every status transition is an INSERT that
collapses to the latest version. page_seq is metrics-only; the backfill resume
key is the furthest completed cursor_end. The realtime watermark is stored in a
reserved partition_key='realtime' row.
"""
from typing import Dict, List, Optional

from src.services.clickhouse import ClickHouse
from src.registry.schema import SyncStrategy

TABLE = "ga_index_state"
REALTIME_PK = "realtime"
RESCAN_PK = "rescan"


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
        return int(self.ch.query_value(
            "SELECT max(attempt_count) FROM " + TABLE + " FINAL "
            "WHERE entity={e:String} AND partition_key={pk:String} AND cursor_start={cs:String}",
            {"e": entity, "pk": partition_key, "cs": cursor_start}, default=0) or 0)

    # ── resume ──────────────────────────────────────────────────────────────────
    def resume_position(self, entity, partition_key="") -> Dict:
        """Furthest completed cursor for a (entity, partition_key) keyset walk."""
        rows = self.ch.execute(
            "SELECT argMax(cursor_end, page_seq) AS c, max(page_seq) AS ps, "
            "max(backfill_complete) AS done "
            "FROM " + TABLE + " FINAL "
            "WHERE entity={e:String} AND partition_key={pk:String} AND status='completed'",
            {"e": entity, "pk": partition_key})
        if not rows or rows[0]["c"] is None:
            return {"cursor": "", "page_seq": -1, "complete": False}
        r = rows[0]
        return {"cursor": r["c"] or "", "page_seq": int(r["ps"] or 0),
                "complete": bool(r["done"])}

    def is_backfill_complete(self, entity, partition_key="") -> bool:
        return self.resume_position(entity, partition_key)["complete"]

    # ── realtime watermark ──────────────────────────────────────────────────────
    def get_watermark(self, entity) -> int:
        v = self.ch.query_value(
            "SELECT argMax(cursor_end, insert_version) FROM " + TABLE + " FINAL "
            "WHERE entity={e:String} AND partition_key={pk:String}",
            {"e": entity, "pk": REALTIME_PK}, default="")
        try:
            return int(v) if v not in (None, "") else 0
        except (TypeError, ValueError):
            return 0

    def set_watermark(self, entity, strategy, value: int, rows: int = 0):
        self._record(entity, "completed", self._strategy_str(strategy),
                     partition_key=REALTIME_PK, cursor_end=str(int(value)), rows_indexed=rows)

    def get_last_rescan(self, entity) -> int:
        v = self.ch.query_value(
            "SELECT argMax(cursor_end, insert_version) FROM " + TABLE + " FINAL "
            "WHERE entity={e:String} AND partition_key={pk:String}",
            {"e": entity, "pk": RESCAN_PK}, default="")
        try:
            return int(v) if v not in (None, "") else 0
        except (TypeError, ValueError):
            return 0

    def set_last_rescan(self, entity, strategy, epoch: int, rows: int = 0):
        self._record(entity, "completed", self._strategy_str(strategy),
                     partition_key=RESCAN_PK, cursor_end=str(int(epoch)), rows_indexed=rows)

    # ── fix / repair ────────────────────────────────────────────────────────────
    def find_pages_by_status(self, status: str, entity: Optional[str] = None) -> List[Dict]:
        where = "status={s:String}"
        params = {"s": status}
        if entity:
            where += " AND entity={e:String}"
            params["e"] = entity
        return self.ch.execute(
            "SELECT entity, partition_key, page_seq, cursor_start, strategy, attempt_count "
            "FROM " + TABLE + " FINAL WHERE " + where + " ORDER BY entity, page_seq", params)

    def recover_stuck_pages(self, timeout_s: int, entity: Optional[str] = None) -> int:
        where = "status='claimed' AND created_at < now64(3) - {t:UInt32}"
        params = {"t": timeout_s}
        if entity:
            where += " AND entity={e:String}"
            params["e"] = entity
        stuck = self.ch.execute(
            "SELECT entity, partition_key, page_seq, cursor_start, strategy "
            "FROM " + TABLE + " FINAL WHERE " + where, params)
        for s in stuck:
            self._record(s["entity"], "pending", str(s["strategy"]),
                         partition_key=s["partition_key"], page_seq=s["page_seq"],
                         cursor_start=s["cursor_start"])
        return len(stuck)

    def requeue_page(self, entity, partition_key, cursor_start, page_seq, strategy):
        self._record(entity, "pending", self._strategy_str(strategy),
                     partition_key=partition_key, page_seq=page_seq, cursor_start=cursor_start)

    def find_page_gaps(self, entity, partition_key="") -> List[str]:
        """Return cursor_start values where the keyset chain is broken
        (a completed page's cursor_end != the next completed page's cursor_start)."""
        pages = self.ch.execute(
            "SELECT page_seq, cursor_start, cursor_end FROM " + TABLE + " FINAL "
            "WHERE entity={e:String} AND partition_key={pk:String} AND status='completed' "
            "ORDER BY page_seq", {"e": entity, "pk": partition_key})
        gaps = []
        prev_end = None
        for p in pages:
            if prev_end is not None and p["cursor_start"] != prev_end:
                gaps.append(prev_end)
            prev_end = p["cursor_end"]
        return gaps

    # ── summary ─────────────────────────────────────────────────────────────────
    def summary(self) -> List[Dict]:
        return self.ch.execute(
            "SELECT entity, "
            "countIf(status='completed' AND partition_key!='realtime') AS completed, "
            "countIf(status='failed') AS failed, countIf(status='dead') AS dead, "
            "countIf(status='claimed') AS claimed, "
            "max(backfill_complete) AS backfill_complete, "
            "sumIf(rows_indexed, status='completed') AS rows_indexed "
            "FROM " + TABLE + " FINAL GROUP BY entity ORDER BY entity")
