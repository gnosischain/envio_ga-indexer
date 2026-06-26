"""ClickHouse client wrapper, adapted (and slimmed) from beacon-indexer.

Keeps: secure connection with long timeouts, a small connection pool for
concurrent async inserts, robust row-oriented insert with value normalization
(int passthrough for UInt256, dict/list -> JSON, datetime -> naive, bool -> 0/1),
and FINAL-friendly read helpers. Drops all beacon-specific chunk/validator logic
(state-table claim logic lives in src/services/state.py).
"""
import asyncio
import json
from datetime import datetime
from typing import Any, Dict, List, Optional

import clickhouse_connect
from clickhouse_connect.driver.client import Client

from src.config import config
from src.utils.logger import logger


def connect_clickhouse(
    host: str,
    port: int,
    user: str,
    password: str,
    database: str,
    secure: bool = False,
    verify: bool = False,
) -> Client:
    """Open a clickhouse-connect client and verify it with SELECT 1."""
    client = clickhouse_connect.get_client(
        host=host,
        port=port,
        username=user,
        password=password,
        database=database,
        secure=secure,
        verify=verify,
        send_receive_timeout=1800,
        connect_timeout=300,
    )
    client.command("SELECT 1")
    return client


def _norm(v: Any) -> Any:
    """Normalize a Python value for clickhouse-connect row insert."""
    if isinstance(v, datetime):
        return v.replace(tzinfo=None)
    if isinstance(v, bool):
        return 1 if v else 0
    if isinstance(v, (dict, list)):
        return json.dumps(v, ensure_ascii=False)
    return v  # int (incl. UInt256), str, float, None pass through


class ClickHouse:
    """Thin ClickHouse client with a pool for concurrent inserts."""

    POOL_SIZE = 8
    MAX_ROWS_PER_CHUNK = 10000

    # Memory-friendly settings for ClickHouse Cloud.
    INSERT_SETTINGS = {
        "max_insert_block_size": 1000,
        "max_memory_usage": "200000000",
        "input_format_parallel_parsing": 0,
    }

    def __init__(self):
        self.client = self._new_client()
        self._pool: List[Client] = []
        self._pool_lock = asyncio.Lock()
        logger.info("ClickHouse client initialized",
                    host=config.CLICKHOUSE_HOST, database=config.CLICKHOUSE_DATABASE)

    def _new_client(self) -> Client:
        return connect_clickhouse(
            host=config.CLICKHOUSE_HOST,
            port=config.CLICKHOUSE_PORT,
            user=config.CLICKHOUSE_USER,
            password=config.CLICKHOUSE_PASSWORD,
            database=config.CLICKHOUSE_DATABASE,
            secure=config.CLICKHOUSE_SECURE,
            verify=False,
        )

    # ── reads ────────────────────────────────────────────────────────────────
    def execute(self, query: str, params: Optional[Dict] = None,
                settings: Optional[Dict] = None) -> List[Dict]:
        """Run a query, return rows as list of dicts."""
        try:
            kwargs = {}
            if params:
                kwargs["parameters"] = params
            if settings:
                kwargs["settings"] = settings
            result = self.client.query(query, **kwargs)
            if result.result_rows:
                cols = result.column_names
                return [dict(zip(cols, row)) for row in result.result_rows]
            return []
        except Exception as e:
            logger.error("ClickHouse query failed", query=query[:200], error=str(e))
            raise

    def query_value(self, query: str, params: Optional[Dict] = None, default: Any = None,
                    settings: Optional[Dict] = None) -> Any:
        """Return the first column of the first row, or `default`."""
        rows = self.execute(query, params, settings=settings)
        if not rows:
            return default
        first = rows[0]
        return next(iter(first.values()), default)

    def command(self, sql: str, params: Optional[Dict] = None):
        """Execute a statement (DDL / INSERT ... VALUES / DELETE / etc.)."""
        try:
            if params:
                return self.client.command(sql, parameters=params)
            return self.client.command(sql)
        except Exception as e:
            logger.error("ClickHouse command failed", sql=sql[:200], error=str(e))
            raise

    # ── writes ────────────────────────────────────────────────────────────────
    def insert_batch(self, table: str, data: List[Dict[str, Any]], column_order: Optional[List[str]] = None):
        """Row-oriented chunked insert. Synchronous (main client)."""
        if not data:
            return
        self._do_insert(self.client, table, data, column_order)

    async def insert_batch_concurrent(self, table: str, data: List[Dict[str, Any]],
                                      column_order: Optional[List[str]] = None):
        """Async insert using a pooled connection (off the event loop)."""
        if not data:
            return
        conn = await self._get_conn()
        try:
            await asyncio.get_event_loop().run_in_executor(
                None, self._do_insert, conn, table, data, column_order
            )
        finally:
            await self._return_conn(conn)

    def _do_insert(self, client: Client, table: str, data: List[Dict[str, Any]],
                   column_order: Optional[List[str]]):
        if column_order:
            cols = list(column_order)
        else:
            cols = list(data[0].keys())
            for r in data[1:]:
                for k in r.keys():
                    if k not in cols:
                        cols.append(k)

        def as_row(r: Dict[str, Any]):
            return [_norm(r.get(c)) for c in cols]

        for i in range(0, len(data), self.MAX_ROWS_PER_CHUNK):
            chunk = data[i:i + self.MAX_ROWS_PER_CHUNK]
            rows = [as_row(r) for r in chunk]
            client.insert(table, rows, column_names=cols, column_oriented=False,
                          settings=self.INSERT_SETTINGS)

    # ── pool ──────────────────────────────────────────────────────────────────
    async def _get_conn(self) -> Client:
        async with self._pool_lock:
            if self._pool:
                return self._pool.pop()
        return self._new_client()

    async def _return_conn(self, conn: Client):
        async with self._pool_lock:
            if len(self._pool) < self.POOL_SIZE:
                self._pool.append(conn)
                return
        try:
            conn.close()
        except Exception:
            pass

    def close_all_connections(self):
        try:
            self.client.close()
        except Exception:
            pass
        while self._pool:
            try:
                self._pool.pop().close()
            except Exception:
                pass
