"""Async GraphQL client for the Hasura/Envio endpoint.

Single POST endpoint with Bearer auth, retry/backoff on 429/5xx/timeout, a shared
token-bucket rate limiter + in-flight semaphore to protect the dedicated Hasura,
and keyset query builders. There is NO count method (the API exposes no
*_aggregate). Modeled on beacon-indexer's beacon_api.py.

Query builders:
  - fetch_keyset(spec, after_id, block_lo, block_hi, ids_only): id-keyset walk,
    optionally bounded to a [block_lo, block_hi) window. Used by backfill,
    full_rescan, block_cursor realtime (block window), dual rescan, reconcile.
  - fetch_cursor(spec, after_value, after_id): compound (cursor_field, id) keyset.
    Used by field_cursor realtime.
"""
import asyncio
import json
import random
import time
from typing import Any, Dict, List, Optional

import aiohttp

from src.config import config
from src.registry.schema import EntitySpec
from src.utils.logger import logger
from src import observability as obs


class GraphQLError(Exception):
    """Non-retryable GraphQL validation error (the `errors` payload)."""


class _RateLimiter:
    """Simple async token bucket: at most rps requests/second, shared."""

    def __init__(self, rps: float):
        self.min_interval = (1.0 / rps) if rps and rps > 0 else 0.0
        self._lock = asyncio.Lock()
        self._next = 0.0

    async def acquire(self):
        if self.min_interval <= 0:
            return
        async with self._lock:
            now = asyncio.get_event_loop().time()
            wait = self._next - now
            if wait > 0:
                await asyncio.sleep(wait)
                now = asyncio.get_event_loop().time()
            self._next = max(now, self._next) + self.min_interval


def _gql_str(value: str) -> str:
    """Quote a string as a GraphQL string literal (JSON escaping is compatible)."""
    return json.dumps(value)


def _selection(spec: EntitySpec, ids_only: bool) -> str:
    if ids_only:
        return "id"
    return " ".join(spec.gql_field_names)


class GraphQLClient:
    def __init__(self):
        self.endpoint = config.GRAPHQL_ENDPOINT
        self.session: Optional[aiohttp.ClientSession] = None
        self.max_retries = config.GQL_MAX_RETRIES
        self._limiter = _RateLimiter(config.GQL_MAX_RPS)
        self._sem = asyncio.Semaphore(max(1, config.BACKFILL_CONCURRENCY))
        self._headers = {"Content-Type": "application/json"}
        if config.GRAPHQL_API_KEY:
            scheme = (config.GRAPHQL_AUTH_SCHEME + " ") if config.GRAPHQL_AUTH_SCHEME else ""
            self._headers[config.GRAPHQL_AUTH_HEADER] = f"{scheme}{config.GRAPHQL_API_KEY}"

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    async def start(self):
        if not self.session:
            timeout = aiohttp.ClientTimeout(total=config.GQL_TIMEOUT)
            self.session = aiohttp.ClientSession(timeout=timeout, headers=self._headers)

    async def close(self):
        if self.session:
            await self.session.close()
            self.session = None

    # ── core execute ──────────────────────────────────────────────────────────
    async def execute(self, query: str, operation: str = "query") -> Dict[str, Any]:
        if not self.session:
            await self.start()
        body = json.dumps({"query": query})

        for attempt in range(self.max_retries):
            start = time.monotonic()
            try:
                await self._limiter.acquire()
                async with self._sem:
                    async with self.session.post(self.endpoint, data=body) as resp:
                        status = resp.status
                        obs.graphql_requests_total.labels(operation=operation, status=str(status)).inc()
                        obs.graphql_request_duration_seconds.labels(operation=operation).observe(
                            time.monotonic() - start)
                        if status == 200:
                            payload = await resp.json()
                            if "errors" in payload and payload["errors"]:
                                # GraphQL validation error: non-retryable.
                                raise GraphQLError(json.dumps(payload["errors"])[:500])
                            return payload.get("data", {})
                        text = await resp.text()
                        if status in (429,) or status >= 500:
                            self._backoff_log(operation, status, attempt, text)
                        else:
                            raise GraphQLError(f"HTTP {status}: {text[:300]}")
            except GraphQLError:
                raise
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                obs.graphql_requests_total.labels(operation=operation, status="neterror").inc()
                self._backoff_log(operation, "neterror", attempt, str(e))

            if attempt < self.max_retries - 1:
                await asyncio.sleep(self._backoff(attempt))
            else:
                raise GraphQLError(f"{operation} failed after {self.max_retries} attempts")

        raise GraphQLError(f"{operation} exhausted retries")

    @staticmethod
    def _backoff(attempt: int) -> float:
        return min(60.0, (2 ** attempt)) + random.uniform(0, 1)

    def _backoff_log(self, operation, status, attempt, detail):
        logger.warning("GraphQL request retrying", operation=operation, status=str(status),
                       attempt=attempt + 1, detail=str(detail)[:200])

    # ── head / liveness ───────────────────────────────────────────────────────
    async def head_block(self) -> int:
        data = await self.execute("{ chain_metadata { block_height } }", operation="head_block")
        rows = data.get("chain_metadata") or []
        if not rows:
            return 0
        return int(rows[0].get("block_height") or 0)

    # ── query builders ────────────────────────────────────────────────────────
    async def fetch_keyset(self, spec: EntitySpec, after_id: str = "",
                           block_lo: Optional[int] = None, block_hi: Optional[int] = None,
                           ids_only: bool = False, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """id-keyset page, optionally bounded to a [block_lo, block_hi) block window."""
        limit = limit or spec.page_size
        conds = []
        if after_id:
            conds.append(f"id: {{_gt: {_gql_str(after_id)}}}")
        if (block_lo is not None or block_hi is not None) and spec.block_field:
            parts = []
            if block_lo is not None:
                parts.append(f"_gte: {int(block_lo)}")
            if block_hi is not None:
                parts.append(f"_lt: {int(block_hi)}")
            conds.append(f"{spec.block_field}: {{{', '.join(parts)}}}")
        where = ("where: {" + ", ".join(conds) + "}, ") if conds else ""
        query = (f"{{ {spec.root_field}({where}order_by: {{id: asc}}, limit: {limit}) "
                 f"{{ {_selection(spec, ids_only)} }} }}")
        data = await self.execute(query, operation=("ids" if ids_only else "page"))
        return data.get(spec.root_field) or []

    async def fetch_cursor(self, spec: EntitySpec, after_value: Optional[int],
                           after_id: Optional[str] = None,
                           limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Field-cursor page. Two index-friendly modes (no `_or` compound keyset, which
        Hasura/Postgres cannot serve from the index and which 504s on large tables like
        transaction_action, ~200M rows):

          - advance (after_id falsy): WHERE cursor_field >= after_value,
            ORDER BY (cursor_field, id). Walks forward across cursor values. The boundary
            value's rows are re-read each page and deduped by ReplacingMergeTree. The caller
            advances after_value to the last row's cursor value.
          - drain (after_id given): WHERE cursor_field == after_value AND id > after_id,
            ORDER BY id. Pages through a single *dense* cursor value — one where more than
            page_size rows share the same value (e.g. day-boundary timestamps in
            transaction_action) — which the advance form cannot get past.
        """
        if not spec.cursor_field:
            raise GraphQLError(f"{spec.gql_type} has no cursor_field for fetch_cursor")
        limit = limit or spec.page_size
        cf = spec.cursor_field
        if after_id:
            where = (f"where: {{{cf}: {{_eq: {int(after_value or 0)}}}, "
                     f"id: {{_gt: {_gql_str(after_id)}}}}}, ")
            order = "order_by: {id: asc}"
        else:
            where = f"where: {{{cf}: {{_gte: {int(after_value or 0)}}}}}, "
            order = f"order_by: [{{{cf}: asc}}, {{id: asc}}]"
        query = (f"{{ {spec.root_field}({where}{order}, limit: {limit}) "
                 f"{{ {_selection(spec, False)} }} }}")
        data = await self.execute(query, operation="cursor")
        return data.get(spec.root_field) or []
