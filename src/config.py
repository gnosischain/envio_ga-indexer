"""Central configuration loaded from environment / .env (python-dotenv).

All values are plain env vars — no validation library — mirroring beacon-indexer.
"""
import os
from typing import List
from dotenv import load_dotenv

load_dotenv()


def _bool(name: str, default: str = "false") -> bool:
    return os.getenv(name, default).strip().lower() in ("1", "true", "yes", "on")


def _csv(name: str, default: str = "") -> List[str]:
    raw = os.getenv(name, default).strip()
    if not raw:
        return []
    return [x.strip() for x in raw.split(",") if x.strip()]


class Config:
    # ── GraphQL source (Hasura / Envio HyperIndex) ──────────────────────────
    GRAPHQL_ENDPOINT = os.getenv("GRAPHQL_ENDPOINT", "")
    GRAPHQL_API_KEY = os.getenv("GRAPHQL_API_KEY", "").strip()
    GRAPHQL_AUTH_HEADER = os.getenv("GRAPHQL_AUTH_HEADER", "Authorization").strip()
    GRAPHQL_AUTH_SCHEME = os.getenv("GRAPHQL_AUTH_SCHEME", "Bearer").strip()

    # ── Pagination & throughput ─────────────────────────────────────────────
    PAGE_SIZE = int(os.getenv("PAGE_SIZE", "1000"))          # hard server cap is 1000
    BACKFILL_CONCURRENCY = int(os.getenv("BACKFILL_CONCURRENCY", "4"))
    GQL_MAX_RPS = float(os.getenv("GQL_MAX_RPS", "10"))
    GQL_MAX_RETRIES = int(os.getenv("GQL_MAX_RETRIES", "5"))
    GQL_TIMEOUT = int(os.getenv("GQL_TIMEOUT", "120"))       # per-request total timeout (s)

    # ── Entity selection (empty = all enabled entities in the registry) ─────
    ENABLED_ENTITIES = _csv("ENABLED_ENTITIES")

    # ── Continuous ingestion & reconcile cadence ────────────────────────────
    POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "30"))
    REALTIME_OVERLAP_BLOCKS = int(os.getenv("REALTIME_OVERLAP_BLOCKS", "5"))
    RESCAN_INTERVAL_S = int(os.getenv("RESCAN_INTERVAL_S", "300"))
    RECONCILE_INTERVAL_S = int(os.getenv("RECONCILE_INTERVAL_S", "86400"))

    # ── ClickHouse (managed instance — required) ────────────────────────────
    CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
    CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8443"))
    CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
    CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
    CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "envio_ga")
    CLICKHOUSE_SECURE = _bool("CLICKHOUSE_SECURE", "true")
    CLICKHOUSE_TIMEOUT = int(os.getenv("CLICKHOUSE_TIMEOUT", "120"))  # send/receive timeout (s)

    # ── Raw audit log retention (0 = no TTL, keep forever) ──────────────────
    RAW_TTL_DAYS = int(os.getenv("RAW_TTL_DAYS", "0"))

    # ── Observability & logging ─────────────────────────────────────────────
    METRICS_ENABLED = _bool("METRICS_ENABLED", "true")
    METRICS_PORT = int(os.getenv("METRICS_PORT", "9090"))
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")


config = Config()
