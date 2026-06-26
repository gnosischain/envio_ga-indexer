"""Prometheus metrics + lightweight health endpoints for envio_ga-indexer.

Metrics are namespaced `envio_ga_*` and labelled by `entity` (not slot/fork).
Adapted from beacon-indexer's observability module.
"""
import json
import logging
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Dict

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)

API_LATENCY_BUCKETS = (0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300)
PAGE_DURATION_BUCKETS = (0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60)

# ── GraphQL client ──────────────────────────────────────────────────────────
graphql_requests_total = Counter(
    "envio_ga_graphql_requests_total",
    "GraphQL requests by operation and status",
    ["operation", "status"],
)
graphql_request_duration_seconds = Histogram(
    "envio_ga_graphql_request_duration_seconds",
    "GraphQL request duration by operation",
    ["operation"],
    buckets=API_LATENCY_BUCKETS,
)

# ── Source head / liveness ──────────────────────────────────────────────────
chain_head_block = Gauge(
    "envio_ga_chain_head_block",
    "Latest block_height reported by chain_metadata",
)

# ── Pages / pipeline ────────────────────────────────────────────────────────
pages_total = Counter(
    "envio_ga_pages_total",
    "Page state transitions",
    ["entity", "status"],
)
page_duration_seconds = Histogram(
    "envio_ga_page_duration_seconds",
    "Page fetch+write duration",
    ["entity"],
    buckets=PAGE_DURATION_BUCKETS,
)
rows_written_total = Counter(
    "envio_ga_rows_written_total",
    "Rows written to a table (typed or raw)",
    ["table"],
)
entity_rows = Gauge(
    "envio_ga_entity_rows",
    "Live (non-deleted) distinct id count per entity, last observed",
    ["entity"],
)

# ── Realtime / staleness ────────────────────────────────────────────────────
entity_staleness_seconds = Gauge(
    "envio_ga_entity_staleness_seconds",
    "Seconds since last successful cursor advance / rescan per entity",
    ["entity"],
)
entity_watermark = Gauge(
    "envio_ga_entity_watermark",
    "Numeric watermark (last_block / last_field_value) per entity",
    ["entity"],
)

# ── Reconcile (INV-1 delete detection) ──────────────────────────────────────
reconcile_added_total = Counter(
    "envio_ga_reconcile_added_total",
    "Ids newly seen during reconcile",
    ["entity"],
)
reconcile_tombstoned_total = Counter(
    "envio_ga_reconcile_tombstoned_total",
    "Ids tombstoned (disappeared upstream) during reconcile",
    ["entity"],
)

# ── Schema drift / parse ────────────────────────────────────────────────────
unknown_fields_total = Counter(
    "envio_ga_unknown_fields_total",
    "Unknown upstream fields seen at parse time (schema drift signal)",
    ["entity"],
)

_health_state: Dict[str, Any] = {
    "status": "starting",
    "clickhouse_connected": False,
    "graphql_connected": False,
    "operation": "",
}
_health_lock = threading.Lock()
_metrics_server = None


def update_health(**kwargs):
    with _health_lock:
        _health_state.update(kwargs)


def get_health() -> Dict[str, Any]:
    with _health_lock:
        return dict(_health_state)


class MetricsHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/metrics":
            output = generate_latest()
            self.send_response(200)
            self.send_header("Content-Type", CONTENT_TYPE_LATEST)
            self.end_headers()
            self.wfile.write(output)
            return
        if self.path == "/health":
            health = get_health()
            status_code = 200 if health.get("status") not in {"failed", "error"} else 503
            self.send_response(status_code)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(health, default=str).encode("utf-8"))
            return
        self.send_response(404)
        self.end_headers()

    def log_message(self, format, *args):
        pass


def start_metrics_server(port: int = 9090):
    """Start the metrics/health server once in a background thread."""
    global _metrics_server
    if _metrics_server is not None:
        return _metrics_server
    server = HTTPServer(("0.0.0.0", port), MetricsHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    _metrics_server = server
    logging.getLogger(__name__).info("Metrics server started on port %s", port)
    return server
