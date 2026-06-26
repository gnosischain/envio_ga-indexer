#!/usr/bin/env python3
"""Run SQL migrations against the configured ClickHouse database.

Adapted from beacon-indexer: discover migrations/*.sql in lexical order, split
on ';', run each statement (SET handled separately). Comment-only fragments are
skipped so trailing `-- ...` notes after the last ';' don't error.
"""
import os
import sys

# Ensure the repo root is importable whether run as a module or a script.
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from src.services.clickhouse import ClickHouse  # noqa: E402
from src.utils.logger import logger, setup_logger  # noqa: E402


def _migrations_dir() -> str:
    for cand in ("/app/migrations", os.path.join(_ROOT, "migrations"), "migrations"):
        if os.path.isdir(cand):
            return cand
    raise FileNotFoundError("migrations directory not found")


def _strip_comments(sql: str) -> str:
    """Remove full-line `-- ...` comments before splitting on ';'.

    Done file-wide (not per-fragment) so a semicolon inside a comment can't break
    statement splitting.
    """
    return "\n".join(ln for ln in sql.splitlines() if not ln.strip().startswith("--"))


def run_migrations(clickhouse: ClickHouse = None):
    setup_logger()
    ch = clickhouse or ClickHouse()
    migrations_dir = _migrations_dir()

    files = sorted(f for f in os.listdir(migrations_dir) if f.endswith(".sql"))
    logger.info("Starting migrations", files=files)

    for fname in files:
        path = os.path.join(migrations_dir, fname)
        with open(path, "r") as f:
            sql = _strip_comments(f.read())

        statements = [s.strip() for s in sql.split(";") if s.strip()]
        logger.info("Running migration", file=fname, statements=len(statements))

        for stmt in statements:
            try:
                ch.client.command(stmt)
            except Exception as e:
                logger.error("Migration statement failed", file=fname,
                             statement=stmt[:120], error=str(e))
                raise

    logger.info("All migrations completed successfully")


if __name__ == "__main__":
    run_migrations()
