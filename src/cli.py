"""Command-line interface for envio_ga-indexer.

Verbs:
  migrate                              run SQL migrations
  introspect                           regenerate registry + typed DDL (+ drift diff)
  load backfill   [opts]               historical backfill
  load realtime   [opts]               continuous, strategy-routed ingestion
  reconcile       [--entities]         INV-1 delete detection (id-diff tombstones)
  maintain check  [--entities]         report state / gaps / failed pages
  maintain fix    [opts]               recover + re-queue + targeted re-ingest
  maintain reset  [--entities] [--status]   requeue pages or clear backfill state
  maintain reprocess [--entities]      re-derive typed tables from raw (no API)
  status                               progress overview
"""
import argparse
import asyncio

from src.config import config
from src.services.clickhouse import ClickHouse
from src.services.state import GaStateManager
from src.utils.logger import logger, setup_logger
from src import observability as obs

_CONSISTENT = {"select_sequential_consistency": 1}


def _entities(arg):
    return [e.strip() for e in arg.split(",") if e.strip()] if arg else None


def create_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="envio_ga-indexer")
    sub = p.add_subparsers(dest="command")

    sub.add_parser("migrate", help="run SQL migrations")
    sub.add_parser("introspect", help="regenerate registry + typed DDL")
    sub.add_parser("status", help="progress overview")

    load = sub.add_parser("load", help="load data").add_subparsers(dest="load_cmd")
    bf = load.add_parser("backfill")
    bf.add_argument("--entities"); bf.add_argument("--page-size", type=int)
    bf.add_argument("--concurrency", type=int); bf.add_argument("--restart", action="store_true")
    rt = load.add_parser("realtime")
    rt.add_argument("--entities"); rt.add_argument("--poll-interval", type=int)

    rec = sub.add_parser("reconcile", help="delete detection (id-diff)")
    rec.add_argument("--entities")

    maint = sub.add_parser("maintain", help="maintenance").add_subparsers(dest="maint_cmd")
    chk = maint.add_parser("check"); chk.add_argument("--entities")
    fix = maint.add_parser("fix")
    fix.add_argument("--entities"); fix.add_argument("--id-range"); fix.add_argument("--block-range")
    fix.add_argument("--dry-run", action="store_true")
    rst = maint.add_parser("reset"); rst.add_argument("--entities"); rst.add_argument("--status")
    rep = maint.add_parser("reprocess"); rep.add_argument("--entities")

    return p


def print_status(ch: ClickHouse = None):
    ch = ch or ClickHouse()
    state = GaStateManager(ch)
    rows = state.summary()
    print(f"{'entity':<28} {'backfill':<9} {'pages':>6} {'rows':>10} {'failed':>6} {'dead':>5} {'live_rows':>12}")
    print("-" * 90)
    for r in rows:
        if r["entity"].startswith("_"):
            continue
        try:
            live = ch.query_value(
                f"SELECT count(DISTINCT id) FROM {r['entity']} FINAL WHERE _deleted=0",
                default=0, settings=_CONSISTENT)
        except Exception:
            live = "?"
        bf = "complete" if r["backfill_complete"] else "partial"
        print(f"{r['entity']:<28} {bf:<9} {r['completed']:>6} {r['rows_indexed']:>10} "
              f"{r['failed']:>6} {r['dead']:>5} {str(live):>12}")


def main(argv=None):
    setup_logger()
    if config.METRICS_ENABLED:
        try:
            obs.start_metrics_server(config.METRICS_PORT)
            obs.update_health(status="ok", operation="cli")
        except Exception as e:
            logger.warning("metrics server failed to start", error=str(e))

    args = create_parser().parse_args(argv)
    cmd = args.command
    if not cmd:
        create_parser().print_help()
        return

    if cmd == "migrate":
        from scripts.migrate import run_migrations
        run_migrations()
    elif cmd == "introspect":
        from scripts import introspect
        introspect.main()
    elif cmd == "status":
        print_status()
    elif cmd == "load":
        from src.services.loader import LoaderService
        svc = LoaderService()
        try:
            if args.load_cmd == "backfill":
                asyncio.run(svc.backfill(_entities(args.entities), args.concurrency,
                                         args.page_size, args.restart))
            elif args.load_cmd == "realtime":
                asyncio.run(svc.realtime(_entities(args.entities), args.poll_interval))
            else:
                print("usage: load [backfill|realtime]")
        finally:
            svc.close()
    elif cmd == "reconcile":
        from src.services.maintenance import MaintenanceService
        svc = MaintenanceService()
        try:
            asyncio.run(svc.reconcile(_entities(args.entities)))
        finally:
            svc.close()
    elif cmd == "maintain":
        from src.services.maintenance import MaintenanceService
        svc = MaintenanceService()
        try:
            if args.maint_cmd == "check":
                _print_check(svc.check(_entities(args.entities)))
            elif args.maint_cmd == "fix":
                actions = asyncio.run(svc.fix(_entities(args.entities), args.id_range,
                                              args.block_range, args.dry_run))
                for a in actions:
                    print(" -", a)
            elif args.maint_cmd == "reset":
                print(svc.reset(_entities(args.entities), args.status))
            elif args.maint_cmd == "reprocess":
                print(svc.reprocess(_entities(args.entities)))
            else:
                print("usage: maintain [check|fix|reset|reprocess]")
        finally:
            svc.close()


def _print_check(report):
    print("== state summary ==")
    for r in report["summary"]:
        print(f"  {r['entity']:<28} completed={r['completed']} failed={r['failed']} "
              f"dead={r['dead']} claimed={r['claimed']} backfill_complete={r['backfill_complete']}")
    if report["gaps"]:
        print("== keyset gaps ==")
        for e, g in report["gaps"].items():
            print(f"  {e}: {len(g)} gap(s) at {g[:5]}")
    for key in ("failed", "dead", "stuck"):
        if report[key]:
            print(f"== {key} pages ({len(report[key])}) ==")
            for r in report[key][:20]:
                print(f"  {r['entity']} pk={r['partition_key']} cursor_start={r['cursor_start']} "
                      f"attempts={r['attempt_count']}")
