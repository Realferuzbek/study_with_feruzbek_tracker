from __future__ import annotations

import argparse
import asyncio
import logging
import time as _time
from datetime import date, datetime, time, timedelta

from env_loader import load_project_env
from study_tracker import POST_HOUR, POST_MINUTE, TZ, build_leaderboard_snapshot
from web_export import send_export

load_project_env()

logger = logging.getLogger("tracker")

DEFAULT_WINDOW_DAYS = 60


def _parse_date(value: str) -> date:
    try:
        return datetime.fromisoformat(value).date()
    except ValueError as exc:
        raise argparse.ArgumentTypeError("Expected YYYY-MM-DD") from exc


def _resolve_range(args, parser: argparse.ArgumentParser) -> tuple[date, date]:
    today = datetime.now(TZ).date()
    end_date = args.end or (today - timedelta(days=1))
    start_date = args.start or (end_date - timedelta(days=DEFAULT_WINDOW_DAYS))
    if start_date > end_date:
        parser.error("--start must be on or before --end")
    return start_date, end_date


def _iter_dates(start: date, end: date):
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)


async def _run_backfill(start_date: date, end_date: date) -> None:
    for d in _iter_dates(start_date, end_date):
        snapshot_dt = datetime.combine(d, time(POST_HOUR, POST_MINUTE), tzinfo=TZ)
        try:
            snapshot = await build_leaderboard_snapshot(snapshot_dt)
            send_export(snapshot)
            logger.info("backfill_export: sent snapshot for %s", d.isoformat())
        except Exception as exc:
            logger.error("backfill_export: failed for %s: %r", d.isoformat(), exc)
        _time.sleep(0.2)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Replay historical leaderboard snapshots to the ingest endpoint.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--start", type=_parse_date, help="Inclusive start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=_parse_date, help="Inclusive end date (YYYY-MM-DD)")
    args = parser.parse_args()

    start_date, end_date = _resolve_range(args, parser)
    logger.info(
        "backfill_export: exporting snapshots from %s to %s",
        start_date.isoformat(),
        end_date.isoformat(),
    )
    asyncio.run(_run_backfill(start_date, end_date))


if __name__ == "__main__":
    main()
