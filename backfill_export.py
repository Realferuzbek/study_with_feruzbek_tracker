from __future__ import annotations

import argparse
import asyncio
import logging
import re
import sqlite3
import time as _time
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Iterable

from env_loader import load_project_env
from study_tracker import DB_PATH, POST_HOUR, POST_MINUTE, TZ, build_leaderboard_snapshot
from web_export import build_export_payload, send_export

load_project_env()

logger = logging.getLogger("tracker")

DEFAULT_WINDOW_DAYS = 60
LOG_FILES: list[Path] = [Path("var/tracker.log"), Path("var/tracker_2.log")]
_AUTO_RE = re.compile(r"Posted leaderboard for (\d{4}-\d{2}-\d{2}) \(mark_daily=True\)")
_VALID_SCOPES = {"day", "week", "month"}


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


def _auto_dates_from_logs() -> set[date]:
    dates: set[date] = set()
    for path in LOG_FILES:
        try:
            data = path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        for match in _AUTO_RE.finditer(data):
            try:
                dates.add(date.fromisoformat(match.group(1)))
            except ValueError:
                continue
    return dates


def _dates_with_tracked_seconds() -> set[date]:
    if not DB_PATH:
        return set()
    try:
        con = sqlite3.connect(DB_PATH)
    except Exception:
        return set()
    try:
        rows = con.execute("SELECT DISTINCT d FROM seconds_totals").fetchall()
    finally:
        con.close()
    dates: set[date] = set()
    for (d_str,) in rows:
        try:
            dates.add(date.fromisoformat(d_str))
        except ValueError:
            continue
    return dates


def _candidate_dates(start_date: date, end_date: date) -> Iterable[tuple[date, str]]:
    log_dates = _auto_dates_from_logs()
    data_dates = _dates_with_tracked_seconds()
    earliest_logged = min(log_dates) if log_dates else None
    for d in _iter_dates(start_date, end_date):
        if d in log_dates:
            yield d, "log"
        elif earliest_logged and d >= earliest_logged:
            continue
        elif d in data_dates:
            yield d, "db"


def _validate_payload(payload: dict) -> tuple[bool, str]:
    if payload.get("source") != "tracker":
        return False, "source mismatch"
    if not isinstance(payload.get("posted_at"), str):
        return False, "posted_at missing"
    boards = payload.get("boards")
    if not isinstance(boards, list) or not boards:
        return False, "boards missing/empty"
    for board in boards:
        if not isinstance(board, dict):
            return False, "board not an object"
        if board.get("scope") not in _VALID_SCOPES:
            return False, "invalid scope"
        if not isinstance(board.get("period_start"), str) or not isinstance(board.get("period_end"), str):
            return False, "missing period bounds"
        entries = board.get("entries")
        if not isinstance(entries, list):
            return False, "entries missing"
        for entry in entries:
            if not isinstance(entry, dict):
                return False, "entry not an object"
            required_fields = ("rank", "user_id", "minutes", "seconds")
            if any(k not in entry for k in required_fields):
                return False, "entry missing required fields"
    return True, "ok"


async def _run_backfill(start_date: date, end_date: date, *, inspect: bool) -> None:
    targets = list(_candidate_dates(start_date, end_date))
    if not targets:
        logger.info("backfill_export: no automatic snapshots found in range")
        return

    for d, origin in targets:
        snapshot_dt = datetime.combine(d, time(POST_HOUR, POST_MINUTE), tzinfo=TZ)
        try:
            snapshot = await build_leaderboard_snapshot(snapshot_dt)
            payload = build_export_payload(snapshot)
            ok, detail = _validate_payload(payload)
            if not ok:
                logger.warning("backfill_export: skipping %s (%s)", d.isoformat(), detail)
                continue

            boards = payload.get("boards", [])
            if inspect:
                for board in boards:
                    entries = board.get("entries") or []
                    scope = board.get("scope")
                    print(
                        f"{d.isoformat()} scope={scope} entries={len(entries)} "
                        f"chat_id={payload.get('chat_id')} message_id={payload.get('message_id')} origin={origin}"
                    )
                continue

            resp = send_export(snapshot, capture_response=True)
            status, body = (resp or (None, "")) if isinstance(resp, tuple) else (None, "")
            scopes = ",".join(str(b.get("scope")) for b in boards if isinstance(b, dict))
            if status is None:
                logger.info(
                    "backfill_export: sent snapshot for %s scopes=[%s]",
                    d.isoformat(),
                    scopes,
                )
            elif 200 <= status < 300:
                logger.info(
                    "backfill_export: sent snapshot for %s scopes=[%s] status=%s",
                    d.isoformat(),
                    scopes,
                    status,
                )
            else:
                logger.warning(
                    "backfill_export: send failed for %s scopes=[%s] status=%s body=%s",
                    d.isoformat(),
                    scopes,
                    status,
                    body,
                )
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
    parser.add_argument(
        "--inspect",
        action="store_true",
        help="Dry-run mode: print snapshots that would be sent without calling the ingest API.",
    )
    args = parser.parse_args()

    start_date, end_date = _resolve_range(args, parser)
    logger.info(
        "backfill_export: exporting snapshots from %s to %s",
        start_date.isoformat(),
        end_date.isoformat(),
    )
    asyncio.run(_run_backfill(start_date, end_date, inspect=args.inspect))


if __name__ == "__main__":
    main()
