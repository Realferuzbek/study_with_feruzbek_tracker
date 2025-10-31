"""Optional webhook export for leaderboard snapshots."""

from __future__ import annotations

import json
import logging
import os
import threading
import urllib.request
from typing import Any, Dict


_LOGGER = logging.getLogger("tracker")

_TRUE_VALUES = {"1", "true", "yes", "on"}


def _should_export() -> bool:
    enabled = os.getenv("LEADERBOARD_WEB_EXPORT_ENABLED", "").strip().lower()
    if enabled not in _TRUE_VALUES:
        return False
    if not os.getenv("LEADERBOARD_INGEST_URL"):
        return False
    if not os.getenv("LEADERBOARD_INGEST_SECRET"):
        return False
    return True


def _timeout_seconds() -> float:
    raw = os.getenv("LEADERBOARD_EXPORT_TIMEOUT_MS", "1500").strip()
    try:
        millis = int(raw)
    except ValueError:
        millis = 1500
    if millis < 0:
        millis = 0
    return millis / 1000.0


def _post_snapshot(snapshot: Dict[str, Any]) -> None:
    url = os.getenv("LEADERBOARD_INGEST_URL")
    secret = os.getenv("LEADERBOARD_INGEST_SECRET")
    if not url or not secret:
        return

    payload = {
        "posted_at": snapshot.get("posted_at"),
        "source": "tracker",
        "message_id": snapshot.get("message_id"),
        "chat_id": snapshot.get("chat_id"),
        "boards": snapshot.get("boards", []),
    }

    try:
        data = json.dumps(payload).encode("utf-8")
    except Exception as exc:  # pragma: no cover
        _LOGGER.warning("[export] failed: %s", exc)
        return

    req = urllib.request.Request(
        url,
        data=data,
        headers={
            "Content-Type": "application/json",
            "X-Leaderboard-Secret": secret,
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=_timeout_seconds()) as resp:
            resp.read()
        _LOGGER.info("[export] sent")
    except Exception as exc:  # pragma: no cover - network failures are logged
        _LOGGER.warning("[export] failed: %s", exc)


def export_latest_leaderboards(snapshot: Dict[str, Any]) -> None:
    """Export the latest leaderboard snapshot to the ingest endpoint."""

    if not _should_export():
        return

    thread = threading.Thread(
        target=_post_snapshot,
        args=(snapshot.copy(),),
        daemon=True,
    )
    thread.start()

