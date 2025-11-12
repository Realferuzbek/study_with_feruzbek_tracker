#!/usr/bin/env python
"""
Manage DPAPI-protected environment secrets.

Usage:
  python scripts/secure_env_tool.py migrate
  python scripts/secure_env_tool.py list
  python scripts/secure_env_tool.py set KEY VALUE
  python scripts/secure_env_tool.py get KEY
  python scripts/secure_env_tool.py delete KEY
"""

from __future__ import annotations

import argparse
import getpass
import os
from pathlib import Path
from typing import Dict

import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from secure_env import SecureEnvStore  # noqa: E402
from env_loader import _parse_env_file  # type: ignore  # noqa: E402

BASE_DIR = Path(__file__).resolve().parents[1]
ENV_PATH = BASE_DIR / ".env.local"
BACKUP_PATH = BASE_DIR / ".env.local.backup"
store = SecureEnvStore()


def cmd_list(args: argparse.Namespace) -> None:
    data = store.load()
    if not data:
        print("Secure store is empty.")
        return
    for key in sorted(data):
        print(key)


def cmd_get(args: argparse.Namespace) -> None:
    data = store.load()
    value = data.get(args.key)
    if value is None:
        raise SystemExit(f"{args.key} not set.")
    print(value)


def cmd_set(args: argparse.Namespace) -> None:
    value = args.value
    if value is None:
        value = getpass.getpass(f"Value for {args.key}: ")
    store.set(args.key, value)
    print(f"Stored {args.key}.")


def cmd_delete(args: argparse.Namespace) -> None:
    store.set(args.key, None)


def _rewrite_env_local(non_secret: Dict[str, str]) -> None:
    lines = [
        "# Sensitive secrets now live in var/secure_env.dat (DPAPI protected).",
        "# Use `python scripts/secure_env_tool.py set KEY VALUE` to manage secrets.",
        "",
    ]
    for key in sorted(non_secret):
        lines.append(f"{key}={non_secret[key]}")
    ENV_PATH.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def cmd_migrate(args: argparse.Namespace) -> None:
    if not ENV_PATH.exists():
        raise SystemExit(".env.local not found to migrate.")
    entries = _parse_env_file(ENV_PATH)
    if not entries:
        raise SystemExit(".env.local is empty; nothing to migrate.")

    BACKUP_PATH.write_text(ENV_PATH.read_text(encoding="utf-8"), encoding="utf-8")
    print(f"Backup written to {BACKUP_PATH}")

    for key, value in entries.items():
        store.set(key, value)
    print(f"Migrated {len(entries)} entries into secure store.")

    non_secret = {
        "NEXT_PUBLIC_TZ": entries.get("NEXT_PUBLIC_TZ", "Asia/Tashkent"),
        "LEADERBOARD_WEB_EXPORT_ENABLED": entries.get("LEADERBOARD_WEB_EXPORT_ENABLED", "false"),
        "LEADERBOARD_EXPORT_TIMEOUT_MS": entries.get("LEADERBOARD_EXPORT_TIMEOUT_MS", "1500"),
    }
    ingest_url = entries.get("LEADERBOARD_INGEST_URL", "").strip() or "https://studywithferuzbek.vercel.app/api/leaderboard/ingest"
    non_secret["LEADERBOARD_INGEST_URL"] = ingest_url.replace("https://https://", "https://")
    _rewrite_env_local(non_secret)
    print(".env.local rewritten with public-safe defaults.")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Secure env store manager")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("list").set_defaults(func=cmd_list)

    get_p = sub.add_parser("get")
    get_p.add_argument("key")
    get_p.set_defaults(func=cmd_get)

    set_p = sub.add_parser("set")
    set_p.add_argument("key")
    set_p.add_argument("value", nargs="?")
    set_p.set_defaults(func=cmd_set)

    del_p = sub.add_parser("delete")
    del_p.add_argument("key")
    del_p.set_defaults(func=cmd_delete)

    sub.add_parser("migrate").set_defaults(func=cmd_migrate)
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
