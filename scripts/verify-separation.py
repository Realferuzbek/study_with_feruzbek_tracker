#!/usr/bin/env python3
"""Ensure website paths stay isolated to web_export.py."""

from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TRACKED_PATTERNS = (
    "apps/website",
    "apps.website",
    "website/",
    "website\\",
)


def main() -> int:
    offenders: list[tuple[Path, str]] = []
    for path in ROOT.rglob("*.py"):
        if path.name == "web_export.py":
            continue
        if not path.is_file():
            continue
        rel_path = path.relative_to(ROOT)
        if rel_path == Path("scripts/verify-separation.py"):
            continue
        text = path.read_text(encoding="utf-8", errors="ignore")
        for pattern in TRACKED_PATTERNS:
            if pattern in text:
                offenders.append((rel_path, pattern))
                break

    if offenders:
        sys.stderr.write(
            "Website integration must stay isolated to web_export.py.\n"
        )
        for rel_path, pattern in offenders:
            sys.stderr.write(f"  {rel_path} → {pattern}\n")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

