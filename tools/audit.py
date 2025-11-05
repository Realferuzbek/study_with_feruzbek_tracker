from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import study_tracker as tracker  # noqa: E402
from tools.audit_runner import AuditRunner  # noqa: E402


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Study With Feruzbek audit suite",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--quick", action="store_true", help="Run quick audit (no DB reads).")
    group.add_argument("--full", action="store_true", help="Run full audit (A..I).")
    return parser.parse_args()


async def _run_audit(args: argparse.Namespace) -> AuditRunner:
    runner = AuditRunner(tracker)
    report = await runner.run(quick=args.quick)
    print(report.format_summary())
    if report.has_fail:
        raise SystemExit(1)
    return runner


def main() -> None:
    args = _parse_args()
    try:
        asyncio.run(_run_audit(args))
    except SystemExit:
        raise
    except KeyboardInterrupt:
        print("\nInterrupted.")
        sys.exit(130)


if __name__ == "__main__":
    main()
