import asyncio
import sys
from pathlib import Path
from typing import Dict, List

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from emojis_runtime import NORMAL_SET, PremiumEmojiResolver

try:
    from study_tracker import client, ensure_connected
except ImportError:  # pragma: no cover
    client = None
    ensure_connected = None  # type: ignore[assignment]


async def _rehydrate() -> None:
    if client is None or ensure_connected is None:
        return
    try:
        await ensure_connected()
        changed = await PremiumEmojiResolver.refresh_if_needed(client, force=True)
        if not PremiumEmojiResolver.last_refresh_success():
            print("[emoji_doctor] hydration failed; using cached data")
        elif changed:
            print(f"[emoji_doctor] cache refreshed fingerprint={PremiumEmojiResolver.fingerprint_short()}")
    except Exception as exc:  # pragma: no cover - diagnostics only
        print(f"[emoji_doctor] hydration skipped: {exc}")


def _format_list(title: str, keys: List[str]) -> str:
    body = ", ".join(keys) if keys else "(none)"
    return f"{title} ({len(keys)}): {body}"


async def main() -> None:
    await _rehydrate()

    breakdown: Dict[str, List[str]] = PremiumEmojiResolver.resolution_breakdown()
    mapped = breakdown.get("MAPPED_PREMIUM", [])
    unicode_keys = breakdown.get("PINNED_UNICODE", [])
    fallback = breakdown.get("FALLING_BACK", [])
    counts = PremiumEmojiResolver.counts()

    print(_format_list("MAPPED_PREMIUM", mapped))
    print(_format_list("PINNED_UNICODE", unicode_keys))
    print(_format_list("FALLING_BACK", fallback))
    print(f"Emoji mode: {PremiumEmojiResolver.resolution_mode()}")
    print(f"Fingerprint: {PremiumEmojiResolver.fingerprint_short()} (updated {PremiumEmojiResolver.last_updated()})")
    print(f"Counts: {counts}")
    print(f"Tracked keys: {len(NORMAL_SET)}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as exc:  # pragma: no cover - diagnostics should not crash
        print(f"[emoji_doctor] unexpected error: {exc}")
