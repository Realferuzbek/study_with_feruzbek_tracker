# Emoji Rendering Diagnostics

- Each emoji token resolves via `PremiumEmojiResolver.emoji_for_key(key)` choosing a premium document, pinned Unicode glyph, or the default `NORMAL_SET` fallback without affecting other keys.
- Mixed output is safe: custom emoji entities are attached only for keys that provide a premium document ID; others render as Unicode in the same message sent by the premium Telethon user client.

Example (trimmed):

```python
import asyncio
from study_tracker import render_preview

preview = asyncio.run(render_preview())
print(preview.mode)                 # e.g. "mixed (per-key)"
print(preview.explain_table.splitlines()[:5])
# ['KEY | SOURCE', '----------------', 'BAR_CHART | PREMIUM_ID', 'FIRE | NORMAL_SET', 'MOON | PINNED_UNICODE']
```

Use `python tools/emoji_doctor.py` to list `MAPPED_PREMIUM`, `PINNED_UNICODE`, and `FALLING_BACK` counts before posting.

## Self-Healing Pipeline

- Resolver auto-hydrates on startup and every `HYDRATE_INTERVAL_MIN` (default 10) minutes using the pinned Saved Message fingerprint (message id + plain text + ordered custom emoji IDs).
- Cache schema v2 is stored in `premium_emoji_cache.json` via atomic writes (`*.tmp` then rename) and includes timestamps, fingerprint, and per-key `{premium_id, unicode}` entries.
- Read errors or schema mismatches mark the cache stale; the next refresh rebuilds automatically without manual deletion. If Telegram is unreachable, the last good cache stays in memory and `NORMAL_SET` fallbacks keep working.
- Every 24h the tracker logs a status line with premium/unicode/fallback counts and current fingerprint for quick health checks.

## Admin Commands (ADMIN_CHAT_ID)

- `.emoji status` – show current counts, fingerprint, and a trimmed preview table of sources per key.
- `.emoji refresh` – force an immediate hydrate and report whether the cache changed.
- `.post now` – queue one manual leaderboard post using the premium Telethon user client.
- `.logs tail 50` – return the latest 50 log lines (defaults to 50, accepts a custom count).

Tip: `python tools/emoji_doctor.py` outputs the same counts/fingerprint from the CLI.
