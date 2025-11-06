# Emoji & Layout Notes

- The leaderboard message now relies exclusively on the Unicode glyphs defined in `NORMAL_SET`.
- `render_preview_layout()` returns the HTML and plain-text view that will be posted; `_audit_layout_text()` validates the formatting.
- Admins can run `.layout audit` in Telegram (or `python tools/audit.py --quick`) to confirm the layout before posting.

## Admin Commands (ADMIN_CHAT_ID)

- `.audit quick|full` – runs the lightweight audit from `tools/audit_runner.py`.
- `.layout audit` – checks the current preview text for formatting issues.
- `.layout preview` – sends the raw HTML preview to chat.
- `.post now` – queues a manual leaderboard post without touching the daily flag.
- `.logs tail [n]` – echoes the last `n` lines from `tracker.log` (defaults to 50).
