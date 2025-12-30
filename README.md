# Focus Squad

Python-based Telegram Study Tracker for the Focus Squad community. Tracks Telegram video chat presence via Telethon, stores totals in local SQLite (`var/study.db`), posts daily/weekly/monthly leaderboards to Telegram, and optionally exports snapshots to a webhook for a web dashboard. The repo also includes a minimal Next.js 14 API route (`/api/admin/state`) used for session state checks.

## Getting Started (Tracker)

1. Create a virtualenv and install requirements:
   ```bash
   python -m venv .venv
   .\.venv\Scripts\pip.exe install -r requirements.txt
   ```
2. Copy `.env.example` to `.env.local` and fill the required values (or use `scripts/secure_env_tool.py` for secrets).
3. Create a Telethon session with `python tg_session_qr.py` (QR login) or `python login.py` (interactive).
4. Run the tracker:
   ```bash
   python study_tracker.py
   ```

## Getting Started (Web API, optional)

1. Install Node dependencies:
   ```bash
   npm install
   ```
2. Set `CRON_SECRET` if you call `/api/admin/state`.
3. Run the dev server:
   ```bash
   npm run dev
   ```

## Environment Variables

Required (tracker):
- `TELEGRAM_API_ID`
- `TELEGRAM_API_HASH`
- `TELEGRAM_GROUP_USERNAME`

Optional:
- `TG_STRING_SESSION` (use a StringSession instead of a local `.session` file)
- `TELEGRAM_BOT_TOKEN`, `TELEGRAM_BOT_USERNAME`, `TELEGRAM_BOT_TARGET` or `TELEGRAM_GROUP_ID`
- `LEADERBOARD_WEB_EXPORT_ENABLED`
- `LEADERBOARD_INGEST_URL`
- `LEADERBOARD_INGEST_SECRET`
- `LEADERBOARD_EXPORT_TIMEOUT_MS`
- `CRON_SECRET`

## Managing Secrets Safely

Secrets live in `var/secure_env.dat`, encrypted with Windows DPAPI. Use the helper to manage them:

```bash
python scripts/secure_env_tool.py list
python scripts/secure_env_tool.py set TELEGRAM_API_HASH your_value_here
```

`.env.local` should only contain non-sensitive defaults. To migrate an existing plaintext file into the encrypted store run `python scripts/secure_env_tool.py migrate`.
