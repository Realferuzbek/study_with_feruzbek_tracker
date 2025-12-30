# Focus Squad Study Tracker

A Python-based Telegram study tracker that measures real participation in a study group and publishes daily/weekly/monthly leaderboards—with an optional web dashboard export.

**For:** online study communities that want accountability without manual spreadsheets.

## Demo

- Web leaderboard (dashboard): https://studywithferuzbek.vercel.app/leaderboard
- Telegram group (leaderboard posts): https://t.me/studywithferuzbek

## Screenshots

> Add images under `docs/screenshots/` and update paths below.

- `docs/screenshots/dashboard.png`
- `docs/screenshots/telegram_post.png`

---

## Features

- Tracks Telegram voice/video chat presence using Telethon (event-driven + polling fallback).
- Stores per-user study totals in local SQLite (`var/study.db`).
- Computes leaderboards for day / week / rolling 30 days.
- Daily auto-post at 21:30 Asia/Tashkent (plus optional manual “post now”).
- Alias/username merge support (e.g., multiple accounts → one identity).
- Minimum-session gate (prevents tiny joins from skewing rankings).
- Generates snapshot payloads and optionally exports to a webhook for a web dashboard.
- Includes a minimal Next.js API endpoint for admin/state checks (`/api/admin/state`) (optional).

---

## Tech Stack

**Tracker (core)**
- Python
- Telethon (Telegram client)
- SQLite (local persistence)

**Optional tooling**
- PowerShell / VBScript wrappers (Windows)
- Next.js 14 (minimal API route for admin/state)

---

## How It Works

1. A Telethon client listens to Telegram group call updates and maintains a presence model.
2. A periodic fallback snapshot poll (e.g., ~30s) ensures robustness if raw updates are missed.
3. Session seconds are accumulated per user and persisted to `var/study.db`.
4. At 21:30 Asia/Tashkent, the tracker computes leaderboard windows and renders a post.
5. The post is sent to Telegram either via:
   - Bot API (if configured), or
   - the authenticated Telethon user session.
6. If enabled, the tracker exports the snapshot to a webhook for a dashboard/history view.

---

## Evidence / Proof

- Daily auto-post schedule is configured in code as 21:30 Asia/Tashkent (`POST_HOUR=21`, `POST_MINUTE=30`, `TZ`).
- Persistent storage is local SQLite (`var/study.db`) for auditable, replayable totals.
- Supports both event-driven updates and a polling fallback for reliability.
- Secrets are designed to stay out of git via `.env.local` and an optional Windows DPAPI-backed store (`var/secure_env.dat`).

> If you want admissions-grade metrics (uptime, active users, posts/day, latency), see “Measurement Plan” in the Roadmap section below.

---

## My Role & Contributions

**Primary author (solo): Feruzbek Qurbonov**

I owned:
- System design: presence tracking → persistence → leaderboard computation → posting pipeline
- Reliability strategy: event updates + polling fallback
- Automation: daily scheduled posting and manual override flow
- Reproducibility and repo hygiene: env scaffolding, script portability, and setup clarity

---

## Roadmap

**Short-term (1–2 weeks)**
- Add a reproducible “Measurement Plan” script: post latency, run stability, and error rate logging.
- Add tests for leaderboard aggregation and edge cases (alias merge, minimum-session gate).
- Add a `docs/screenshots/` folder and keep README visuals up to date.

**Mid-term (1–2 months)**
- Standardize the webhook export schema and publish a reference dashboard receiver.
- Add a lightweight admin UI for reviewing/merging aliases safely.

**Long-term**
- Research: anomaly detection for suspicious sessions (e.g., bot-like joins) and fairness-aware ranking.
- Multi-community support with isolated configs and storage per group.

---

## Configuration

Create `.env.local` from `.env.example`:

```bash
cp .env.example .env.local
