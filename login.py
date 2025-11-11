import os
from pathlib import Path

from telethon import TelegramClient


def _load_local_env() -> None:
    """Populate os.environ from .env.local without overriding existing vars."""
    env_path = Path(__file__).with_name(".env.local")
    try:
        data = env_path.read_text(encoding="utf-8")
    except OSError:
        return
    for raw_line in data.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            continue
        value = value.strip()
        if value and value[0] == value[-1] and value[0] in ("'", '"'):
            value = value[1:-1]
        os.environ.setdefault(key, value)


def _require(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Set {name} before running login.py")
    return value


_load_local_env()
api_id = int(_require("TELEGRAM_API_ID"))
api_hash = _require("TELEGRAM_API_HASH")
session_name = os.getenv("TELEGRAM_SESSION_NAME", "study_session")
client = TelegramClient(session_name, api_id, api_hash)


async def main():
    me = await client.get_me()
    print("Logged in as:", me.first_name, "(id:", me.id, ")")


with client:
    client.loop.run_until_complete(main())
