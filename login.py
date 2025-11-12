import os

from telethon import TelegramClient

from env_loader import load_project_env

def _require(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Set {name} before running login.py")
    return value


load_project_env()
api_id = int(_require("TELEGRAM_API_ID"))
api_hash = _require("TELEGRAM_API_HASH")
session_name = os.getenv("TELEGRAM_SESSION_NAME", "study_session")
client = TelegramClient(session_name, api_id, api_hash)


async def main():
    me = await client.get_me()
    print("Logged in as:", me.first_name, "(id:", me.id, ")")


with client:
    client.loop.run_until_complete(main())
