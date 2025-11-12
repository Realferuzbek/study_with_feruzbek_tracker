import os
from telethon import TelegramClient
from telethon.sessions import StringSession

from env_loader import load_project_env

load_project_env()
API_ID = int(os.getenv("TELEGRAM_API_ID", "0"))
API_HASH = os.getenv("TELEGRAM_API_HASH", "")
SESSION = os.getenv("TELEGRAM_SESSION_NAME", "study_session")

tg_string = os.getenv("TG_STRING_SESSION")
if tg_string:
    client = TelegramClient(StringSession(tg_string), API_ID, API_HASH)
else:
    client = TelegramClient(SESSION, API_ID, API_HASH)

async def main():
    me = await client.get_me()
    print("Logged in as:", me.first_name, me.last_name or "", f"(id: {me.id})")

with client:
    client.loop.run_until_complete(main())

    
