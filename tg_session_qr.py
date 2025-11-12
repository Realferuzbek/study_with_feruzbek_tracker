"""
Generate a fresh Telethon StringSession via QR login.

Outputs the string to stdout so you can set TG_STRING_SESSION without ever
creating a .session file on disk.
"""

import asyncio
import getpass
import os
from pathlib import Path

import qrcode
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
from telethon.sessions import StringSession

from env_loader import load_project_env

BASE_DIR = Path(__file__).resolve().parent
VAR_DIR = BASE_DIR / "var"
VAR_DIR.mkdir(parents=True, exist_ok=True)
IMG_PATH = VAR_DIR / "tg_session_qr.png"


def _require(name: str) -> str:
    value = os.getenv(name)
    if value:
        return value.strip()
    return input(f"{name}: ").strip()


load_project_env()
API_ID = int(_require("TELEGRAM_API_ID"))
API_HASH = _require("TELEGRAM_API_HASH")
client = TelegramClient(StringSession(), API_ID, API_HASH)


async def show_qr_and_wait() -> None:
    print("Requesting QR token…")
    qr = await client.qr_login()
    qrcode.make(qr.url).save(IMG_PATH)
    try:
        os.startfile(str(IMG_PATH))
    except Exception:
        pass
    print("Telegram → Settings → Devices → Link Desktop Device → scan the QR.")
    print("Waiting up to 180 seconds…")
    try:
        await qr.wait(180)
    except SessionPasswordNeededError:
        pwd = os.getenv("TELEGRAM_2FA_PASSWORD") or getpass.getpass("Enter Telegram 2FA password: ")
        await client.sign_in(password=pwd)
    finally:
        try:
            IMG_PATH.unlink(missing_ok=True)
        except Exception:
            pass


async def main() -> None:
    await client.connect()
    if await client.is_user_authorized():
        me = await client.get_me()
        print(f"Already logged in as: {me.first_name} (id: {me.id})")
        print("Existing StringSession:\n")
        print(client.session.save())
        return

    while True:
        try:
            await show_qr_and_wait()
            me = await client.get_me()
            print(f"Logged in as: {me.first_name} (id: {me.id})")
            print("\nString session (copy/paste into TG_STRING_SESSION):\n")
            print(client.session.save())
            break
        except asyncio.TimeoutError:
            print("QR expired — generating a new one…\n")
            continue
    await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
