import asyncio, os, time
from telethon import TelegramClient
import qrcode

api_id = 27333292
api_hash = "d8e1fbba6f100090d6876036ccb121df"

client = TelegramClient("study_session", api_id, api_hash)

async def show_qr_and_wait():
    print("Requesting QR token…")
    qr = await client.qr_login()
    img_path = "tg_login_qr.png"
    qrcode.make(qr.url).save(img_path)
    try:
        os.startfile(img_path)  # Windows: open the image automatically
    except Exception:
        pass
    print("On your phone: Telegram → Settings → Devices → Link Desktop Device → scan the QR")
    print("Waiting up to 180 seconds…")
    await qr.wait(180)  # wait up to 3 minutes
    me = await client.get_me()
    print(f"Logged in as: {me.first_name} (id: {me.id})")
    print("Session saved to study_session.session")

async def main():
    await client.connect()
    if await client.is_user_authorized():
        me = await client.get_me()
        print(f"Already logged in as: {me.first_name} (id: {me.id})")
        return

    # Keep regenerating new QR codes until scanned
    while True:
        try:
            await show_qr_and_wait()
            break
        except asyncio.TimeoutError:
            print("QR expired — generating a new one…")
            continue

    await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
