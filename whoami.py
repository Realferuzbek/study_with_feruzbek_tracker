from telethon import TelegramClient
API_ID = 27333292
API_HASH = "d8e1fbba6f100090d6876036ccb121df"
SESSION = "study_session"

client = TelegramClient(SESSION, API_ID, API_HASH)

async def main():
    me = await client.get_me()
    print("Logged in as:", me.first_name, me.last_name or "", f"(id: {me.id})")

with client:
    client.loop.run_until_complete(main())

    