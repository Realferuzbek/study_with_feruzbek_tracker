from telethon import TelegramClient

# Replace with your values
api_id = 27333292
api_hash = 'd8e1fbba6f100090d6876036ccb121df'

# Session name (will create a .session file locally)
client = TelegramClient('study_session', api_id, api_hash)

async def main():
    me = await client.get_me()
    print("Logged in as:", me.first_name, "(id:", me.id, ")")

with client:
    client.loop.run_until_complete(main())
