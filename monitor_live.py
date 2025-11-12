import asyncio, time, re, os
from telethon import TelegramClient, functions, types
from telethon.utils import get_peer_id
from telethon.sessions import StringSession

from env_loader import load_project_env

load_project_env()

API_ID = int(os.getenv("TELEGRAM_API_ID", "0"))
API_HASH = os.getenv("TELEGRAM_API_HASH", "")
SESSION = os.getenv("TELEGRAM_SESSION_NAME", "study_session")
GROUP = os.getenv("PUBLIC_TG_GROUP_LINK") or os.getenv("TELEGRAM_GROUP_USERNAME") or "studywithferuzbek"

def _build_client():
    tg_string = os.getenv("TG_STRING_SESSION")
    if tg_string:
        return TelegramClient(StringSession(tg_string), API_ID, API_HASH)
    return TelegramClient(SESSION, API_ID, API_HASH)


client = _build_client()

async def resolve_group(target: str):
    """Resolve entity from invite link, +hash, or @username"""
    m = re.search(r'(?:t\.me\/\+|t\.me\/joinchat\/|\+|joinchat\/)([A-Za-z0-9_-]+)', target)
    if m:
        inv_hash = m.group(1)
        try:
            info = await client(functions.messages.CheckChatInviteRequest(inv_hash))
            if isinstance(info, types.ChatInviteAlready):
                ent = await client.get_entity(info.chat.id)
                print("Resolved via invite (already joined):", getattr(ent, 'title', getattr(ent, 'username', '')))
                return ent
            else:
                joined = await client(functions.messages.ImportChatInviteRequest(inv_hash))
                chat = joined.chats[0]
                ent = await client.get_entity(chat.id)
                print("Joined via invite:", getattr(ent, 'title', getattr(ent, 'username', '')))
                return ent
        except Exception as e:
            print("Invite resolve/join failed:", e)

    # Otherwise treat as username or ID
    ent = await client.get_entity(target)
    print("Resolved via username/ID:", getattr(ent, 'title', getattr(ent, 'username', '')))
    return ent

async def get_current_group_call(ent):
    try:
        if isinstance(ent, types.Channel):
            full = await client(functions.channels.GetFullChannelRequest(ent))
        else:
            full = await client(functions.messages.GetFullChatRequest(ent.id))
    except Exception as e:
        print("GetFull* error:", e)
        return None

    fc = getattr(full, "full_chat", None)
    call = getattr(fc, "call", None)
    if not call:
        return None
    return types.InputGroupCall(id=call.id, access_hash=call.access_hash)

async def list_participants(input_call):
    gp = await client(functions.phone.GetGroupParticipantsRequest(
        call=input_call,
        ids=[],
        sources=[],
        offset="",
        limit=200
    ))
    users_map = {u.id: u for u in gp.users}
    out = []
    for p in gp.participants:
        peer = getattr(p, "peer", None)
        if isinstance(peer, types.PeerChannel):
            # Skip channel “participant”
            continue

        try:
            uid = get_peer_id(peer)
        except Exception:
            uid = getattr(p, "user_id", None)

        u = users_map.get(uid)
        if not u and uid:
            try:
                u = await client.get_entity(uid)
            except Exception:
                u = None

        if u:
            name = (u.first_name or "") + (" " + u.last_name if getattr(u, "last_name", None) else "")
            handle = "@" + u.username if getattr(u, "username", None) else ""
        else:
            name, handle = str(uid or "?"), ""

        out.append((uid, name.strip(), handle))
    return out

async def main():
    await client.connect()
    if not await client.is_user_authorized():
        print("Not logged in. Run tg_session_qr.py first to generate a string session.")
        return

    ent = await resolve_group(GROUP)

    print("Looking for active live/voice chat…")
    call = None
    for _ in range(30):
        call = await get_current_group_call(ent)
        if call:
            break
        await asyncio.sleep(2)

    if not call:
        print("❗ No active voice/live chat detected.")
        return

    print("✅ Connected. Polling participants… (Ctrl+C to stop)")
    seen, totals = {}, {}

    try:
        while True:
            now = time.time()
            participants = await list_participants(call)
            current = {uid for uid, _, _ in participants if uid}

            # joins
            for uid in current:
                if uid not in seen:
                    seen[uid] = now

            # leaves
            for uid in list(seen.keys()):
                if uid not in current:
                    dur = now - seen.pop(uid)
                    totals[uid] = totals.get(uid, 0) + dur

            names_now = [f"{name}{' ' + handle if handle else ''}" for uid, name, handle in participants if name]
            print(f"In call ({len(names_now)}): {', '.join(names_now) if names_now else '—'}")
            await asyncio.sleep(10)
    except KeyboardInterrupt:
        pass

    now = time.time()
    for uid, start in seen.items():
        totals[uid] = totals.get(uid, 0) + (now - start)

    print("\n=== Session totals (minutes) ===")
    for uid, secs in totals.items():
        mins = int(secs // 60)
        try:
            u = await client.get_entity(uid)
            name = (u.first_name or "") + (" " + (u.last_name or "") if getattr(u, "last_name", None) else "")
        except Exception:
            name = str(uid)
        print(f"{name.strip()}: {mins} min")

    await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
