import asyncio, json, time, subprocess, ctypes, os
from pathlib import Path
from telethon import TelegramClient
from telethon.errors import RPCError
from telethon.sessions import StringSession

from env_loader import load_project_env

BASE = Path(__file__).resolve().parent
HEARTBEAT = BASE / "tracker.lock"
STATE_FILE = BASE / "tracker_state.json"     # already present
MANUAL_FLAG = BASE / "manual_stop.flag"      # created by our stop script
TASK_NAME = r"\StudyTracker-Logon"           # match your Task Scheduler name
TARGET_USERNAME = "realferuzbek"             # no '@'

STALE_AFTER = 60      # heartbeat older than 60s => crash
BOOT_SUPPRESS = 180   # don't alert for first 3 minutes after boot
CHECK_EVERY = 30
SEND_RECOVERY = False # tracker already sends its own ✅; set True if you want keeper to send too

load_project_env()
API_ID = int(os.getenv("TELEGRAM_API_ID", "0"))
API_HASH = os.getenv("TELEGRAM_API_HASH", "")
TG_STRING_SESSION = os.getenv("TG_STRING_SESSION")

def uptime_seconds() -> int:
    return ctypes.windll.kernel32.GetTickCount64() // 1000

def is_stale(now: int) -> bool:
    if not HEARTBEAT.exists():
        return True
    try:
        ts = int(HEARTBEAT.read_text(encoding="utf-8").strip())
    except Exception:
        ts = int(HEARTBEAT.stat().st_mtime)
    return (now - ts) > STALE_AFTER

def load_state():
    try: return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except Exception: return {"crash_sent": False, "last_alert_ts": 0}

def save_state(st):
    try: STATE_FILE.write_text(json.dumps(st), encoding="utf-8")
    except Exception: pass

def find_session():
    for p in BASE.glob("*.session"):
        return p.with_suffix("")  # Telethon uses path without .session
    return None

def build_client():
    if TG_STRING_SESSION:
        return TelegramClient(StringSession(TG_STRING_SESSION), API_ID, API_HASH)
    session = find_session()
    if not session:
        raise RuntimeError("No Telethon session found. Provide TG_STRING_SESSION or place a .session file beside keeper.py.")
    return TelegramClient(str(session), 0, "")

async def send_dm(client, text: str):
    try: await client.send_message(TARGET_USERNAME, text)
    except RPCError as e: print("DM failed:", e)

def restart_task():
    subprocess.run(["schtasks", "/Run", "/TN", TASK_NAME], check=False)

async def main():
    try:
        client = build_client()
    except RuntimeError as exc:
        print(exc)
        return
    await client.connect()

    st = load_state()
    while True:
        now = int(time.time())

        # don't alert right after boot
        if uptime_seconds() < BOOT_SUPPRESS:
            st["crash_sent"] = False
            save_state(st)
            await asyncio.sleep(CHECK_EVERY)
            continue

        # user stopped intentionally
        if MANUAL_FLAG.exists():
            if st.get("crash_sent"):
                st["crash_sent"] = False
                save_state(st)
            await asyncio.sleep(CHECK_EVERY)
            continue

        stale = is_stale(now)

        if stale and not st.get("crash_sent", False):
            st["crash_sent"] = True
            st["last_alert_ts"] = now
            save_state(st)
            await send_dm(client, "⚠️ StudyTracker CRASH detected. Auto-restarting…")
            restart_task()

        elif (not stale) and st.get("crash_sent", False) and SEND_RECOVERY:
            delta = now - st.get("last_alert_ts", now)
            await send_dm(client, f"✅ StudyTracker recovered after {delta}s.")
            st["crash_sent"] = False
            save_state(st)

        await asyncio.sleep(CHECK_EVERY)

if __name__ == "__main__":
    asyncio.run(main())
