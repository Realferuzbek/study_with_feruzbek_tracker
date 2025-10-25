import asyncio, os, json, time, subprocess, ctypes
from pathlib import Path
from telethon import TelegramClient
from telethon.errors import RPCError

BASE = Path(file).resolve().parent
HEARTBEAT = BASE / "tracker.lock"
STATE_FILE = BASE / "tracker_state.json"     # already present
MANUAL_FLAG = BASE / "manual_stop.flag"      # created by our stop script
TASK_NAME = r"\StudyTracker-Logon"           # match your Task Scheduler name
TARGET_USERNAME = "realferuzbek"             # no '@'

STALE_AFTER = 60      # heartbeat older than 60s => crash
BOOT_SUPPRESS = 180   # don't alert for first 3 minutes after boot
CHECK_EVERY = 30
SEND_RECOVERY = False # tracker already sends its own ✅; set True if you want keeper to send too

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

async def send_dm(client, text: str):
    try: await client.send_message(TARGET_USERNAME, text)
    except RPCError as e: print("DM failed:", e)

def restart_task():
    subprocess.run(["schtasks", "/Run", "/TN", TASK_NAME], check=False)

async def main():
    session = find_session()
    if not session:
        print("No Telethon .session found beside keeper.py")
        return
    client = TelegramClient(str(session), 0, "")
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

if name == "main":
    asyncio.run(main())
