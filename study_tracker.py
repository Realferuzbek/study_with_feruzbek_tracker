# F:\study_with_me\study_tracker.py
# Event-driven, robust Telegram Study Tracker.
# - Near real-time join/leave via Raw updates (+ snapshot fetch)
# - Pagination (no 200-cap)
# - Backoff networking
# - Per-videochat 5-minute gate (sub-5m is ignored entirely for all boards)
# - Alias merge (your accounts merged into @realferuzbek)
# - Daily auto post at 22:00 Asia/Tashkent
# - Manual "post now" without breaking daily schedule (post_now.flag)

import asyncio, time, re, sqlite3, os, sys, traceback, random, html, json, copy
from datetime import datetime, timedelta, timezone, date
from pathlib import Path
from typing import List, NamedTuple, Optional

# ---- Timezone (Asia/Tashkent) with fallback ----
try:
    from zoneinfo import ZoneInfo
    TZ = ZoneInfo("Asia/Tashkent")
except Exception:
    TZ = timezone(timedelta(hours=5))  # UTC+5 fallback

from telethon import TelegramClient, functions, types, events
from telethon.errors import RPCError
from telethon.extensions import html as tele_html
from telethon.utils import get_peer_id

from emojis_runtime import NORMAL_SET, PremiumEmojiResolver, has_premium

# ---- Windows single-instance guard (coexists with PS1 mutex) ----
import ctypes, ctypes.wintypes
try:
    _kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    _CreateMutexW = _kernel32.CreateMutexW
    _CreateMutexW.argtypes = [ctypes.c_void_p, ctypes.wintypes.BOOL, ctypes.wintypes.LPCWSTR]
    _CreateMutexW.restype  = ctypes.wintypes.HANDLE
    _ST_MUTEX = _CreateMutexW(None, False, r"Global\\StudyTrackerMutex")
except Exception:
    _ST_MUTEX = None

import logging, logging.handlers, builtins
from logging.handlers import RotatingFileHandler
import threading

from web_export import export_latest_leaderboards

# ---- Logging ----
logger = logging.getLogger("tracker")
logger.setLevel(logging.INFO)

BASE_DIR = Path(__file__).resolve().parent
LOG_FILE = BASE_DIR / "tracker.log"
ROTATED = BASE_DIR / "tracker_2.log"
HEARTBEAT_FILE = BASE_DIR / "tracker.lock"

fh = RotatingFileHandler(LOG_FILE, maxBytes=2_000_000, backupCount=1, encoding="utf-8")
def _namer(default_name: str):
    # Rename tracker.log.1 -> tracker_2.log
    p = Path(default_name)
    return str(ROTATED if p.name.endswith(".1") else p)
fh.namer = _namer
fh.setFormatter(logging.Formatter("%(asctime)s %(message)s"))

logger.handlers = [fh]
logger.propagate = False

_builtin_print = builtins.print
def print(*args, **kwargs):
    s = " ".join(str(a) for a in args)
    try: logger.info(s)
    except Exception: pass
    _builtin_print(*args, **kwargs)

def _log_exc(label: str, e: Exception):
    print(f"{label}: {type(e).__name__}: {e}")
    try:
        tb = "".join(traceback.format_exception(type(e), e, e.__traceback__))
        for line in tb.rstrip().splitlines():
            print(line)
    except Exception:
        pass

# =================== CONFIG ===================
API_ID   = 27333292
API_HASH = "d8e1fbba6f100090d6876036ccb121df"
SESSION  = "study_session"                    # single session file

# >>> SET THIS EXACTLY TO YOUR GROUP USERNAME (no https://t.me/ or @)
GROUP = "studywithferuzbek"

# Fallback snapshot poll (safety net). 30s is fine.
SNAPSHOT_POLL_EVERY  = 30

# Persist "currently in call" check every X seconds (we buffer; DB writes use gating logic)
FLUSH_EVERY  = 600  # 10 minutes

# Daily post time (Asia/Tashkent)
POST_HOUR    = 21
POST_MINUTE  = 0

DB_PATH      = "study.db"

# Display / compliments
SHOW_MAX_PER_LIST = 10
USE_COMPLIMENTS   = True

# Count yourself while testing?
TRACK_SELF = True

# Networking backoff
BACKOFF_RETRIES = 6
BACKOFF_BASE    = 1.0  # seconds

# Manual control flag for "post now"
CONTROL_POST_NOW_FILE = "post_now.flag"

# ---- Gating ----
# Minimum time within ONE videochat session to count at all (5 minutes)
SESSION_MIN_SECONDS = 300
# Daily minimum for inclusion is now disabled (session gate handles fairness)
MIN_DAILY_SECONDS = 0

# ---- Alias groups (merge these usernames as one person, shown as the canonical) ----
# canonical_username: [canonical_username, alias1, alias2, ...]
ALIAS_GROUPS_USERNAMES = {
    "realferuzbek": ["realferuzbek", "contact_admin_me", "studywithferuzbek"]
}

# Quiet roster logging unless call is active/changed
ROSTER_LOG_EVERY = 300  # seconds; only print roster at most every 5 minutes

# ---- Watchdog / alerts ----
WATCHDOG_NOTIFY_TO = "realferuzbek"      # DM target (your main account)
HEARTBEAT_THRESHOLDS = [300, 600, 900]   # alert at 5, 10, 15 minutes since last OK

client = TelegramClient(SESSION, API_ID, API_HASH)
MY_ID: int | None = None

# ---------- NEW: Heartbeat / state (1/6) ----------
HEARTBEAT_IDLE_EVERY = int(os.getenv("HEARTBEAT_IDLE_EVERY", "600"))   # 10 min when no livestream
HEARTBEAT_OFFLINE_EVERY = int(os.getenv("HEARTBEAT_OFFLINE_EVERY", "60"))  # 1 min while offline
STATE_FILE = "tracker_state.json"

HEARTBEAT_SEC = 10   # set to 30 if you prefer

_hb_stop = threading.Event()
def _heartbeat():
    import json, time
    while not _hb_stop.is_set():
        try:
            HEARTBEAT_FILE.write_text(str(int(time.time())), encoding="utf-8")
            # log a compact pulse
            logger.info("[heartbeat] alive")
            # also refresh tracker_state.json last_seen for the watchdog
            try:
                st = {"last_seen": int(time.time())}
                Path("tracker_state.json").write_text(json.dumps(st), encoding="utf-8")
            except Exception:
                pass
        except Exception as e:
            try: logger.warning(f"heartbeat error: {e}")
            except Exception: pass
        _hb_stop.wait(HEARTBEAT_SEC)

_last_idle_beat = 0.0
_last_offline_beat = 0.0

def _now_ts():
    return time.time()

def _log_beat(msg: str):
    ts = datetime.now().strftime("%H:%M:%S")
    try:
        logger.info("[%s] %s", ts, msg)
    except Exception:
        print(f"[{ts}] {msg}")

def _load_state():
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def _save_state(d):
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(d, f)
    except Exception:
        pass

# ---------- Guard: session file free ----------
def assert_session_free():
    sess_path = f"{SESSION}.session"
    if not os.path.exists(sess_path):
        return
    try:
        con = sqlite3.connect(sess_path, timeout=1)
        con.execute("PRAGMA user_version")
        con.close()
    except sqlite3.OperationalError:
        print("\n[ABORT] Another copy is using the session. Close it first.\n")
        try:
            _hb_stop.set()
            if "_hb_thr" in globals():
                _hb_thr.join(timeout=2)
        except Exception:
            pass
        sys.exit(1)

# ---------- DB helpers ----------
def _con():
    con = sqlite3.connect(DB_PATH, timeout=30)
    try: con.execute("PRAGMA journal_mode=WAL")
    except Exception: pass
    return con

def db_init():
    con = _con(); cur = con.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS seconds_totals (
        d TEXT NOT NULL,
        user_id INTEGER NOT NULL,
        seconds INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (d, user_id)
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT)""")
    cur.execute("""CREATE TABLE IF NOT EXISTS user_cache (
        user_id INTEGER PRIMARY KEY,
        display_name TEXT,
        username TEXT
    )""")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS compliments_period (
            period   TEXT NOT NULL,
            user_id  INTEGER NOT NULL,
            compliment TEXT NOT NULL,
            PRIMARY KEY (period, user_id)
        )
    """)
    con.commit(); con.close()

def _add_seconds_for_date(date_str: str, user_id: int, delta: int):
    if delta <= 0: return
    con = _con(); cur = con.cursor()
    cur.execute("INSERT OR IGNORE INTO seconds_totals(d,user_id,seconds) VALUES(?,?,0)", (date_str, user_id))
    cur.execute("UPDATE seconds_totals SET seconds = seconds + ? WHERE d = ? AND user_id = ?",
                (int(delta), date_str, user_id))
    con.commit(); con.close()

def db_add_span(user_id: int, start_ts: float, end_ts: float):
    """Adds a continuous span, splitting by day when needed."""
    if user_id is None or end_ts <= start_ts: return
    cur_ts = start_ts
    while cur_ts < end_ts:
        dt = datetime.fromtimestamp(cur_ts, TZ)
        next_day_ts = (datetime(dt.year, dt.month, dt.day, tzinfo=TZ) + timedelta(days=1)).timestamp()
        chunk_end = min(end_ts, next_day_ts)
        _add_seconds_for_date(dt.date().isoformat(), user_id, int(chunk_end - cur_ts))
        cur_ts = chunk_end

def db_set_meta(k: str, v: str):
    con = _con(); cur = con.cursor()
    cur.execute("INSERT OR REPLACE INTO meta(k,v) VALUES(?,?)", (k, v))
    con.commit(); con.close()

def db_get_meta(k: str) -> str | None:
    con = _con(); cur = con.cursor()
    cur.execute("SELECT v FROM meta WHERE k = ?", (k,))
    row = cur.fetchone(); con.close()
    return row[0] if row else None

def db_cache_user(user_id: int, display_name: str, username: str | None):
    con = _con(); cur = con.cursor()
    cur.execute("INSERT OR REPLACE INTO user_cache(user_id, display_name, username) VALUES(?,?,?)",
                (user_id, display_name, username or None))
    con.commit(); con.close()

def db_get_day_seconds(user_id: int, d_str: str) -> int:
    con = _con(); cur = con.cursor()
    cur.execute("SELECT seconds FROM seconds_totals WHERE d = ? AND user_id = ?", (d_str, user_id))
    row = cur.fetchone(); con.close()
    return int(row[0]) if row else 0

def db_fetch_period_seconds(start_date: datetime, end_date: datetime, min_daily: int = 0):
    """
    Sum seconds for users between dates. If min_daily>0, only include a day's
    seconds for a user if that day's seconds >= min_daily (per-day gating).
    """
    sd = start_date.date().isoformat(); ed = end_date.date().isoformat()
    con = _con(); cur = con.cursor()
    if min_daily > 0:
        cur.execute(f"""
            SELECT user_id,
                   SUM(CASE WHEN seconds >= ? THEN seconds ELSE 0 END) AS s
            FROM seconds_totals
            WHERE d BETWEEN ? AND ?
            GROUP BY user_id
            HAVING s > 0
            ORDER BY s DESC
        """, (int(min_daily), sd, ed))
    else:
        cur.execute("""
            SELECT user_id, SUM(seconds) AS s
            FROM seconds_totals
            WHERE d BETWEEN ? AND ?
            GROUP BY user_id
            HAVING s > 0
            ORDER BY s DESC
        """, (sd, ed))
    rows = cur.fetchall(); con.close()
    return [(int(uid), int(sec)) for (uid, sec) in rows]

# ---------- Quotes (Word of the Day) ----------
def _load_quotes(path="quotes.txt"):
    lines = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                s = line.strip()
                if s:
                    lines.append(s)
    except FileNotFoundError:
        pass
    return lines

def _quote_for_today(now: datetime):
    quotes = _load_quotes()
    if not quotes:
        return None
    anchor_str = db_get_meta("anchor_date")
    try:
        anchor = datetime.fromisoformat(anchor_str).replace(tzinfo=TZ) if anchor_str else now.replace(hour=0, minute=0, second=0, microsecond=0)
    except Exception:
        anchor = now.replace(hour=0, minute=0, second=0, microsecond=0)
    idx = ((now.date() - anchor.date()).days) % len(quotes)
    return quotes[idx]

# ---------- NEW: Safe Telegram wrapper (2/6 + 3/6) ----------
class NetworkDown(Exception):
    pass

async def ensure_connected() -> bool:
    """
    Returns True if connected (and authorized), False if offline.
    Never raises; callers can keep looping calmly.
    """
    try:
        if not client.is_connected():
            await client.connect()
        try:
            ok = await client.is_user_authorized()
            if not ok:
                await client.start()
        except Exception:
            pass
        return client.is_connected()
    except Exception as e:
        logger.warning("[connect] offline: %s", e)
        return False

async def _tg(req):
    """Call Telegram safely. Raises NetworkDown if we are offline or transport fails."""
    if not await ensure_connected():
        raise NetworkDown("offline")
    try:
        return await client(req)
    except Exception as e:
        logger.debug("[tg] transient error: %r", e)
        raise NetworkDown(str(e))

# ---------- Telegram helpers ----------
async def resolve_group(target: str):
    await ensure_connected()

    # handle invite links like t.me/+HASH or joinchat/HASH
    m = re.search(r'(?:t\.me\/\+|t\.me\/joinchat\/|\+|joinchat\/)([A-Za-z0-9_-]+)', target)
    if m:
        inv_hash = m.group(1)
        try:
            info = await _tg(functions.messages.CheckChatInviteRequest(hash=inv_hash))
            if isinstance(info, types.ChatInvite):
                joined = await _tg(functions.messages.ImportChatInviteRequest(hash=inv_hash))
                chat = joined.chats[0]
                ent = await client.get_entity(chat.id)
                print("Joined via invite:", getattr(ent, 'title', getattr(ent, 'username', ''))); return ent
            if isinstance(info, types.ChatInviteAlready):
                ent = await client.get_entity(info.chat.id)
                print("Resolved via invite (already joined):", getattr(ent, 'title', getattr(ent, 'username', ''))); return ent
        except Exception as e:
            _log_exc("Invite resolve/join failed", e)

    # username or full link supported
    uname = str(target).replace("https://t.me/", "").replace("@", "")
    ent = await client.get_entity(uname)
    print("Resolved via username/ID:", getattr(ent, 'title', getattr(ent, 'username', ''))); return ent

# ---- NEW: Current group call (4/6) ----
async def get_current_group_call(ent):
    """
    Returns an InputGroupCall or None. Never explodes on disconnect.
    """
    # If we're offline, just say "no call" quietly
    if not await ensure_connected():
        return None
    try:
        input_entity = await client.get_input_entity(ent)

        if isinstance(ent, types.Channel) or isinstance(input_entity, types.InputChannel):
            try:
                full = await _tg(functions.channels.GetFullChannelRequest(channel=input_entity))
            except NetworkDown:
                return None
            except Exception as e:
                _log_exc("GetFullChannel primary failed", e)
                try:
                    fresh = await client.get_input_entity(getattr(ent, "username", ent))
                    full = await _tg(functions.channels.GetFullChannelRequest(channel=fresh))
                except Exception as e2:
                    _log_exc("GetFullChannel retry failed", e2)
                    return None

        elif isinstance(ent, types.Chat) or isinstance(input_entity, types.InputPeerChat):
            try:
                full = await _tg(functions.messages.GetFullChatRequest(chat_id=ent.id if isinstance(ent, types.Chat) else input_entity.chat_id))
            except NetworkDown:
                return None
            except Exception as e:
                _log_exc("GetFullChat failed", e)
                return None
        else:
            e2 = await client.get_entity(ent)
            return await get_current_group_call(e2)

        fc = getattr(full, "full_chat", None)
        call = getattr(fc, "call", None)
        if not call:
            return None
        return types.InputGroupCall(id=call.id, access_hash=call.access_hash)

    except NetworkDown:
        return None
    except Exception as e:
        _log_exc("GetFull* error (outer)", e)
        return None

# ---------- PAGINATED participants ----------
async def fetch_participants(input_call):
    await ensure_connected()
    out = []
    next_offset = ""
    users_map = {}
    while True:
        req = functions.phone.GetGroupParticipantsRequest(
            call=input_call, ids=[], sources=[], offset=next_offset, limit=200
        )
        gp = await _tg(req)
        users_map.update({u.id: u for u in gp.users})
        for p in gp.participants:
            peer = getattr(p, "peer", None)
            if not peer:
                continue
            # NOTE: we now SUPPORT channels (group "joining as a channel")
            try:
                uid = get_peer_id(peer)  # works for users and channels; channels become negative ids
            except Exception:
                uid = getattr(p, "user_id", None)
            if not uid:
                continue
            u = users_map.get(uid)
            if not u:
                try:
                    u = await client.get_entity(uid)
                except Exception:
                    u = None
            if isinstance(u, types.User):
                name = (u.first_name or "") + (" " + u.last_name if getattr(u, "last_name", None) else "")
                handle = getattr(u, "username", None)
            elif isinstance(u, (types.Channel, types.Chat)):
                name = getattr(u, "title", "") or getattr(u, "username", "") or str(uid)
                handle = getattr(u, "username", None)
            else:
                name, handle = str(uid), None
            try: db_cache_user(int(uid), (name or "").strip(), (handle or None))
            except Exception: pass
            out.append((int(uid), (name or "").strip(), handle))
        next_offset = getattr(gp, "next_offset", "")
        if not next_offset:
            break
    return out

# ---------- Compliments / formatting ----------
def _load_compliments_file(path="compliments.txt"):
    pool = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                s = line.strip()
                if not s or s.startswith("["): continue
                pool.append(s)
    except FileNotFoundError:
        pass
    return pool

_COMPL_EXTRAS = [
    "Iron Discipline 🦾","Early Bird Energy 🌞","Distraction Slayer 🛡️","Deep Work Dynamo ⚡",
    "Laser Precision 🛰️","Leveling Up 📈","Night Owl Power 🌙","Flow Controller 🎛️",
    "Habit Climber 🧗","Full Throttle 🏎️","King of Study 👑","Lord of Focus 🗡️",
    "Alpha Concentration 🦁","Craftsman of Consistency 🛠️","Power Grinder 🔧",
    "Queen of Study 👑","Angel of Focus 🪽","Graceful Grinder 🌸","Rhythm of Discipline 💃",
    "Weaver of Consistency 🧵","Moonlight Scholar 🌙","Study Engine 📚","Target Locked 🎯",
    "Focus Machine 🧠","Productivity Ninja 🥷","Finish Line Closer 🏁","Streak Keeper 📆",
    "Premium Grinder 💎","Consistency Beast 💪","King of Focus 👑","Study Titan 🗿",
    "Mind Sprint 🏃‍♂️","Momentum Master 🧲","Calm Laser 🎯","Unbreakable Chain ⛓️",
    "No-Excuse Executor ✅","Deadline Tamer ⏳","Clarity Crafter ✨","Courage of Action 🦅",
    "Relentless Rhythm 🥁","Shadow of Distraction ☄️","Focus Lighthouse 🗼",
    "Steady Flame 🔥","Bold Consistency 🧱","Quiet Thunder ⚡","Grit Architect 🧱",
    "Habit Sculptor 🪚","Minute Millionaire 🕰️","Study Momentum 🚀","Page Turner 📖",
    "First In, Last Out 🚪","Mind Gardener 🌿","Storm-Proof Focus ⛈️","Zen Executioner 🧘",
    "Precision Pilot 🧭","Depth Diver 🐬","Quiet Conqueror 🤫","Willpower Weaver 🪡",
    "Discipline Dancer 🎼","Task Wrangler 🤠","Flow Surfer 🏄","Stamina Engine ⚙️",
    "Craft of Patience 🪵","Focus Alchemist ⚗️","Study Sentinel 🛡️","Hustle Maestro 🎼",
    "Crown of Calm 👑","Laser Archer 🏹","Morning Star ⭐","Evening Torch 🔥",
    "Habit Ranger 🧭","Focus Smith 🔨","Boundless Breath 🌬️","Time Whisperer 🕊️",
    "Pathfinder of Progress 🧭","Pulse of Persistence 💓","Mind Fortress 🏰",
    "Climb of Mastery 🏔️","Grace Under Fire 🔥","Diamond Focus 💎","Echo of Effort 📢",
    "Atlas of Tasks 🗺️","Evergreen Habits 🌲","Sailor of Flow ⛵","Study Sculptor 🗿",
    "Momentum Rider 🐎","Beacon of Routine 🗼","Quiet Blaze 🔥","Peak Consistency 🏔️",
    "Sentry of Focus 🚧","Anchor of Habit ⚓","Praxis Champion 🏆","Tempo Tactician 🥁",
    "Stillness Power 🌊","Minute Samurai 🗡️","Craft of Depth 🪚","Effort Composer 🎻",
    "Order Oracle 🧿","Time Artisan 🎨","Ritual Runner 🏃","Focus Navigator 🧭",
    "Study Voyager 🚀","Calm Commander 🫡","Precision Crafter 🪛","Discipline Smith 🔨",
    "Courageous Focus 🛡️","Patience Pilot ✈️","Focus Monk 🛕","Study Bard 🎶",
    "Resolute Rhythm 🥁","Will of Granite 🪨","Horizon Hunter 🌅","Craft of Momentum 🧰",
    "Serene Storm ⛈️","Task Sculptor 🧱","Endurance Engine 🔩","Mind Cartographer 🗺️",
]
def _load_compliments():
    seen = set(); merged = []
    for s in _load_compliments_file() + _COMPL_EXTRAS:
        s = s.strip()
        if not s or s in seen: continue
        merged.append(s); seen.add(s)
    if not merged:
        merged = ["Consistency Beast 💪", "Focus Machine 🧠", "Iron Discipline 🦾"]
    return merged
_COMPL_POOL = _load_compliments()

def fmt_name(uid: int) -> str:
    """Preferred display: @username. If no username, use display name."""
    con = _con(); cur = con.cursor()
    cur.execute("SELECT display_name, username FROM user_cache WHERE user_id = ?", (uid,))
    row = cur.fetchone(); con.close()
    if not row:
        return str(uid)
    display_name, username = row
    username = (username or "").strip()
    if username:
        return f"@{username}"
    display_name = (display_name or str(uid)).strip()
    return display_name

# ---------- Compliment persistence ----------
def _period_key_day(d: datetime)   -> str: return f"day:{d.date().isoformat()}"
def _period_key_week(start: datetime)-> str: return f"week:{start.date().isoformat()}"
def _period_key_month(start: datetime)-> str: return f"month:{start.date().isoformat()}"

def _get_saved_compliment(pk: str, user_id: int) -> str | None:
    con = _con(); cur = con.cursor()
    cur.execute("SELECT compliment FROM compliments_period WHERE period = ? AND user_id = ?", (pk, user_id))
    row = cur.fetchone(); con.close()
    return row[0] if row else None

def _save_compliment(pk: str, user_id: int, txt: str):
    con = _con(); cur = con.cursor()
    cur.execute("INSERT OR REPLACE INTO compliments_period(period, user_id, compliment) VALUES(?,?,?)",
                (pk, user_id, txt))
    con.commit(); con.close()

def _all_used_for_scope(prefix: str, user_id: int) -> set[str]:
    con = _con(); cur = con.cursor()
    cur.execute("SELECT compliment FROM compliments_period WHERE period LIKE ? AND user_id = ?",
                (f"{prefix}%", user_id))
    rows = [r[0] for r in cur.fetchall()]
    con.close()
    return set(rows)

def _choose_from_pool(exclude: set[str]) -> str:
    pool = [c for c in _COMPL_POOL if c not in exclude]
    if not pool: pool = list(_COMPL_POOL)
    random.shuffle(pool)
    return pool[0]

def choose_weekly(user_id: int, week_start: datetime) -> str:
    pk = _period_key_week(week_start)
    prev = _get_saved_compliment(pk, user_id)
    if prev: return prev
    used_before = _all_used_for_scope("week:", user_id)
    c = _choose_from_pool(used_before); _save_compliment(pk, user_id, c); return c

def choose_monthly(user_id: int, month_start: datetime) -> str:
    pk = _period_key_month(month_start)
    prev = _get_saved_compliment(pk, user_id)
    if prev: return prev
    used_before = _all_used_for_scope("month:", user_id)
    c = _choose_from_pool(used_before); _save_compliment(pk, user_id, c); return c

def choose_daily(user_id: int, day_dt: datetime, avoid: set[str]) -> str:
    pk = _period_key_day(day_dt)
    prev = _get_saved_compliment(pk, user_id)
    if prev: return prev
    c = _choose_from_pool(avoid); _save_compliment(pk, user_id, c); return c

# ---------- Ranking/formatting ----------
def _rank_medal(i: int) -> str:
    return "🥇" if i == 1 else ("🥈" if i == 2 else ("🥉" if i == 3 else ""))

_KEYCAPS = {"1":"1️⃣","2":"2️⃣","3":"3️⃣","4":"4️⃣","5":"5️⃣","6":"6️⃣","7":"7️⃣","8":"8️⃣","9":"9️⃣","10":"🔟"}
def _rank_keycap(n: int) -> str: return _KEYCAPS.get(str(n), str(n)) if 1 <= n <= 10 else str(n)

def _badge_for_minutes(mins: int) -> str:
    if mins >= 180: return "🚀"
    if mins >= 120: return "🔥"
    if mins >=  60: return "💪"
    if mins >=   1: return "✅"
    return "😴"

def _mins(secs: int) -> int: return max(0, secs // 60)

def _unique_sorted(rows):
    best = {}
    for uid, secs in rows:
        if secs > 0:
            best[uid] = max(secs, best.get(uid, 0))
    return sorted(best.items(), key=lambda x: x[1], reverse=True)

def _b(s: str) -> str: return f"<b>{html.escape(s)}</b>"

# ---- Force compliment emojis to the END ----
_EMOJI_LEAD_RE = re.compile(r'^\s*([\u2600-\u27BF\uFE0F\U0001F300-\U0001FAFF]+)\s*(.+)$')
def _emoji_to_end(s: str) -> str:
    s = (s or "").strip()
    m = _EMOJI_LEAD_RE.match(s)
    if m:
        lead, rest = m.groups()
        return f"{rest}{lead}"
    return s

_DAY_FLARES = ["💥","❤️‍🔥","👑","🔥","⚡","🌟","🏁","🎯","💫","🧠","🦁","🪽","🧵","🛡️","🌙","🚀","✨","💎"]
def _title_with_day(anchor: datetime, today: datetime) -> str:
    day_idx = _day_index(anchor, today)
    flare = _DAY_FLARES[(day_idx-1) % len(_DAY_FLARES)]
    return f"📊 {_b(f'LEADERBOARD — DAY {day_idx}')} {flare}"

def _header_block(label: str, header_right: str) -> str:
    return f"<blockquote>{_b(f'{label} — {header_right}')}</blockquote>"

def _section_entries(
    rows,
    compliments_by_user: dict[int, str],
    name_overrides: dict[int, str],
) -> list[dict[str, object]]:
    entries: list[dict[str, object]] = []
    for idx, (uid, secs) in enumerate(rows[:SHOW_MAX_PER_LIST], 1):
        mins = _mins(secs)
        preferred = name_overrides.get(uid)
        display_name = preferred if preferred else fmt_name(uid)
        rank_emoji = _rank_medal(idx) or _rank_keycap(idx)
        badge = _badge_for_minutes(mins)
        compliment = ""
        if USE_COMPLIMENTS:
            comp = compliments_by_user.get(uid)
            if comp:
                compliment = _emoji_to_end(comp)
        entries.append(
            {
                "rank": idx,
                "user_id": uid,
                "seconds": int(secs),
                "minutes": mins,
                "display": display_name,
                "rank_emoji": rank_emoji,
                "badge": badge,
                "compliment": compliment,
            }
        )
    return entries


def _render_section(label: str, header_right: str, entries: list[dict[str, object]]) -> str:
    if not entries:
        return f"{_header_block(label, header_right)}\n{_b('nobody did lessons 😴')}"
    lines = [_header_block(label, header_right)]
    for entry in entries:
        name_html = _b(str(entry["display"]))
        tail = ""
        compliment = str(entry.get("compliment", "") or "")
        if compliment:
            tail = f" − {_b(compliment)}"
        lines.append(
            f"{entry['rank_emoji']} {name_html} — {entry['minutes']}m {entry['badge']}{tail}"
        )
    return "\n".join(lines)

# ---------- Premium emoji helpers ----------
class _TokenMatch(NamedTuple):
    key: str
    original_start: int
    original_len: int
    token_start: int
    token_len: int


_NORMAL_EMOJI_SORTED = sorted(NORMAL_SET.items(), key=lambda kv: len(kv[1]), reverse=True)
_PREMIUM_LOGGED = False


def _tokenize_plain_text(text: str) -> tuple[str, List[_TokenMatch]]:
    tokens: List[_TokenMatch] = []
    parts: List[str] = []
    i = 0
    new_len = 0
    while i < len(text):
        matched: Optional[tuple[str, str]] = None
        for key, emoji in _NORMAL_EMOJI_SORTED:
            if text.startswith(emoji, i):
                matched = (key, emoji)
                break
        if matched:
            key, emoji = matched
            token = f"{{{key}}}"
            tokens.append(_TokenMatch(key, i, len(emoji), new_len, len(token)))
            parts.append(token)
            new_len += len(token)
            i += len(emoji)
        else:
            ch = text[i]
            parts.append(ch)
            new_len += 1
            i += 1
    return "".join(parts), tokens


def _offset_stage0_to_stage1(tokens: List[_TokenMatch], offset: int) -> Optional[int]:
    delta = 0
    for tok in tokens:
        tok_end = tok.original_start + tok.original_len
        if offset >= tok_end:
            delta += tok.token_len - tok.original_len
            continue
        if tok.original_start < offset < tok_end:
            return None
        break
    return offset + delta


def _offset_stage1_to_stage2(tokens: List[_TokenMatch], offset: int) -> Optional[int]:
    delta = 0
    for tok in tokens:
        tok_end = tok.token_start + tok.token_len
        if offset >= tok_end:
            delta += 1 - tok.token_len
            continue
        if tok.token_start < offset < tok_end:
            return None
        break
    return offset + delta


def _retarget_markup_entities(
    entities: List[types.TypeMessageEntity],
    tokens: List[_TokenMatch]
) -> Optional[List[types.TypeMessageEntity]]:
    adjusted: List[types.TypeMessageEntity] = []
    for ent in entities:
        start = ent.offset
        end = ent.offset + ent.length
        stage1_start = _offset_stage0_to_stage1(tokens, start)
        stage1_end = _offset_stage0_to_stage1(tokens, end)
        if stage1_start is None or stage1_end is None:
            return None
        final_start = _offset_stage1_to_stage2(tokens, stage1_start)
        final_end = _offset_stage1_to_stage2(tokens, stage1_end)
        if (
            final_start is None
            or final_end is None
            or final_end < final_start
        ):
            return None
        ent_copy = copy.deepcopy(ent)
        ent_copy.offset = final_start
        ent_copy.length = final_end - final_start
        adjusted.append(ent_copy)
    return adjusted


async def _try_send_premium(client, target, html_text: str) -> Optional[types.Message]:
    global _PREMIUM_LOGGED

    plain_text, markup_entities = tele_html.parse(html_text)
    tokenized_text, tokens = _tokenize_plain_text(plain_text)
    if not tokens:
        return None

    pinned_msg = await PremiumEmojiResolver.refresh_if_changed(client)
    if not PremiumEmojiResolver.is_ready():
        await PremiumEmojiResolver.hydrate(client, pinned=pinned_msg)
    if not PremiumEmojiResolver.is_ready():
        return None

    rendered_text, emoji_entities = PremiumEmojiResolver.render(tokenized_text)
    markup_shifted = _retarget_markup_entities(markup_entities, tokens)
    if markup_shifted is None:
        raise ValueError("Failed to remap markup entities for premium emojis.")

    formatting_entities = markup_shifted + emoji_entities
    formatting_entities.sort(key=lambda ent: ent.offset)

    message = await client.send_message(
        target,
        rendered_text,
        formatting_entities=formatting_entities,
    )

    if not _PREMIUM_LOGGED:
        logger.info("premium_emojis=on")
        _PREMIUM_LOGGED = True
    return message

# ---------- GROUP CHANGE AUTO-RESET ----------
def _reset_all_state_for_new_group(new_group_key: str):
    today = datetime.now(TZ).replace(hour=0, minute=0, second=0, microsecond=0)
    con = _con(); cur = con.cursor()
    cur.execute("DELETE FROM seconds_totals")
    cur.execute("DELETE FROM compliments_period")
    cur.execute("DELETE FROM meta WHERE k IN ('last_post_date','anchor_date','quote_index','quote_seed','group_key','group_since')")
    con.commit(); con.close()
    db_set_meta("anchor_date", today.date().isoformat())
    db_set_meta("group_key", new_group_key)
    db_set_meta("group_since", today.date().isoformat())
    db_set_meta("quote_index", "0")
    print(f"[reset] Detected new group. Counters reset. Anchor set to {today.date().isoformat()}.")

def _maybe_reset_on_group_change(ent):
    try: new_key = str(get_peer_id(ent))
    except Exception:
        new_key = f"{getattr(ent,'id',None)}:{getattr(ent,'username',None)}:{getattr(ent,'title',None)}"
    old_key = db_get_meta("group_key")
    if old_key != new_key:
        _reset_all_state_for_new_group(new_key)

# ---------- Alias helpers ----------
def _alias_maps_from_cache():
    """
    Returns:
      alias_to_canon_id: dict[alias_id] -> canonical_id
      canon_id_to_label: dict[canonical_id] -> '@canonical_username'
    """
    con = _con(); cur = con.cursor()
    cur.execute("SELECT user_id, username FROM user_cache")
    rows = cur.fetchall(); con.close()

    uname_to_id = {}
    for uid, uname in rows:
        if uname:
            uname_to_id[uname.strip().lower()] = int(uid)

    alias_to_canon: dict[int, int] = {}
    canon_label: dict[int, str] = {}

    for canon_uname, group in ALIAS_GROUPS_USERNAMES.items():
        ids = [uname_to_id[u.lower()] for u in group if u and u.lower() in uname_to_id]
        if not ids:
            continue
        # Prefer the true canonical username's ID if we have it, else first present
        canon_id = uname_to_id.get(canon_uname.lower(), ids[0])
        for aid in ids:
            alias_to_canon[aid] = canon_id
        canon_label[canon_id] = f"@{canon_uname}"
    return alias_to_canon, canon_label

def _fold_alias_rows(rows, alias_to_canon):
    merged = {}
    for uid, secs in rows:
        cid = alias_to_canon.get(uid, uid)
        merged[cid] = merged.get(cid, 0) + secs
    return _unique_sorted(list(merged.items()))

# ---------- Leaderboard post ----------
async def post_leaderboard(
    ent,
    live_seen_snapshot: dict[int, float] | None = None,
    session_accum_secs: dict[int, int] | None = None,
    session_qualified: dict[int, bool] | None = None,
    mark_daily: bool = True,             # only auto/backfill should mark the day
    override_now: datetime | None = None # ← NEW: post for a specific calendar day
):
    await ensure_connected()
    now = override_now or datetime.now(TZ)
    now_ts = time.time()
    anchor = _ensure_anchor()

    # Build alias maps (merge your accounts)
    alias_to_canon, canon_label = _alias_maps_from_cache()

    week_idx, w_start, w_end = _week_block(anchor, now)
    month_idx, m_start, m_end = _month30_block(anchor, now)

    t_start = datetime(now.year, now.month, now.day, 0, 0, tzinfo=TZ)
    t_end   = datetime(now.year, now.month, now.day, 23, 59, 59, tzinfo=TZ)
    today_str = now.date().isoformat()

    # SQL totals (no daily gate; zeros filtered)
    day_rows   = _unique_sorted(db_fetch_period_seconds(t_start, t_end,   min_daily=MIN_DAILY_SECONDS))
    week_rows  = _unique_sorted(db_fetch_period_seconds(w_start,  w_end,  min_daily=MIN_DAILY_SECONDS))
    month_rows = _unique_sorted(db_fetch_period_seconds(m_start, m_end,   min_daily=MIN_DAILY_SECONDS))

    # Merge aliases on DB rows
    day_rows   = _fold_alias_rows(day_rows, alias_to_canon)
    week_rows  = _fold_alias_rows(week_rows, alias_to_canon)
    month_rows = _fold_alias_rows(month_rows, alias_to_canon)

    # Adjust with live extras — only if current session meets 5m threshold (only for "now")
    if live_seen_snapshot and override_now is None:
        sess_acc = session_accum_secs or {}
        sess_ok  = session_qualified or {}
        day_map   = {uid: secs for uid, secs in day_rows}
        week_map  = {uid: secs for uid, secs in week_rows}
        month_map = {uid: secs for uid, secs in month_rows}

        for raw_uid, join_ts in list(live_seen_snapshot.items()):
            uid = alias_to_canon.get(raw_uid, raw_uid)
            active_delta = int(max(0, now_ts - join_ts))
            total_session_so_far = int(sess_acc.get(uid, 0)) + int(sess_acc.get(raw_uid, 0))
            qualified_now = bool(sess_ok.get(uid, False) or (total_session_so_far + active_delta) >= SESSION_MIN_SECONDS)
            if not qualified_now:
                continue

            stored_today = db_get_day_seconds(uid, today_str)
            extra_for_today = active_delta
            if extra_for_today <= 0:
                continue

            day_map[uid]   = max(day_map.get(uid, 0), stored_today) + extra_for_today
            week_map[uid]  = week_map.get(uid, 0)  + extra_for_today
            month_map[uid] = month_map.get(uid, 0) + extra_for_today

        day_rows   = _unique_sorted(list(day_map.items()))
        week_rows  = _unique_sorted(list(week_map.items()))
        month_rows = _unique_sorted(list(month_map.items()))

    # Compliments (by canonical id)
    week_comps, month_comps, day_comps = {}, {}, {}
    if USE_COMPLIMENTS:
        for uid, _ in week_rows[:SHOW_MAX_PER_LIST]:
            week_comps[uid] = choose_weekly(uid, w_start)
        for uid, _ in month_rows[:SHOW_MAX_PER_LIST]:
            month_comps[uid] = choose_monthly(uid, m_start)
        for uid, _ in day_rows[:SHOW_MAX_PER_LIST]:
            avoid = {week_comps.get(uid, ""), month_comps.get(uid, "")}
            avoid.discard("")
            day_comps[uid] = choose_daily(uid, now, avoid)

    title = _title_with_day(anchor, now)
    today_hdr = f"{_format_d(now)} ({_dow(now)})"
    week_hdr  = f"{_format_d(w_start)} - {_format_d(w_end)} (WEEK {week_idx})"
    month_hdr = f"{_format_d(m_start)} - {_format_d(m_end)} (MONTH {month_idx})"

    day_entries = _section_entries(day_rows, day_comps, canon_label)
    week_entries = _section_entries(week_rows, week_comps, canon_label)
    month_entries = _section_entries(month_rows, month_comps, canon_label)

    today_txt = _render_section("📅 Today", today_hdr, day_entries)
    week_txt = _render_section("📆 This Week", week_hdr, week_entries)
    month_txt = _render_section("🗓️ This Month", month_hdr, month_entries)

    # WORD OF THE DAY — only the quote inside the spoiler
    q = _quote_for_today(now)
    motd = ""
    if q:
        quote_html = f'<span class="tg-spoiler"><b><i>{html.escape(q)}</i></b></span>'
        motd = "\n\n" + _b("WORD OF THE DAY 🌟") + "\n" + f"<blockquote>{quote_html}</blockquote>"

    msg = f"{title}\n\n{today_txt}\n\n{week_txt}\n\n{month_txt}{motd}"

    premium_message: types.Message | None = None
    try:
        premium_capable = await has_premium(client)
    except Exception:
        premium_capable = False

    if premium_capable:
        try:
            premium_message = await _try_send_premium(client, ent, msg)
        except RPCError:
            premium_message = None
            logger.warning("premium_failed_fallback")
        except Exception:
            premium_message = None
            logger.warning("premium_failed_fallback")

    if premium_message is not None:
        sent_message = premium_message
    else:
        sent_message = await client.send_message(ent, msg, parse_mode="html")
    if mark_daily:
        db_set_meta("last_post_date", now.date().isoformat())
    print(f"Posted leaderboard for {now.date().isoformat()} (mark_daily={mark_daily}).")

    try:
        chat_id = int(get_peer_id(ent))
    except Exception:
        chat_id = getattr(getattr(sent_message, "peer_id", None), "channel_id", None)
    snapshot = {
        "posted_at": datetime.now(timezone.utc).isoformat(),
        "message_id": getattr(sent_message, "id", None),
        "chat_id": chat_id,
        "boards": [
            {
                "scope": "day",
                "title": "📅 Today",
                "header": today_hdr,
                "period_start": t_start.isoformat(),
                "period_end": t_end.isoformat(),
                "entries": [dict(entry) for entry in day_entries],
            },
            {
                "scope": "week",
                "title": "📆 This Week",
                "header": week_hdr,
                "period_start": w_start.isoformat(),
                "period_end": w_end.isoformat(),
                "entries": [dict(entry) for entry in week_entries],
            },
            {
                "scope": "month",
                "title": "🗓️ This Month",
                "header": month_hdr,
                "period_start": m_start.isoformat(),
                "period_end": m_end.isoformat(),
                "entries": [dict(entry) for entry in month_entries],
            },
        ],
    }
    export_latest_leaderboards(snapshot)

# ---------- Event-driven participant tracking ----------
class _State:
    ent = None
    seen: dict[int, float] = {}             # active start ts per uid (raw ids)
    last_flush_ts: float = time.time()
    refresh_task: asyncio.Task | None = None

    # Per-session gating state
    current_call_id: int | None = None
    pending_segments: dict[int, list[tuple[float, float]]] = {}  # uid -> list of (start,end) waiting to be gated/committed
    session_accum_secs: dict[int, int] = {}     # uid -> total seconds accrued this session (sum of pending + committed this session)
    session_qualified: dict[int, bool] = {}     # uid -> has crossed SESSION_MIN_SECONDS in this session

    # Quiet logging controls
    call_active: bool = False
    last_roster_sig: str = ""
    last_roster_print_ts: float = 0.0

    # Watchdog / heartbeat
    start_ts: float = time.time()
    last_ok_snapshot_ts: float = time.time()
    incident_thresholds_sent: set[int] = set()

STATE = _State()

def _start_new_session(call_id: int):
    STATE.current_call_id = call_id
    STATE.pending_segments = {}
    STATE.session_accum_secs = {}
    STATE.session_qualified = {}
    STATE.seen = {}
    STATE.last_flush_ts = time.time()
    print(f"[session] New group call id={call_id} started; gating <{SESSION_MIN_SECONDS}s per session.")

def _finalize_session(now_ts: float):
    """Close all open segments; commit those users who met the 5m gate; drop the rest."""
    for uid, start_ts in list(STATE.seen.items()):
        if now_ts > start_ts:
            STATE.pending_segments.setdefault(uid, []).append((start_ts, now_ts))
            STATE.session_accum_secs[uid] = STATE.session_accum_secs.get(uid, 0) + int(now_ts - start_ts)
    STATE.seen.clear()

    for uid, segs in list(STATE.pending_segments.items()):
        total = STATE.session_accum_secs.get(uid, 0)
        qualified = total >= SESSION_MIN_SECONDS or STATE.session_qualified.get(uid, False)
        if not qualified:
            continue
        for (s, e) in segs:
            db_add_span(uid, s, e)
    STATE.pending_segments.clear()
    STATE.session_accum_secs.clear()
    STATE.session_qualified.clear()
    STATE.current_call_id = None
    print("[session] Ended; committed only qualified users (>=5m).")

# ---------- Watchdog helpers ----------
async def _notify_admin(text: str):
    """Send a short DM alert to your main account from the account logged into study_session."""
    try:
        await ensure_connected()
        await client.send_message(WATCHDOG_NOTIFY_TO, f"⚠️ StudyTracker: {text}")
    except Exception as e:
        _log_exc("notify failed", e)

def _note_ok_snapshot():
    """
    Mark a healthy heartbeat. If we were in an incident (one or more alerts sent),
    send a single recovery DM and clear the incident state.
    """
    was_in_incident = bool(STATE.incident_thresholds_sent)
    STATE.last_ok_snapshot_ts = time.time()
    if was_in_incident:
        try:
            asyncio.create_task(_notify_admin("it is working again ✅ You are good to go."))
        except Exception:
            pass
        STATE.incident_thresholds_sent.clear()

async def _check_watchdog():
    """
    If no successful snapshot for 5/10/15 minutes, send ONE alert at each threshold.
    After 15 minutes: no more reminders until it recovers (then a single ✅ message).
    """
    now = time.time()

    # Avoid false alerts immediately on boot/start
    if now - STATE.start_ts < HEARTBEAT_THRESHOLDS[0]:
        return

    elapsed = now - STATE.last_ok_snapshot_ts
    for thr in HEARTBEAT_THRESHOLDS:
        if elapsed >= thr and thr not in STATE.incident_thresholds_sent:
            STATE.incident_thresholds_sent.add(thr)
            mins = thr // 60
            await _notify_admin(f"no tracking heartbeat for {mins} minutes. please check the tracker.")
            break  # only one alert per loop

# ---------- Snapshot ----------
async def _refresh_snapshot():
    """Fetch current participants and reconcile joins/leaves vs STATE.seen with 5m per-session gate."""
    if not STATE.ent:
        return
    try:
        call = await get_current_group_call(STATE.ent)
        now_ts = time.time()

        # Detect session transitions
        if not call:
            if STATE.current_call_id is not None:
                _finalize_session(now_ts)
            if STATE.call_active:  # only print when switching from active -> inactive
                print("[snapshot] No active call.")
            STATE.call_active = False
            STATE.last_roster_sig = ""
            _note_ok_snapshot()   # record healthy heartbeat even with no active call
            return
        else:
            if STATE.current_call_id is None or STATE.current_call_id != call.id:
                _start_new_session(call.id)

        # periodic checkpoint of active users (buffer only; commit applies once qualified)
        if STATE.seen and (now_ts - STATE.last_flush_ts) >= FLUSH_EVERY:
            for uid, start_ts in list(STATE.seen.items()):
                if now_ts > start_ts:
                    dur = int(now_ts - start_ts)
                    if STATE.session_qualified.get(uid, False):
                        db_add_span(uid, start_ts, now_ts)
                    else:
                        STATE.pending_segments.setdefault(uid, []).append((start_ts, now_ts))
                        STATE.session_accum_secs[uid] = STATE.session_accum_secs.get(uid, 0) + dur
                        if STATE.session_accum_secs[uid] >= SESSION_MIN_SECONDS:
                            for (s, e) in STATE.pending_segments.get(uid, []):
                                db_add_span(uid, s, e)
                            STATE.pending_segments[uid] = []
                            STATE.session_qualified[uid] = True
                    STATE.seen[uid] = now_ts
            STATE.last_flush_ts = now_ts
            print("[flush] checkpointed active users")

        participants = await fetch_participants(call)
        current = set()
        for uid, _, _ in participants:
            if not uid:
                continue
            if not TRACK_SELF and (MY_ID is not None and uid == MY_ID):
                continue
            current.add(uid)

        # joins
        for uid in current:
            if uid not in STATE.seen:
                STATE.seen[uid] = now_ts

        # leaves
        for uid in list(STATE.seen.keys()):
            if uid not in current:
                start_ts = STATE.seen.pop(uid)
                if now_ts <= start_ts:
                    continue
                dur = int(now_ts - start_ts)
                if STATE.session_qualified.get(uid, False):
                    db_add_span(uid, start_ts, now_ts)
                    STATE.session_accum_secs[uid] = STATE.session_accum_secs.get(uid, 0) + dur
                else:
                    STATE.pending_segments.setdefault(uid, []).append((start_ts, now_ts))
                    STATE.session_accum_secs[uid] = STATE.session_accum_secs.get(uid, 0) + dur
                    if STATE.session_accum_secs[uid] >= SESSION_MIN_SECONDS:
                        for (s, e) in STATE.pending_segments.get(uid, []):
                            db_add_span(uid, s, e)
                        STATE.pending_segments[uid] = []
                        STATE.session_qualified[uid] = True

        # Roster log (canonical labels) — only on change / every few minutes
        STATE.call_active = True
        alias_to_canon, canon_label = _alias_maps_from_cache()
        names_now, ids_for_sig = [], []
        for uid, n, _ in participants:
            if not n or (not TRACK_SELF and uid == MY_ID):
                continue
            cid = alias_to_canon.get(uid, uid)
            label = canon_label.get(cid)
            names_now.append(label if label else n)
            ids_for_sig.append(str(cid))

        roster_sig = ",".join(sorted(ids_for_sig))
        now_str = datetime.now(TZ).strftime('%H:%M:%S')
        if (roster_sig != STATE.last_roster_sig) or (time.time() - STATE.last_roster_print_ts >= ROSTER_LOG_EVERY):
            roster = ", ".join(names_now) if names_now else "—"
            print(f"[{now_str}] In call ({len(set(ids_for_sig))}): {roster}")
            STATE.last_roster_sig = roster_sig
            STATE.last_roster_print_ts = time.time()

        _note_ok_snapshot()  # snapshot completed fine
    except Exception as e:
        _log_exc("Snapshot error", e)

def _schedule_refresh():
    # Debounce multiple raw updates by scheduling one task
    if STATE.refresh_task and not STATE.refresh_task.done():
        return
    STATE.refresh_task = asyncio.create_task(_refresh_snapshot())

@client.on(events.Raw)
async def _raw_handler(update):
    # React to call-related updates: UpdateGroupCall, UpdateGroupCallParticipants
    if isinstance(update, (types.UpdateGroupCall, types.UpdateGroupCallParticipants)):
        _schedule_refresh()

# ---------- Persistent anchor ----------
def _ensure_anchor() -> datetime:
    v = db_get_meta("anchor_date")
    if v:
        try: return datetime.fromisoformat(v).replace(tzinfo=TZ)
        except Exception: pass
    today = datetime.now(TZ).replace(hour=0, minute=0, second=0, microsecond=0)
    db_set_meta("anchor_date", today.date().isoformat())
    return today

def _format_d(d: datetime) -> str: return d.strftime("%d.%m.%y")
def _dow(d: datetime) -> str:      return d.strftime("%A").upper()
def _day_index(anchor: datetime, today: datetime) -> int:
    return (today.date() - anchor.date()).days + 1

def _week_block(anchor: datetime, today: datetime):
    days = (today.date() - anchor.date()).days
    idx = (days // 7) + 1
    start = anchor + timedelta(days=(idx-1)*7)
    end   = start + timedelta(days=6)
    return idx, start, end

def _month30_block(anchor: datetime, today: datetime):
    days = (today.date() - anchor.date()).days
    idx = (days // 30) + 1
    start = anchor + timedelta(days=(idx-1)*30)
    end   = start + timedelta(days=29)
    return idx, start, end

# ---------- NEW: catch-up DM on startup (6/6) ----------
async def _notify_catchup_if_needed():
    state = _load_state()
    last = state.get("last_seen")
    if last:
        gap = int(_now_ts() - last)
        if gap > 600:
            try:
                await client.send_message(WATCHDOG_NOTIFY_TO, f"Tracker restarted after downtime of ~{gap//60} min.")
            except Exception:
                pass

# ---------- Main loop ----------
async def main():
    global MY_ID, _last_idle_beat, _last_offline_beat, _hb_thr
    assert_session_free()
    db_init()

    if "_hb_thr" not in globals():
        _hb_thr = threading.Thread(target=_heartbeat, name="heartbeat", daemon=True)
        _hb_thr.start()

    try:
        await client.connect()
    except Exception as e:
        _log_exc("Initial connect failed (will retry)", e)

    while True:
        try:
            is_auth = await client.is_user_authorized()
        except Exception:
            is_auth = False
        if is_auth:
            break
        print("Not logged in. Waiting for session; will retry every 60s.")
        await asyncio.sleep(60)
        try:
            await client.connect()
        except Exception:
            pass

    STATE.ent = await resolve_group(GROUP)
    _maybe_reset_on_group_change(STATE.ent)

    print("Tracker running. Will post automatically at 21:00 Asia/Tashkent.")
    me = await client.get_me()
    MY_ID = me.id
    print("Running as user id:", MY_ID)

    # First snapshot immediately (in case call already active)
    await _refresh_snapshot()

    # NEW: catch-up DM if we were down a while (6/6)
    await _notify_catchup_if_needed()

    # --- Backfill any missed days on startup (and today if after 22:00) ---
    last_posted = db_get_meta("last_post_date")  # ISO string or None
    now = datetime.now(TZ)
    today = now.date()

    def _parse_date(s: str | None) -> date | None:
        if not s: return None
        try: return datetime.fromisoformat(s).date()
        except Exception: return None

    lp = _parse_date(last_posted)
    to_post: list[date] = []

    if lp:
        d = lp + timedelta(days=1)
        while d < today:        # all fully missed days before today
            to_post.append(d)
            d += timedelta(days=1)
        if (now.hour, now.minute) >= (POST_HOUR, POST_MINUTE) and lp != today:
            to_post.append(today)  # also post today immediately if we’re already past 22:00
    else:
        # first-ever run: only post immediately if we’re already past 22:00 today
        if (now.hour, now.minute) >= (POST_HOUR, POST_MINUTE):
            to_post.append(today)

    for d in to_post:
        target_dt = datetime(d.year, d.month, d.day, POST_HOUR, POST_MINUTE, tzinfo=TZ)
        print(f"[catch-up] Backfilling leaderboard for {d.isoformat()}")
        try:
            await post_leaderboard(
                STATE.ent,
                live_seen_snapshot=None,
                session_accum_secs=None,
                session_qualified=None,
                mark_daily=True,
                override_now=target_dt
            )
            last_posted = d.isoformat()
        except Exception as e:
            _log_exc("Catch-up post error", e)

    # Initialize state file (5/6 – we also update every loop)
    state = _load_state()
    state["last_seen"] = _now_ts()
    _save_state(state)

    # Safety snapshot poll + daily post + manual post-now flag + NEW heartbeats (5/6)
    while True:
        # If offline: emit quiet 1-min heartbeat and skip heavy work
        if not await ensure_connected():
            now_ts = _now_ts()
            if (_last_offline_beat == 0.0) or (now_ts - _last_offline_beat >= HEARTBEAT_OFFLINE_EVERY):
                _log_beat("offline; waiting for Telegram/network…")
                _last_offline_beat = now_ts
                state["last_seen"] = now_ts
                _save_state(state)
            await asyncio.sleep(5)
            continue

        # Connected path (clear offline cadence)
        _last_offline_beat = 0.0

        now = datetime.now(TZ)
        today_str = now.date().isoformat()

        # Manual "post now" (does NOT mark daily posted)
        if os.path.exists(CONTROL_POST_NOW_FILE):
            try:
                await post_leaderboard(
                    STATE.ent,
                    live_seen_snapshot=STATE.seen.copy(),
                    session_accum_secs=STATE.session_accum_secs.copy(),
                    session_qualified=STATE.session_qualified.copy(),
                    mark_daily=False
                )
            finally:
                try: os.remove(CONTROL_POST_NOW_FILE)
                except Exception: pass

        # Daily scheduled post
        last_posted = db_get_meta("last_post_date")
        if (now.hour, now.minute) >= (POST_HOUR, POST_MINUTE) and last_posted != today_str:
            try:
                await post_leaderboard(
                    STATE.ent,
                    live_seen_snapshot=STATE.seen.copy(),
                    session_accum_secs=STATE.session_accum_secs.copy(),
                    session_qualified=STATE.session_qualified.copy(),
                    mark_daily=True
                )
                last_posted = today_str
            except Exception as e:
                _log_exc("Post error", e)

        # Safety snapshot in case no raw updates arrived recently
        await _refresh_snapshot()

        # NEW: idle heartbeat every 10 min when no livestream (5/6)
        now_ts = _now_ts()
        if not STATE.call_active and (_last_idle_beat == 0.0 or (now_ts - _last_idle_beat >= HEARTBEAT_IDLE_EVERY)):
            _log_beat("In call (0): —")
            _last_idle_beat = now_ts

        # 5/10/15-min watchdog
        await _check_watchdog()

        # remember last alive
        state["last_seen"] = now_ts
        _save_state(state)

        await asyncio.sleep(SNAPSHOT_POLL_EVERY)

if __name__ == "__main__":
    try: asyncio.run(main())
    except KeyboardInterrupt:
        try:
            _hb_stop.set()
            if "_hb_thr" in globals():
                _hb_thr.join(timeout=2)
        except Exception:
            pass
        print("\nExiting cleanly. Bye")
