# reset_all.py
import sqlite3
from pathlib import Path
from datetime import datetime, timedelta, timezone
try:
    from zoneinfo import ZoneInfo
    TZ = ZoneInfo("Asia/Tashkent")
except Exception:
    TZ = timezone(timedelta(hours=5))

BASE_DIR = Path(__file__).resolve().parent
VAR_DIR = BASE_DIR / "var"
VAR_DIR.mkdir(parents=True, exist_ok=True)
DB = VAR_DIR / "study.db"
POST_NOW_FLAG = BASE_DIR / "post_now.flag"

def set_meta(cur, k, v):
    cur.execute("INSERT OR REPLACE INTO meta(k,v) VALUES(?,?)", (k, v))

con = sqlite3.connect(str(DB), timeout=30)
cur = con.cursor()
# wipe totals + compliments
cur.execute("DELETE FROM seconds_totals")
cur.execute("DELETE FROM compliments_period")
# wipe timing/meta but keep group_key (so we stay in same group)
cur.execute("DELETE FROM meta WHERE k IN ('last_post_date','anchor_date','quote_index','quote_seed','group_since')")
anchor = datetime.now(TZ).date().isoformat()
set_meta(cur, "anchor_date", anchor)   # DAY 1 anchor
set_meta(cur, "quote_index", "0")
set_meta(cur, "group_since", anchor)
con.commit()
con.close()

try: POST_NOW_FLAG.unlink()
except FileNotFoundError: pass

print(f"Reset done. New anchor_date={anchor}. Next post will be DAY 1, WEEK 1, MONTH 1 from this date.")

