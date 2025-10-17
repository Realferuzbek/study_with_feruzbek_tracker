# F:\study_with_me\reset_all.py
import sqlite3, os
from datetime import datetime, timedelta, timezone
try:
    from zoneinfo import ZoneInfo
    TZ = ZoneInfo("Asia/Tashkent")
except Exception:
    TZ = timezone(timedelta(hours=5))

DB = r"F:\study_with_me\study.db"

def set_meta(cur, k, v):
    cur.execute("INSERT OR REPLACE INTO meta(k,v) VALUES(?,?)", (k, v))

con = sqlite3.connect(DB, timeout=30)
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

try: os.remove(r"F:\study_with_me\post_now.flag")
except FileNotFoundError: pass

print(f"Reset done. New anchor_date={anchor}. Next post will be DAY 1, WEEK 1, MONTH 1 from this date.")
