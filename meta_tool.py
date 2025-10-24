# F:\study_with_me\meta_tool.py
import sqlite3, sys, datetime

DB = r"F:\study_with_me\study.db"
con = sqlite3.connect(DB); cur = con.cursor()

def show():
    print("== meta ==")
    for k, v in cur.execute("SELECT k, v FROM meta ORDER BY k"):
        print(f"{k} = {v}")
    print("\n== counts ==")
    cur.execute("SELECT COUNT(*) FROM seconds_totals"); print("seconds_totals rows:", cur.fetchone()[0])
    cur.execute("SELECT COUNT(*) FROM compliments_period"); print("compliments_period rows:", cur.fetchone()[0])

def hard_reset():
    today = datetime.date.today().isoformat()
    # keep group_key; wipe counters/period state; re-anchor to today
    cur.execute("SELECT v FROM meta WHERE k='group_key'")
    row = cur.fetchone()
    group_key = row[0] if row else None

    cur.execute("DELETE FROM seconds_totals")
    cur.execute("DELETE FROM compliments_period")
    cur.execute("DELETE FROM meta WHERE k IN ('last_post_date','anchor_date','quote_index','quote_seed')")
    cur.execute("INSERT OR REPLACE INTO meta(k,v) VALUES('anchor_date',?)", (today,))
    if group_key:
        cur.execute("INSERT OR REPLACE INTO meta(k,v) VALUES('group_key',?)", (group_key,))
    con.commit()
    print("Hard reset done. Anchor set to", today)

if __name__ == "__main__":
    if "--show" in sys.argv:
        show()
    elif "--hard-reset" in sys.argv:
        hard_reset()
    else:
        print("Usage:")
        print("  python meta_tool.py --show")
        print("  python meta_tool.py --hard-reset")

con.close()