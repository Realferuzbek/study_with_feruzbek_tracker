# Touch a flag file the running tracker watches; it will post immediately.
import time, os
with open("post_now.flag", "w", encoding="utf-8") as f:
    f.write(str(time.time()))
print("Requested immediate leaderboard post.")
