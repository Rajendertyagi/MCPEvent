import sys, os, sqlite3, subprocess, time, json
sys.path.insert(0, r"D:\Temp\mcp-event")
import store as sm

db = r"D:\Temp\mcp-event\_ro_test.db"
for s in ("", "-wal", "-shm"):
    p = db + s
    if os.path.exists(p):
        os.remove(p)

es = sm.EventStore(db)
# Normal write works
es.mark_source_item_seen("s", "X", "2026-01-01")
print("write before RO:", es.source_item_seen("s", "X"))

# Make read-only
os.chmod(db, 0o444)
try:
    es.mark_source_item_seen("s", "Y", "2026-01-01")
    print("write AFTER RO: SUCCEEDED (chmod did not block)")
except Exception as e:
    print("write AFTER RO: FAILED ->", type(e).__name__, str(e)[:80])

# Try a fresh connection (like the server's save would)
try:
    c = sqlite3.connect(db)
    c.execute("BEGIN")
    c.execute("INSERT OR IGNORE INTO source_seen_items (source_name, external_id, seen_at) VALUES (?,?,?)", ("s2", "Z", "2026-01-01"))
    c.commit()
    c.close()
    print("fresh conn write AFTER RO: SUCCEEDED")
except Exception as e:
    print("fresh conn write AFTER RO: FAILED ->", type(e).__name__, str(e)[:80])

os.chmod(db, 0o666)
for s in ("", "-wal", "-shm"):
    p = db + s
    if os.path.exists(p):
        os.remove(p)
print("DONE")
