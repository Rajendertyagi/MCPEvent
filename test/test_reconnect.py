#!/usr/bin/env python3
"""Restart / reconnection tests — extracted from integrate_test.py and test_phase8.py.

Covers:
  * T10: Persistence — events survive server restart (from integrate_test.py)
  * P7T13: Graceful restart — state preserved (from integrate_test.py)
  * P8T10: source_state survives restart (from test_phase8.py)

Legacy IDs preserved in comments.

Run independently:
    python test/test_reconnect.py
"""

from __future__ import annotations

import asyncio
import os
import sqlite3
import sys
import time

# Allow importing helpers regardless of launch cwd.
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_DIR = os.path.dirname(_SCRIPT_DIR)
for _p in (_PROJECT_DIR, _SCRIPT_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from helpers.lifecycle import (  # noqa: E402
    start_server,
    stop_server,
    restart_server,
    restore_environment,
    get_server_url,
    wait_mcp_ready,
)
from helpers.mcp import (  # noqa: E402
    call,
    read_res,
    list_tools_names,
)
from helpers.runner import R  # noqa: E402
from mcp_result import reserve_free_port, safe_teardown  # noqa: E402


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------

def _uid(suffix: str = "") -> str:
    return f"reconnect-{suffix}-{int(time.time()*1000)}"


def _db_persistent_count(data_dir: str) -> int:
    """Count rows in persistent_events for the given data dir."""
    db_path = os.path.join(data_dir, "events.db")
    try:
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA busy_timeout=5000")
        row = conn.execute("SELECT COUNT(*) FROM persistent_events").fetchone()
        return row[0] if row else 0
    except (OSError, sqlite3.OperationalError):
        return 0


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

async def T10(runner: R) -> None:
    """T10: Persistence — events survive server restart.

    legacy_id: T10 (from integrate_test.py)
    """
    name = "T10-persistence"
    proc = None
    try:
        proc = await start_server()
        cid = _uid("t10")
        await call("consumer_register", {"consumer_id": cid})

        resp = await call("event_publish", {
            "event_type": "test.t10",
            "source": "test",
            "persistent": True,
        })
        eid = resp["event"]["id"]
        seq = resp["event"]["sequence"]

        # Restart using the helper (stops the running server first, then starts
        # a fresh one — preserving the data dir, and leaving no orphan process).
        proc = await restart_server()

        # Event should survive.
        pending = await call("consumer_event_pending_list", {"consumer_id": cid})
        ids = {e["id"] for e in pending.get("events", [])}
        runner.assert_true(name, eid in ids, f"event {eid} lost after restart")
    except Exception as exc:
        runner.fail(name, str(exc))
    finally:
        safe_teardown(stop_server, proc)
        safe_teardown(restore_environment)


async def P7T13(runner: R) -> None:
    """P7-T13: Graceful restart — state preserved.

    legacy_id: P7T13 (from integrate_test.py)
    """
    name = "P7T13-graceful-restart"
    proc = None
    try:
        proc = await start_server()
        cid = _uid("p7t13")
        await call("consumer_register", {"consumer_id": cid})
        resp = await call("event_publish", {
            "event_type": "test.p7t13",
            "source": "test",
            "persistent": True,
        })
        eid = resp["event"]["id"]

        # Restart using the helper (stops the running server first).
        proc = await restart_server()

        cp_resp = await call("consumer_checkpoint_get", {"consumer_id": cid})
        runner.assert_eq(name + "-cp", cp_resp.get("checkpoint"), 0)
        pending = await call("consumer_event_pending_list", {"consumer_id": cid})
        ids = {e["id"] for e in pending.get("events", [])}
        runner.assert_true(name + "-evt", eid in ids, "event lost after restart")
    except Exception as exc:
        runner.fail(name, str(exc))
    finally:
        safe_teardown(stop_server, proc)
        safe_teardown(restore_environment)


async def P8T10(runner: R) -> None:
    """P8-T10: source_state survives restart — set cursor, restart, cursor still there.

    legacy_id: P8T10 (from test_phase8.py)
    """
    name = "P8T10-state-survives"
    proc = None
    try:
        proc = await start_server()

        # Write source_state directly into the DB before restart.
        data_dir = os.path.join(_PROJECT_DIR, "data_test")
        db_path = os.path.join(data_dir, "events.db")
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        import store as store_mod
        es = store_mod.EventStore(db_path)
        es.set_source_state("survival_source", "cursor", "checkpoint_42")
        if hasattr(es, "close"):
            es.close()
        runner.ok(name + "-set")

        # Restart using the helper (preserves the data dir; stops the running
        # server first so no orphan process is left behind).
        proc = await restart_server()

        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA busy_timeout=5000")
        row = conn.execute(
            "SELECT value FROM source_state WHERE source_name = ? AND key = ?",
            ("survival_source", "cursor"),
        ).fetchone()
        conn.close()
        runner.assert_eq(name + "-after-restart", row[0] if row else None, "checkpoint_42")
    except Exception as exc:
        runner.fail(name, str(exc))
    finally:
        safe_teardown(stop_server, proc)
        safe_teardown(restore_environment)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main() -> int:
    runner = R()

    tests = [
        T10,
        P7T13,
        P8T10,
    ]

    for test_fn in tests:
        await test_fn(runner)

    success = runner.summary()
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
