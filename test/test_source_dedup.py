#!/usr/bin/env python3
"""
Deduplication tests for the MCP event server source connectors.

OPTIMIZATION (2026-08): this file now starts ZERO MCP servers. Previously it
spawned up to 3 server subprocesses (D1/D2/D3 across a real restart, S6 with
http_poller). All dedup behavior is application-layer and is now exercised
DIRECTLY against ``HttpJsonPoller`` / ``EventStore`` / ``Publisher`` with an
isolated temp DB, a mock HTTP server, and an injectable bus. Durable restart
dedup (D1/D2/D3) is simulated by two source sessions pointing at the SAME
SQLite file — the exact mechanism that makes a real process restart safe.

Legacy IDs preserved: D1, D2, D3, D4, D5, S6.  The http-poller boundary check
formerly in test_events.py (P8T7) is also exercised here directly.

Run:
    python test/test_source_dedup.py
"""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import sqlite3
import sys
import tempfile
from typing import Any

# Make the test dir importable so the shared modules resolve regardless of cwd.
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_DIR = os.path.dirname(_SCRIPT_DIR)
if _PROJECT_DIR not in sys.path:
    sys.path.insert(0, _PROJECT_DIR)
if _SCRIPT_DIR not in sys.path:
    sys.path.insert(0, _SCRIPT_DIR)

import store as store_mod  # noqa: E402
from sources.http_poller import HttpJsonPoller  # noqa: E402
from sources import create_publisher  # noqa: E402
from mcp.server.subscriptions import InMemorySubscriptionBus  # noqa: E402

from helpers.runner import R  # noqa: E402
from helpers.mock_http import start_mock  # noqa: E402
from helpers.wait import wait_for_value  # noqa: E402


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------


def _db_event_type_count(db_path: str, event_type: str) -> int:
    try:
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA busy_timeout=5000")
        row = conn.execute(
            "SELECT COUNT(*) FROM persistent_events WHERE type=?", (event_type,)
        ).fetchone()
        return row[0] if row else 0
    except (OSError, sqlite3.OperationalError):
        return 0
    finally:
        conn.close()


def _db_external_ids(db_path: str, event_type: str) -> set[str]:
    try:
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA busy_timeout=5000")
        rows = conn.execute(
            "SELECT data FROM persistent_events WHERE type=?", (event_type,)
        ).fetchall()
        ids: set[str] = set()
        for (d,) in rows:
            try:
                ids.add(json.loads(d).get("external_id"))
            except (json.JSONDecodeError, TypeError):
                pass
        return ids
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Durable Deduplication Tests (D1/D2/D3) — two sessions on ONE SQLite file
# ---------------------------------------------------------------------------


async def test_durable_dedup_cross_restart(runner: R) -> None:
    """D1 first ID published (1 event + 1 seen row); D2 same ID next poll (0 new);
    D3 restart, same ID (0 new — durable dedup, DB count stays 1)."""
    name = "D1-D2-D3-durable-dedup"
    mock_items = [{"id": "dedup-X", "title": "one"}]
    mock_srv, mock_port = start_mock(mock_items)
    tmp = tempfile.mkdtemp(prefix="ded_")
    db_path = os.path.join(tmp, "events.db")
    try:
        base_cfg = {
            "url": f"http://127.0.0.1:{mock_port}/api",
            "interval_seconds": 1,
            "timeout_seconds": 5,
            "item_path": "",
            "id_path": "id",
            "event_type_prefix": "test.dedup",
            "source_name": "http_poller",
            "persistent": True,
            "dedup": {"enabled": True, "max_items": 10000},
        }

        # ---- Session 1: publish the item once, keep polling (dedup skips next) ----
        store1 = store_mod.EventStore(db_path)
        bus1 = InMemorySubscriptionBus()
        pub1 = create_publisher(store1, bus1)
        poller1 = HttpJsonPoller(dict(base_cfg))
        stop1 = asyncio.Event()
        task1 = asyncio.create_task(poller1.run(pub1, stop1))
        try:
            await wait_for_value(
                lambda: store1.source_item_seen("http_poller", "dedup-X"),
                True, timeout=15, description="D1 seen",
            )
            runner.assert_true(name + "-D1-seen-row",
                              store1.source_item_seen("http_poller", "dedup-X"),
                              "seen row must exist after publish")
            # D1 + D2: exactly one persistent event so far.
            runner.assert_eq(name + "-D1-D2-db-count",
                            _db_event_type_count(db_path, "test.dedup.item.received"), 1)
        finally:
            stop1.set()
            try:
                await asyncio.wait_for(task1, timeout=3)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                pass

        # ---- Session 2 (restart): brand-new store + poller, SAME db file ----
        await asyncio.sleep(0.3)
        store2 = store_mod.EventStore(db_path)
        bus2 = InMemorySubscriptionBus()
        pub2 = create_publisher(store2, bus2)
        poller2 = HttpJsonPoller(dict(base_cfg))
        stop2 = asyncio.Event()
        task2 = asyncio.create_task(poller2.run(pub2, stop2))
        try:
            await asyncio.sleep(2.5)
            # D3: durable dedup prevents any republish after restart.
            runner.assert_eq(name + "-D3-republished",
                            poller2._events_published, 0)
            runner.assert_eq(name + "-D3-db-count",
                            _db_event_type_count(db_path, "test.dedup.item.received"), 1)
        finally:
            stop2.set()
            try:
                await asyncio.wait_for(task2, timeout=3)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                pass
    finally:
        mock_srv.shutdown()
        shutil.rmtree(tmp, ignore_errors=True)


# ---------------------------------------------------------------------------
# Publish Failure Dedup Tests (D4/D5)
# ---------------------------------------------------------------------------


async def test_dedup_on_publish_failure(runner: R) -> None:
    """D4 publish fails -> seen row NOT created; D5 retry succeeds -> event + seen row."""
    name = "D4-D5-pub-failure"
    mock_items = [{"id": "fail-X", "title": "one"}]
    mock_srv, mock_port = start_mock(mock_items)

    # D4: a store whose save() always fails
    class FailingStore(store_mod.EventStore):
        def save(self, *a: Any, **k: Any) -> int:
            raise RuntimeError("simulated persistence failure")

    test_db = os.path.join(tempfile.mkdtemp(prefix="d4_"), "events.db")
    try:
        fstore = FailingStore(test_db)
        bus = InMemorySubscriptionBus()
        failing_pub = create_publisher(fstore, bus)
        poller = HttpJsonPoller({
            "url": f"http://127.0.0.1:{mock_port}/api",
            "interval_seconds": 1000,
            "id_path": "id",
            "event_type_prefix": "test.d4",
            "source_name": "http_poller",
        })
        stop = asyncio.Event()
        task = asyncio.create_task(poller.run(failing_pub, stop))
        try:
            await asyncio.sleep(2.0)
        finally:
            stop.set()
            try:
                await asyncio.wait_for(task, timeout=3)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                pass
        runner.assert_eq(name + "-D4-events-published", poller._events_published, 0)
        runner.assert_false(name + "-D4-no-seen-row",
                           fstore.source_item_seen("http_poller", "fail-X"),
                           "seen row must NOT be created when publish fails")

        # D5: a working store
        wstore = store_mod.EventStore(test_db)
        working_pub = create_publisher(wstore, bus)
        poller2 = HttpJsonPoller({
            "url": f"http://127.0.0.1:{mock_port}/api",
            "interval_seconds": 1000,
            "id_path": "id",
            "event_type_prefix": "test.d5",
            "source_name": "http_poller",
        })
        stop2 = asyncio.Event()
        task2 = asyncio.create_task(poller2.run(working_pub, stop2))
        try:
            await asyncio.sleep(2.0)
        finally:
            stop2.set()
            try:
                await asyncio.wait_for(task2, timeout=3)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                pass
        runner.assert_ge(name + "-D5-events-published", poller2._events_published, 1)
        runner.assert_true(name + "-D5-seen-row",
                          wstore.source_item_seen("http_poller", "fail-X"),
                          "seen row must be created after successful publish")
    finally:
        mock_srv.shutdown()
        for suffix in ("", "-wal", "-shm", "-journal"):
            p = test_db + suffix
            if os.path.exists(p):
                try:
                    os.remove(p)
                except OSError:
                    pass


# ---------------------------------------------------------------------------
# Poller In-Memory + Durable Dedup (S6) — direct HttpJsonPoller
# ---------------------------------------------------------------------------


async def test_dedup_within_poller(runner: R) -> None:
    """S6: dedup — same item across polls -> exactly 1 event (durable + in-memory)."""
    name = "S6-dedup"
    mock_items = [{"id": "s6-item", "title": "one"}]
    mock_srv, mock_port = start_mock(mock_items)
    tmp = tempfile.mkdtemp(prefix="s6_")
    db_path = os.path.join(tmp, "events.db")
    try:
        store = store_mod.EventStore(db_path)
        bus = InMemorySubscriptionBus()
        pub = create_publisher(store, bus)
        cfg = {
            "url": f"http://127.0.0.1:{mock_port}/api",
            "interval_seconds": 1,
            "timeout_seconds": 5,
            "item_path": "",
            "id_path": "id",
            "event_type_prefix": "test.s6",
            "source_name": "http_poller",
            "persistent": False,
            "dedup": {"enabled": True, "max_items": 10000},
        }
        poller = HttpJsonPoller(cfg)
        stop = asyncio.Event()
        task = asyncio.create_task(poller.run(pub, stop))
        try:
            await wait_for_value(lambda: poller._events_published, 1, timeout=15,
                                 description="S6 first publish")
            runner.assert_eq(name + "-first", poller._events_published, 1)
            await asyncio.sleep(2.5)
            runner.assert_eq(name + "-still-one", poller._events_published, 1)
        finally:
            stop.set()
            try:
                await asyncio.wait_for(task, timeout=3)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                pass
    finally:
        mock_srv.shutdown()
        shutil.rmtree(tmp, ignore_errors=True)


# ---------------------------------------------------------------------------
# HTTP Poller over real HTTP (relocated P8T7) — 3 distinct items -> 3 events
# ---------------------------------------------------------------------------


async def test_http_poller_three_items(runner: R) -> None:
    """P8T7: HTTP poller with mock server — picks up 3 distinct items -> 3 events."""
    name = "P8T7-http-poller"
    mock_items = [
        {"id": "mock-1", "title": "Item 1", "ts": "2026-01-01T00:00:00Z"},
        {"id": "mock-2", "title": "Item 2", "ts": "2026-01-01T00:01:00Z"},
        {"id": "mock-3", "title": "Item 3", "ts": "2026-01-01T00:02:00Z"},
    ]
    mock_srv, mock_port = start_mock(mock_items)
    tmp = tempfile.mkdtemp(prefix="p8t7_")
    db_path = os.path.join(tmp, "events.db")
    try:
        store = store_mod.EventStore(db_path)
        bus = InMemorySubscriptionBus()
        pub = create_publisher(store, bus)
        cfg = {
            "url": f"http://127.0.0.1:{mock_port}/api",
            "interval_seconds": 1000,
            "timeout_seconds": 5,
            "item_path": "",
            "id_path": "id",
            "timestamp_path": "ts",
            "event_type_prefix": "test.http_poller",
            "source_name": "http_poller",
            "persistent": True,
            "dedup": {"enabled": True, "max_items": 10000},
        }
        poller = HttpJsonPoller(cfg)
        stop = asyncio.Event()
        task = asyncio.create_task(poller.run(pub, stop))
        try:
            await wait_for_value(lambda: poller._events_published, 3, timeout=15,
                                 description="P8T7 three items")
            runner.assert_eq(name + "-got-3-events", poller._events_published, 3)
            ext_ids = _db_external_ids(db_path, "test.http_poller.item.received")
            runner.assert_eq(name + "-unique-ids", len(ext_ids), 3)
            # Source recorded on the published event.
            conn = sqlite3.connect(db_path)
            try:
                row = conn.execute(
                    "SELECT source FROM persistent_events WHERE type=? LIMIT 1",
                    ("test.http_poller.item.received",),
                ).fetchone()
                runner.assert_eq(name + "-source", row[0] if row else None, "http_poller")
            finally:
                conn.close()
        finally:
            stop.set()
            try:
                await asyncio.wait_for(task, timeout=3)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                pass
    finally:
        mock_srv.shutdown()
        shutil.rmtree(tmp, ignore_errors=True)


# ===================================================================
# Main
# ===================================================================


async def main() -> bool:
    runner = R()

    await test_durable_dedup_cross_restart(runner)
    await test_dedup_on_publish_failure(runner)
    await test_dedup_within_poller(runner)
    await test_http_poller_three_items(runner)

    return runner.summary()


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
