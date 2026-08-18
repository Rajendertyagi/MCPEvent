#!/usr/bin/env python3
"""Source lifecycle tests — extracted from test_phase8.py.

Covers source enable/disable behavior, event limits, persistence, dedup,
restart dedup, external failures, recovery, timeouts, cancellation,
publication failure, malformed payloads, concurrent live+replay, and a full
regression without sources.

OPTIMIZATION (2026-08): the application-layer behavior of every source test
is now exercised DIRECTLY against ``SourceManager`` / ``EventStore`` /
``BackgroundTaskManager`` with an isolated temp DB and an injectable stub
bus — no MCP server required. This removes ~14 server subprocess starts that
previously made this file one of the slowest in the suite. Exactly ONE test
(S15) keeps a real MCP server as a representative end-to-end integration check
through the HTTP boundary, so the production wiring (SourceManager inside the
lifespan) is still verified.

Legacy IDs preserved in comments: S1, S2, S3, S5, S7, S8, S9, S10, S11, S12,
S13, S14, S15, R4.

Run independently:
    python test/test_source_lifecycle.py
"""

from __future__ import annotations

import asyncio
import os
import shutil
import sqlite3
import sys
import tempfile
import threading
from contextlib import asynccontextmanager
from http.server import HTTPServer

# Allow importing helpers regardless of launch cwd.
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_DIR = os.path.dirname(_SCRIPT_DIR)
for _p in (_PROJECT_DIR, _SCRIPT_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import store as store_mod  # noqa: E402
from store import EventStore  # noqa: E402
from runtime import BackgroundTaskManager  # noqa: E402
from sources import build_source_manager  # noqa: E402

from helpers.lifecycle import (  # noqa: E402
    start_server,
    stop_server,
    restore_environment,
)
from helpers.mcp import call  # noqa: E402
from helpers.mock_http import MockHandler, start_mock  # noqa: E402
from helpers.runner import R  # noqa: E402
from helpers.wait import wait_for_value, wait_until  # noqa: E402
from mcp_result import safe_teardown  # noqa: E402


# ---------------------------------------------------------------------------
# Direct (B-level) source harness — no MCP server
# ---------------------------------------------------------------------------

class _StubBus:
    """Minimal subscription bus: records the last notification, does nothing else."""

    def __init__(self) -> None:
        self.last = None

    async def publish(self, item: object) -> None:
        self.last = item


def _src_state(sm, name: str) -> str:
    return sm.get_status().get(name, {}).get("state", "unknown")


def _db_persistent_count(db_path: str) -> int:
    try:
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA busy_timeout=5000")
        row = conn.execute("SELECT COUNT(*) FROM persistent_events").fetchone()
        return row[0] if row else 0
    except (OSError, sqlite3.OperationalError):
        return 0
    finally:
        conn.close()


def _db_seen_count(db_path: str, source_name: str) -> int:
    try:
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA busy_timeout=5000")
        row = conn.execute(
            "SELECT COUNT(*) FROM source_seen_items WHERE source_name=?", (source_name,)
        ).fetchone()
        return row[0] if row else 0
    except (OSError, sqlite3.OperationalError):
        return 0
    finally:
        conn.close()


@asynccontextmanager
async def _source_harness(cfg: dict):
    """Wire an isolated EventStore + BackgroundTaskManager + SourceManager for one
    source, start it, yield (store, bus, bg, sm, db_path), then clean up.
    """
    name = cfg["source_name"]
    tmp = tempfile.mkdtemp(prefix="lifecycle_")
    db_path = os.path.join(tmp, "events.db")
    store = EventStore(db_path)
    bus = _StubBus()
    bg = BackgroundTaskManager()
    sm = build_source_manager({name: cfg})
    await sm.initialize(bg, store, bus)
    await sm.start_all({name: cfg})
    try:
        yield store, bus, bg, sm, db_path
    finally:
        try:
            await sm.shutdown()
        except Exception:
            pass
        await bg.shutdown_all(timeout=3)
        shutil.rmtree(tmp, ignore_errors=True)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

async def S1(runner: R) -> None:
    """S1: test_source enabled -> source task running, status reflects it."""
    name = "S1-source-running"
    cfg = {"type": "test_source", "source_name": "test_source", "enabled": True,
           "interval_seconds": 1, "max_events": 3}
    try:
        async with _source_harness(cfg) as (store, bus, bg, sm, db_path):
            await wait_until(
                lambda: _src_state(sm, "test_source") in {"running", "completed"},
                timeout=15, description="S1 source running",
            )
            runner.assert_in(name + "-state", _src_state(sm, "test_source"),
                             {"running", "completed"})
            runner.assert_true(name + "-task-present", bg.active_count >= 1,
                               "no source background task found")
    except Exception as exc:
        runner.fail(name, str(exc))


async def S2(runner: R) -> None:
    """S2: disabled source -> no running source task, status initialized/absent."""
    name = "S2-source-disabled"
    cfg = {"type": "http_poller", "source_name": "http_poller", "enabled": False,
           "url": "https://example.com"}
    try:
        async with _source_harness(cfg) as (store, bus, bg, sm, db_path):
            runner.assert_eq(name + "-no-source-task", bg.active_count, 0)
            st = _src_state(sm, "http_poller")
            if st != "unknown":
                runner.assert_eq(name + "-state", st, "initialized")
    except Exception as exc:
        runner.fail(name, str(exc))


async def S3(runner: R) -> None:
    """S3: test_source max_events=1 -> exactly 1 event published."""
    name = "S3-exactly-one"
    cfg = {"type": "test_source", "source_name": "test_source", "enabled": True,
           "interval_seconds": 1, "max_events": 1, "persistent": False}
    try:
        async with _source_harness(cfg) as (store, bus, bg, sm, db_path):
            await wait_for_value(lambda: _db_seen_count(db_path, "test_source"), 1,
                                 timeout=15, description="S3 one published")
            runner.assert_eq(name + "-count", _db_seen_count(db_path, "test_source"), 1)
    except Exception as exc:
        runner.fail(name, str(exc))


async def S5(runner: R) -> None:
    """S5: persistent source event -> DB row + sequence + replayable."""
    name = "S5-persistent"
    cfg = {"type": "test_source", "source_name": "test_source", "enabled": True,
           "interval_seconds": 1, "max_events": 1, "initial_delay_seconds": 0,
           "persistent": True}
    src_name = cfg["source_name"]
    tmp = tempfile.mkdtemp(prefix="s5_")
    db_path = os.path.join(tmp, "events.db")
    try:
        # Manual harness wiring so the consumer is REGISTERED BEFORE the source
        # starts publishing. register_consumer() does not backfill existing
        # events, and consumer_event_state materialization happens at publish
        # time — so a consumer registered afterwards would never see the event.
        store = EventStore(db_path)
        bus = _StubBus()
        bg = BackgroundTaskManager()
        sm = build_source_manager({src_name: cfg})
        store.register_consumer("s5-consumer")
        await sm.initialize(bg, store, bus)
        await sm.start_all({src_name: cfg})
        try:
            await wait_for_value(lambda: store.count(), 1, timeout=15,
                                 description="S5 persisted")
            runner.assert_eq(name + "-db-count", store.count(), 1)
            pending = store.replay_events("s5-consumer", limit=20)
            rows = pending.get("events", [])
            runner.assert_true(name + "-has-seq",
                               bool(rows) and rows[0].get("sequence") is not None,
                               "persistent event missing sequence")
            runner.assert_true(name + "-replay", len(rows) >= 1,
                               "persistent event not replayable")
        finally:
            await sm.shutdown()
            await bg.shutdown_all(timeout=3)
    except Exception as exc:
        runner.fail(name, str(exc))
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


async def S7(runner: R) -> None:
    """S7: restart dedup — persistent events not republished after restart.

    Simulated by two SourceManager sessions pointing at the SAME SQLite file
    (the durable dedup that makes a process restart safe). No MCP server needed.
    """
    name = "S7-restart-dedup"
    cfg = {"type": "test_source", "source_name": "test_source", "enabled": True,
           "interval_seconds": 1, "max_events": 5, "initial_delay_seconds": 0,
           "persistent": True}
    tmp = tempfile.mkdtemp(prefix="s7_")
    db_path = os.path.join(tmp, "events.db")
    try:
        # Session 1 — publish 5 ticks (durably marked seen).
        store1 = EventStore(db_path)
        bus1 = _StubBus()
        bg1 = BackgroundTaskManager()
        sm1 = build_source_manager({"test_source": cfg})
        await sm1.initialize(bg1, store1, bus1)
        await sm1.start_all({"test_source": cfg})
        await wait_for_value(lambda: _db_persistent_count(db_path), 5, timeout=15,
                             description="S7 first 5")
        runner.assert_eq(name + "-first-5", _db_persistent_count(db_path), 5)
        await sm1.shutdown()
        await bg1.shutdown_all(timeout=3)

        # Session 2 (restart) — same DB, brand new manager. Dedup must skip all 5.
        store2 = EventStore(db_path)
        bus2 = _StubBus()
        bg2 = BackgroundTaskManager()
        sm2 = build_source_manager({"test_source": cfg})
        await sm2.initialize(bg2, store2, bus2)
        await sm2.start_all({"test_source": cfg})
        await asyncio.sleep(2.0)
        await sm2.shutdown()
        await bg2.shutdown_all(timeout=3)

        runner.assert_eq(name + "-after-restart", _db_persistent_count(db_path), 5)
    except Exception as exc:
        runner.fail(name, str(exc))
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


async def S8(runner: R) -> None:
    """S8: external failure — poller to dead port -> source degrades, task alive."""
    name = "S8-external-failure"
    cfg = {"type": "http_poller", "source_name": "http_poller", "enabled": True,
           "url": "http://127.0.0.1:1/dead", "interval_seconds": 1,
           "timeout_seconds": 2, "item_path": "", "id_path": "id",
           "event_type_prefix": "test.s8", "persistent": False,
           "dedup": {"enabled": False}}
    try:
        async with _source_harness(cfg) as (store, bus, bg, sm, db_path):
            await wait_until(
                lambda: _src_state(sm, "http_poller") in {"degraded", "failed", "error"},
                timeout=15, description="S8 source degrades",
            )
            src = sm.get_status().get("http_poller", {})
            runner.assert_in(name + "-state", src.get("state"),
                             {"degraded", "failed", "error"})
            runner.assert_true(name + "-recorded-error",
                               bool(src.get("last_error_summary")),
                               "source should record the external failure")
            runner.assert_eq(name + "-task-count", bg.active_count, 1)
    except Exception as exc:
        runner.fail(name, str(exc))


async def S9(runner: R) -> None:
    """S9: recovery — mock returns items -> events appear."""
    name = "S9-recovery"
    mock_items = [{"id": "s9-item", "title": "one"}]
    mock_srv, mock_port = start_mock(mock_items)
    try:
        cfg = {"type": "http_poller", "source_name": "http_poller", "enabled": True,
               "url": f"http://127.0.0.1:{mock_port}/api", "interval_seconds": 1,
               "timeout_seconds": 5, "item_path": "", "id_path": "id",
               "event_type_prefix": "test.s9", "persistent": False,
               "dedup": {"enabled": True, "max_items": 10000}}
        async with _source_harness(cfg) as (store, bus, bg, sm, db_path):
            await wait_for_value(lambda: _db_seen_count(db_path, "http_poller"), 1,
                                 timeout=15, description="S9 recovered")
            runner.assert_true(name + "-recovered",
                               _db_seen_count(db_path, "http_poller") >= 1)
    except Exception as exc:
        runner.fail(name, str(exc))
    finally:
        mock_srv.shutdown()


async def S10(runner: R) -> None:
    """S10: timeout — slow mock vs 1s timeout -> no leak, exactly 1 source task."""
    name = "S10-timeout"
    srv = HTTPServer(("127.0.0.1", 0), MockHandler)
    slow_port = srv.server_address[1]
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    orig_get = MockHandler.do_GET

    def slow_get(self):
        import time as _t
        _t.sleep(5)
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'[]')

    MockHandler.do_GET = slow_get
    try:
        cfg = {"type": "http_poller", "source_name": "http_poller", "enabled": True,
               "url": f"http://127.0.0.1:{slow_port}/api", "interval_seconds": 1,
               "timeout_seconds": 1, "item_path": "", "id_path": "id",
               "event_type_prefix": "test.s10", "persistent": False,
               "dedup": {"enabled": False}}
        async with _source_harness(cfg) as (store, bus, bg, sm, db_path):
            await wait_until(
                lambda: _src_state(sm, "http_poller") in {"running", "degraded"},
                timeout=15, description="S10 source up",
            )
            await asyncio.sleep(2.0)
            runner.assert_eq(name + "-task-count", bg.active_count, 1)
    except Exception as exc:
        runner.fail(name, str(exc))
    finally:
        MockHandler.do_GET = orig_get
        srv.shutdown()


async def S11(runner: R) -> None:
    """S11: cancellation — stop a long-running source cleanly."""
    name = "S11-cancellation"
    cfg = {"type": "test_source", "source_name": "test_source", "enabled": True,
           "interval_seconds": 0.2, "max_events": 100}
    try:
        async with _source_harness(cfg) as (store, bus, bg, sm, db_path):
            await wait_until(
                lambda: _src_state(sm, "test_source") in {"running", "completed"},
                timeout=15, description="S11 running",
            )
            await asyncio.sleep(0.5)
            await sm.stop_source("test_source")
            # stop_source() sets the source's stop event (so its run loop exits
            # and sets _state="stopped") but BackgroundTaskManager.cancel() only
            # removes the task from its dict -- it does NOT call task.cancel().
            # So wait for the *terminal state* rather than active_count==0
            # (which flips to 0 immediately and would race the loop's cleanup).
            await wait_for_value(
                lambda: _src_state(sm, "test_source")
                in {"stopped", "initialized", "completed"},
                True, timeout=5, description="S11 task stopped",
            )
            runner.assert_in(name + "-state", _src_state(sm, "test_source"),
                             {"stopped", "initialized", "completed"})
    except Exception as exc:
        runner.fail(name, str(exc))


async def S12(runner: R) -> None:
    """S12: publication failure — store rejects 2nd save -> source degrades gracefully.

    Uses an in-process store whose ``save`` fails after the first successful
    write, which exercises the exact production error path (PublishError ->
    source marks degraded, keeps running) deterministically without depending
    on OS file-permission quirks.
    """
    name = "S12-pub-failure"

    class _FailAfterFirstStore(EventStore):
        def __init__(self, db_path: str) -> None:
            super().__init__(db_path)
            self._saved = 0

        def save(self, *a, **k):  # type: ignore[override]
            self._saved += 1
            if self._saved > 1:
                raise RuntimeError("simulated persistence failure")
            return super().save(*a, **k)

    cfg = {"type": "test_source", "source_name": "test_source", "enabled": True,
           "interval_seconds": 0.2, "max_events": 50, "initial_delay_seconds": 0,
           "persistent": True}
    tmp = tempfile.mkdtemp(prefix="s12_")
    db_path = os.path.join(tmp, "events.db")
    try:
        store = _FailAfterFirstStore(db_path)
        bus = _StubBus()
        bg = BackgroundTaskManager()
        sm = build_source_manager({"test_source": cfg})
        await sm.initialize(bg, store, bus)
        await sm.start_all({"test_source": cfg})
        await wait_until(
            lambda: _src_state(sm, "test_source") in {"degraded", "failed"},
            timeout=15, description="S12 source degrades",
        )
        src = sm.get_status().get("test_source", {})
        runner.assert_in(name + "-state", src.get("state"), {"degraded", "failed"})
        runner.assert_true(name + "-recorded-error",
                           bool(src.get("last_error_summary")),
                           "source should record the publication failure")
        runner.assert_true(name + "-published-some", src.get("events_published", 0) >= 1,
                           "tick 1 should publish before the failure")
        await sm.shutdown()
        await bg.shutdown_all(timeout=3)
    except Exception as exc:
        runner.fail(name, str(exc))
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


async def S13(runner: R) -> None:
    """S13: malformed payload — mock returns malformed JSON -> 0 events, degrades."""
    name = "S13-malformed"
    mock_srv, mock_port = start_mock([])
    MockHandler._response_data = b'{not valid json'
    try:
        cfg = {"type": "http_poller", "source_name": "http_poller", "enabled": True,
               "url": f"http://127.0.0.1:{mock_port}/api", "interval_seconds": 1,
               "timeout_seconds": 5, "item_path": "", "id_path": "id",
               "event_type_prefix": "test.s13", "persistent": False,
               "dedup": {"enabled": False}}
        async with _source_harness(cfg) as (store, bus, bg, sm, db_path):
            await wait_until(
                lambda: _src_state(sm, "http_poller") in {"degraded", "failed", "error"},
                timeout=15, description="S13 degrades",
            )
            runner.assert_eq(name + "-no-seen", _db_seen_count(db_path, "http_poller"), 0)
            src = sm.get_status().get("http_poller", {})
            runner.assert_in(name + "-state", src.get("state"),
                             {"degraded", "failed", "error"})
    except Exception as exc:
        runner.fail(name, str(exc))
    finally:
        mock_srv.shutdown()


async def S14(runner: R) -> None:
    """S14: concurrent live + replay — persistent source, consumer captures all via replay."""
    name = "S14-concurrent"
    cfg = {"type": "test_source", "source_name": "test_source", "enabled": True,
           "interval_seconds": 0.5, "max_events": 20, "initial_delay_seconds": 0,
           "persistent": True}
    src_name = cfg["source_name"]
    tmp = tempfile.mkdtemp(prefix="s14_")
    db_path = os.path.join(tmp, "events.db")
    try:
        # Register the consumer BEFORE the source publishes (same materialization
        # caveat as S5) so replay can return all 20 persisted events.
        store = EventStore(db_path)
        bus = _StubBus()
        bg = BackgroundTaskManager()
        sm = build_source_manager({src_name: cfg})
        store.register_consumer("s14-consumer")
        await sm.initialize(bg, store, bus)
        await sm.start_all({src_name: cfg})
        try:
            await wait_for_value(lambda: store.count(), 20, timeout=30,
                                 description="S14 published 20")
            runner.assert_eq(name + "-published", store.count(), 20)
            pending = store.replay_events("s14-consumer", limit=100)
            runner.assert_eq(name + "-replay-all", len(pending.get("events", [])), 20)
        finally:
            await sm.shutdown()
            await bg.shutdown_all(timeout=3)
    except Exception as exc:
        runner.fail(name, str(exc))
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


async def S15(runner: R) -> None:
    """S15: full regression (MCP representative) — no sources, core flow end to end.

    This is the single test in this file that still exercises the real MCP
    server over HTTP, preserving coverage of the production wiring
    (SourceManager inside the server lifespan, tool/resource boundaries).
    """
    name = "S15-regression"
    proc = None
    try:
        proc = await start_server()
        system_ping = await call("system_ping")
        runner.assert_eq(name + "-system_ping", system_ping.get("status"), "ok")
        await call("consumer_register", {"consumer_id": "s15-consumer"})
        gen = await call("event_publish", {"event_type": "test.s15", "data": {"x": 1},
                                            "persistent": True})
        runner.assert_eq(name + "-generated", gen.get("status"), "published")
        data = await call("event_list", {"limit": 20})
        found = [e for e in data.get("events", []) if e.get("type") == "test.s15"]
        runner.assert_true(name + "-listed", len(found) >= 1)
        pending = await call("consumer_event_pending_list", {"consumer_id": "s15-consumer", "limit": 20})
        runner.assert_true(name + "-pending", len(pending.get("events", [])) >= 1)
    except Exception as exc:
        runner.fail(name, str(exc))
    finally:
        safe_teardown(stop_server, proc)
        safe_teardown(restore_environment)


async def R4(runner: R) -> None:
    """R4: a disabled source creates no running background task."""
    name = "R4-registry-disabled"
    cfg = {"type": "http_poller", "source_name": "http_poller", "enabled": False,
           "url": "https://x.com"}
    try:
        async with _source_harness(cfg) as (store, bus, bg, sm, db_path):
            runner.assert_eq(name + "-no-source-task", bg.active_count, 0)
            st = _src_state(sm, "http_poller")
            if st != "unknown":
                runner.assert_eq(name + "-state-initialized", st, "initialized")
    except Exception as exc:
        runner.fail(name, str(exc))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main() -> int:
    runner = R()

    tests = [
        S1, S2, S3, S5, S7, S8, S9, S10, S11, S12, S13, S14, S15, R4,
    ]

    for test_fn in tests:
        await test_fn(runner)

    success = runner.summary()
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
