#!/usr/bin/env python3
"""
Background task lifecycle tests — DIRECT (no MCP server).

OPTIMIZATION (2026-08): previously every test drove the MCP server via the
[dev_source_start / dev_source_stop / dev_task_list / dev_source_fail]
TESTING tools. The background-task lifecycle lives entirely in
``runtime.BackgroundTaskManager`` and the source wiring in ``sources.SourceManager``,
so these tests now run DIRECTLY against those application objects with an isolated
EventStore + injectable stub bus — ZERO server subprocesses, sub-second runtime.
Tool-boundary coverage for these paths is preserved by the D-level files
(e.g. test_sdk_alignment / test_events).

Legacy IDs preserved in comments: P7T10, P7T11, P7T12, P7T20, P7T21.

Run:
    python test/test_background_tasks.py
"""

from __future__ import annotations

import asyncio
import os
import shutil
import sys
import tempfile

# Ensure project root is importable.
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_DIR = os.path.dirname(_SCRIPT_DIR)
for _p in (_PROJECT_DIR, _SCRIPT_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import events  # noqa: E402
from store import EventStore  # noqa: E402
from runtime import BackgroundTaskManager  # noqa: E402
from sources import build_source_manager  # noqa: E402

from helpers.runner import R  # noqa: E402
from helpers.wait import wait_for_value  # noqa: E402


class _StubBus:
    """Minimal subscription bus: records the last notification, does nothing else."""

    def __init__(self) -> None:
        self.last = None

    async def publish(self, item: object) -> None:
        self.last = item


def _bg_status(bg, task_name: str) -> str | None:
    """Return the inner 'status' string for a named background task.

    ``BackgroundTaskManager.status(name)`` returns a dict keyed by task name,
    e.g. ``{"name": {"status": "running", ...}}`` — so index by name first.
    """
    return bg.status(task_name).get(task_name, {}).get("status")


async def _publish_persistent(bus, store, event_type: str, source: str = "background") -> dict:
    """Publish one persistent event through the canonical path."""
    return await events.publish_event(
        event_type=event_type,
        source=source,
        data={},
        persistent=True,
        store=store,
        bus=bus,
    )


# ===================================================================
# Background Task Tests (legacy P7T10..P7T21)
# ===================================================================


async def p7t10_background_task_lifecycle(runner: R) -> None:
    """P7T10: Background task lifecycle (start, status, stop) via BackgroundTaskManager."""
    name = "P7T10-bg-lifecycle"
    bg = BackgroundTaskManager()
    stop = asyncio.Event()

    async def _loop() -> None:
        await stop.wait()

    await bg.start("p7t10-src", _loop())
    try:
        runner.assert_true(name + "-active", bg.active_count >= 1, "no task running")
        runner.assert_eq(name + "-status", _bg_status(bg, "p7t10-src"), "running")
    finally:
        await bg.cancel("p7t10-src")
        await bg.shutdown_all(timeout=3)
    runner.ok(name + "-stopped")


async def p7t11_background_persistent_publish(runner: R) -> None:
    """P7T11: Background persistent publish (mirrors the server's dev_source_start)."""
    name = "P7T11-bg-persist"
    tmp = tempfile.mkdtemp(prefix="bg11_")
    db_path = os.path.join(tmp, "events.db")
    store = EventStore(db_path)
    bus = _StubBus()
    bg = BackgroundTaskManager()

    async def _source_loop() -> None:
        await asyncio.sleep(0.3)
        await _publish_persistent(bus, store, "test.p7t11", source="background:p7t11")

    await bg.start("p7t11-src", _source_loop())
    try:
        await wait_for_value(lambda: store.count(), 1, timeout=15,
                             description="P7T11 published")
        runner.assert_eq(name + "-count", store.count(), 1)
    finally:
        await bg.shutdown_all(timeout=3)
        shutil.rmtree(tmp, ignore_errors=True)


async def p7t12_background_failure_survives(runner: R) -> None:
    """P7T12: A failing background task does NOT kill the manager / other tasks."""
    name = "P7T12-bg-fail"
    bg = BackgroundTaskManager()
    stop = asyncio.Event()

    async def _healthy() -> None:
        await stop.wait()

    async def _fail() -> None:
        await asyncio.sleep(0.2)
        raise RuntimeError("intentional failure in source 'p7t12-fail'")

    await bg.start("p7t12-healthy", _healthy())
    await bg.start("p7t12-fail", _fail())
    try:
        await asyncio.sleep(1.0)  # let the failing task raise
        runner.assert_eq(name + "-healthy-alive", _bg_status(bg, "p7t12-healthy"), "running")
        runner.assert_true(name + "-manager-alive", bg.active_count >= 1,
                          "manager lost tasks after a failure")
        fail_status = bg.status("p7t12-fail").get("p7t12-fail", {})
        runner.assert_true(
            name + "-fail-recorded",
            fail_status.get("status") == "done" and fail_status.get("exception") is not None,
            "failing task exception was not recorded",
        )
    finally:
        stop.set()
        await bg.shutdown_all(timeout=3)


async def p7t20_extension_seam(runner: R) -> None:
    """P7T20: Extension seam — a 2nd source type starts via SourceManager without touching internals."""
    name = "P7T20-extension-seam"
    tmp = tempfile.mkdtemp(prefix="bg20_")
    db_path = os.path.join(tmp, "events.db")
    store = EventStore(db_path)
    bus = _StubBus()
    bg = BackgroundTaskManager()
    sm = build_source_manager({"test_source": {
        "type": "test_source",
        "source_name": "test_source",
        "enabled": True,
        "interval_seconds": 0.2,
        "max_events": 3,
        "initial_delay_seconds": 0,
        "persistent": False,
    }})
    await sm.initialize(bg, store, bus)
    await sm.start_all({"test_source": {"type": "test_source", "enabled": True}})
    try:
        await wait_for_value(lambda: bg.active_count, 1, timeout=15,
                             description="P7T20 source started")
        runner.assert_eq(name + "-started", bg.active_count, 1)
    finally:
        await sm.shutdown()
        await bg.shutdown_all(timeout=3)
        shutil.rmtree(tmp, ignore_errors=True)


async def p7t21_background_task_status(runner: R) -> None:
    """P7T21: Background task status reflects running, then gone after cancel."""
    name = "P7T21-bg-status"
    bg = BackgroundTaskManager()
    stop = asyncio.Event()

    async def _loop() -> None:
        await stop.wait()

    await bg.start("p7t21-src", _loop())
    try:
        runner.assert_eq(name + "-running", _bg_status(bg, "p7t21-src"), "running")
    finally:
        await bg.cancel("p7t21-src")
        await bg.shutdown_all(timeout=3)
    runner.ok(name + "-cancelled")


# ===================================================================
# Main
# ===================================================================


async def main() -> int:
    runner = R()
    print("  Background Task Tests (direct, no server)")
    print("=" * 50)

    tests = [
        p7t10_background_task_lifecycle,
        p7t11_background_persistent_publish,
        p7t12_background_failure_survives,
        p7t20_extension_seam,
        p7t21_background_task_status,
    ]
    for fn in tests:
        try:
            await fn(runner)
        except Exception as exc:
            runner.fail(fn.__name__, str(exc))

    success = runner.summary()
    sys.exit(0 if success else 1)
    return success


if __name__ == "__main__":
    asyncio.run(main())
