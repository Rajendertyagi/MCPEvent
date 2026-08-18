#!/usr/bin/env python3
"""
Subscription and live-notification tests.

Extracted from test_phase8.py (N1, N2, N3, N4, S4).

Run:
    python test/test_subscriptions.py
"""

from __future__ import annotations

import asyncio
import os
import sys
import time

# Ensure project root is importable.
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_DIR = os.path.dirname(_SCRIPT_DIR)
for _p in (_PROJECT_DIR, _SCRIPT_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from helpers.lifecycle import (  # noqa: E402
    restore_environment,
    start_server,
    stop_server,
)
from helpers.mcp import (  # noqa: E402
    call,
    read_res,
    wait_source_ready,
    wait_for_event_count,
)
from helpers.runner import R  # noqa: E402
from helpers.mock_http import start_mock  # noqa: E402

from mcp.client.client import Client  # noqa: E402
from mcp.client.subscriptions import ResourceUpdated  # noqa: E402


EVENT_RESOURCE_URI = "mcp-event://events/latest"


async def _collect_resource_updates(total: float) -> list:
    """Connect a real MCP client, subscribe to mcp-event://events/latest, collect ResourceUpdated events."""
    from helpers.lifecycle import get_server_url
    updates = []
    async with Client(get_server_url()) as client:
        async with client.listen(resource_subscriptions=[EVENT_RESOURCE_URI]) as sub:
            end = time.monotonic() + total
            while time.monotonic() < end:
                remaining = end - time.monotonic()
                if remaining <= 0:
                    break
                try:
                    event = await asyncio.wait_for(sub.__anext__(), timeout=remaining)
                except (StopAsyncIteration, asyncio.TimeoutError):
                    break
                if isinstance(event, ResourceUpdated) and event.uri == EVENT_RESOURCE_URI:
                    updates.append(event)
    return updates


# ===================================================================
# Subscription / Notification Tests
# ===================================================================


async def n1_test_source_exactly_one_notification(runner: R) -> None:
    """N1: test_source (max_events=1, initial_delay=3) -> exactly 1 live notification."""
    name = "N1-direct-notify"
    proc = await start_server({
        "sources": {
            "test_source": {"type": "test_source", "enabled": True,
                            "interval_seconds": 3.0, "max_events": 1,
                            "initial_delay_seconds": 3, "persistent": False},
        },
    })
    try:
        await wait_source_ready("test_source", {"running", "completed"}, timeout=15)
        updates = await _collect_resource_updates(5)
        runner.assert_eq(name + "-exactly-1", len(updates), 1)
        if updates:
            runner.assert_eq(name + "-uri", updates[0].uri, EVENT_RESOURCE_URI)
    finally:
        stop_server(proc)


async def n2_http_poller_dedup_exactly_one_notification(runner: R) -> None:
    """N2: http_poller + mock (same item, dedup) -> exactly 1 live notification."""
    name = "N2-direct-notify-dedup"
    mock_items = [{"id": "n2-item", "title": "one"}]
    mock_srv, mock_port = start_mock(mock_items)
    proc = await start_server({
        "sources": {
            "http_poller": {
                "type": "http_poller", "enabled": True,
                "url": f"http://127.0.0.1:{mock_port}/api",
                "interval_seconds": 2, "timeout_seconds": 5,
                "initial_delay_seconds": 3,
                "item_path": "", "id_path": "id",
                "event_type_prefix": "test.n2", "persistent": False,
                "dedup": {"enabled": True, "max_items": 10000},
            },
        },
    })
    try:
        await wait_source_ready("http_poller", {"running", "degraded"}, timeout=15)
        updates = await _collect_resource_updates(5)
        runner.assert_eq(name + "-exactly-1", len(updates), 1)
    finally:
        stop_server(proc)
        mock_srv.shutdown()


async def n3_transient_not_delivered_via_replay(runner: R) -> None:
    """N3: transient source event is NOT delivered to a consumer via replay (live only)."""
    name = "N3-transient-no-replay"
    proc = await start_server({
        "sources": {
            "test_source": {"type": "test_source", "enabled": True,
                            "interval_seconds": 2.0, "max_events": 100,
                            "initial_delay_seconds": 3, "persistent": False},
        },
    })
    try:
        await wait_source_ready("test_source", {"running", "completed"}, timeout=15)
        updates = await _collect_resource_updates(5)
        runner.assert_true(name + "-got-live", len(updates) >= 1, "expected >=1 live notification")
        # register a consumer and check replay finds nothing for transient events
        await call("consumer_register", {"consumer_id": "n3-consumer"})
        pending = await call("consumer_event_pending_list", {"consumer_id": "n3-consumer", "limit": 50})
        runner.assert_eq(name + "-pending-empty", len(pending.get("events", [])), 0)
    finally:
        stop_server(proc)


async def n4_persistent_delivered_via_replay(runner: R) -> None:
    """N4: persistent source event IS delivered to a consumer via replay."""
    name = "N4-persistent-replay"
    proc = await start_server({
        "sources": {
            "test_source": {"type": "test_source", "enabled": True,
                            "interval_seconds": 2.0, "max_events": 100,
                            "initial_delay_seconds": 3, "persistent": True},
        },
    })
    try:
        await wait_source_ready("test_source", {"running", "completed"}, timeout=15)
        # Register the consumer BEFORE the first publish so materialization includes it.
        await call("consumer_register", {"consumer_id": "n4-consumer"})
        updates = await _collect_resource_updates(5)
        runner.assert_true(name + "-got-live", len(updates) >= 1, "expected >=1 live notification")
        pending = await call("consumer_event_pending_list", {"consumer_id": "n4-consumer", "limit": 50})
        runner.assert_true(name + "-pending-nonempty",
                           len(pending.get("events", [])) >= 1,
                           "persistent event should be replayable")
    finally:
        stop_server(proc)


async def s4_live_notification_reaches_client(runner: R) -> None:
    """S4: live notification reaches a connected client (subscriptions/listen)."""
    name = "S4-live-notify"
    proc = await start_server({
        "sources": {"test_source": {"type": "test_source", "enabled": True,
                                    "interval_seconds": 2, "max_events": 100,
                                    "initial_delay_seconds": 3}},
    })
    try:
        await wait_source_ready("test_source", {"running", "completed"}, timeout=15)
        updates = await _collect_resource_updates(5)
        runner.assert_true(name + "-got-update", len(updates) >= 1,
                           "no live ResourceUpdated received")
    finally:
        stop_server(proc)


# ===================================================================
# Main
# ===================================================================


async def main() -> bool:
    # NOTE: do NOT start a server here. Each test (n1..s4) starts and stops its
    # own server with a DISTINCT source config (n1 needs max_events=1 "exactly
    # one" while n3/n4/s4 need many events). Starting a server at file scope
    # would be an orphaned process: every test overwrites the module-global
    # _server_proc, so the file-scope server would leak and hold its port until
    # the interpreter exits (atexit only stops the final global handle).
    runner = R()
    try:
        print("  Subscription & Notification Tests")
        print("=" * 50)

        tests = [
            n1_test_source_exactly_one_notification,
            n2_http_poller_dedup_exactly_one_notification,
            n3_transient_not_delivered_via_replay,
            n4_persistent_delivered_via_replay,
            s4_live_notification_reaches_client,
        ]
        for fn in tests:
            try:
                await fn(runner)
            except Exception as exc:
                runner.fail(fn.__name__, str(exc))

    finally:
        restore_environment()
    success = runner.summary()
    sys.exit(0 if success else 1)
    return success


if __name__ == "__main__":
    asyncio.run(main())
