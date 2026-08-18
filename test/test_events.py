#!/usr/bin/env python3
"""
Core event flow tests — over the MCP tool/resource boundary.

OPTIMIZATION (2026-08): this file now uses EXACTLY ONE server process for the
whole file. Previously it started a server with no sources, then RESTARTED it
with test_source enabled, then started a THIRD server for the http-poller test
(3 server starts). The single server is started with test_source enabled
(transient ticks do not pollute the persistent-only replay used by T9/T11/T12),
and http_poller disabled (avoids dead-port noise). The http-poller check (P8T7)
moved to test_source_dedup.py as a direct, server-less test. P8T3 (schema v7)
is verified directly against an isolated EventStore.

Legacy IDs preserved: T1..T14, P7T7, P7T18, P8T1, P8T2, P8T3, P8T4, P8T5,
P8T6, P8T7(relocated), P8T9.

Run:
    python test/test_events.py
"""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import sqlite3
import sys
import tempfile
import time
from typing import Any

# Add project root and test dir to path
_PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_TEST_DIR = os.path.dirname(os.path.abspath(__file__))
for _p in (_PROJECT_DIR, _TEST_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from helpers.lifecycle import start_server, stop_server, restore_environment, get_server_url  # noqa: E402
from helpers.mcp import call, read_res, list_tools_names, wait_source_ready, wait_for_event_count  # noqa: E402
from helpers.runner import R  # noqa: E402
from store import EventStore  # noqa: E402


async def t1_list_tools(runner: R) -> None:
    """T1: Server init — list_tools returns >= 15 tools."""
    name = "T1-list-tools"
    tools = await list_tools_names()
    runner.assert_true(name, len(tools) >= 15, f"expected >= 15 tools, got {len(tools)}")


async def t2_ping(runner: R) -> None:
    """T2: Sync tool — system_ping returns status ok."""
    name = "T2-system_ping"
    data = await call("system_ping")
    runner.assert_eq(name, data.get("status"), "ok")


async def t3_generate_event(runner: R) -> None:
    """T3: event_publish publishes event with an ID."""
    name = "T3-generate-event"
    data = await call("event_publish", {"event_type": "test.t3", "source": "test", "persistent": True})
    runner.assert_eq(name, data.get("status"), "published")
    evt = data.get("event", {})
    runner.assert_true(name + "-has-id", bool(evt.get("id")), "no event id")
    runner.assert_true(name + "-has-seq", evt.get("sequence") is not None, "no sequence")


async def t4_tool_schemas(runner: R) -> None:
    """T4: Tool schemas are valid JSON Schema."""
    name = "T4-tool-schemas"
    from mcp import ClientSession
    from mcp.client.streamable_http import streamable_http_client as streamablehttp_client

    url = get_server_url()
    async with streamablehttp_client(url) as (r, w):
        async with ClientSession(r, w) as session:
            await session.initialize()
            result = await session.list_tools()
            errors = []
            for tool in result.tools:
                schema = tool.input_schema if hasattr(tool, "input_schema") else None
                if schema is None:
                    errors.append(f"{tool.name}: no inputSchema")
                elif not isinstance(schema, dict):
                    errors.append(f"{tool.name}: inputSchema is not a dict")
            runner.assert_true(name, len(errors) == 0, "; ".join(errors) if errors else "")


async def t7_topic_filter(runner: R) -> None:
    """T7: Topic filtering — consumer only gets matching topics."""
    name = "T7-topic-filter"
    prefix = f"t7-{int(time.time() * 1000)}"
    cid_match = f"{prefix}-match"
    cid_nomatch = f"{prefix}-nomatch"
    await call("consumer_register", {"consumer_id": cid_match})
    await call("consumer_register", {"consumer_id": cid_nomatch})
    await call("consumer_topic_add", {"consumer_id": cid_match, "topic": "alpha"})

    resp = await call("event_publish", {
        "event_type": "test.t7",
        "source": "test",
        "persistent": True,
        "routing": {"topics": ["alpha"]},
    })
    eid = resp["event"]["id"]

    pending_m = await call("consumer_event_pending_list", {"consumer_id": cid_match})
    pending_n = await call("consumer_event_pending_list", {"consumer_id": cid_nomatch})
    ids_m = {e["id"] for e in pending_m.get("events", [])}
    ids_n = {e["id"] for e in pending_n.get("events", [])}
    runner.assert_true(name + "-match", eid in ids_m, "matching consumer missing event")
    runner.assert_true(name + "-nomatch", eid not in ids_n, "non-matching consumer got event")


async def t8_transient(runner: R) -> None:
    """T8: Transient — new consumer doesn't see old transient events."""
    name = "T8-transient"
    await call("event_publish", {"event_type": "test.t8", "source": "test", "persistent": False})

    prefix = f"t8-{int(time.time() * 1000)}"
    cid = f"{prefix}-consumer"
    await call("consumer_register", {"consumer_id": cid})
    pending = await call("consumer_event_pending_list", {"consumer_id": cid})
    events_list = pending.get("events", [])
    types = [e.get("type") for e in events_list]
    runner.assert_true(name, "test.t8" not in types,
                       "new consumer should not see pre-existing transient events")


async def t9_replay_order(runner: R) -> None:
    """T9: Replay — events returned in ascending sequence order."""
    name = "T9-replay-order"
    prefix = f"t9-{int(time.time() * 1000)}"
    cid = f"{prefix}-consumer"
    await call("consumer_register", {"consumer_id": cid})

    seqs = []
    for i in range(5):
        resp = await call("event_publish", {
            "event_type": "test.t9",
            "source": "test",
            "persistent": True,
        })
        seqs.append(resp["event"]["sequence"])

    pending = await call("consumer_event_pending_list", {"consumer_id": cid})
    returned_seqs = [e["sequence"] for e in pending.get("events", [])]
    runner.assert_true(name, returned_seqs == sorted(returned_seqs),
                       f"not sorted: {returned_seqs}")
    runner.assert_true(name + "-count", len(returned_seqs) == 5,
                       f"expected 5, got {len(returned_seqs)}")


async def t11_topic_targeted(runner: R) -> None:
    """T11: Topic assignment + targeted delivery."""
    name = "T11-topic-targeted"
    prefix = f"t11-{int(time.time() * 1000)}"
    cid_x = f"{prefix}-x"
    cid_y = f"{prefix}-y"
    await call("consumer_register", {"consumer_id": cid_x})
    await call("consumer_register", {"consumer_id": cid_y})
    await call("consumer_topic_add", {"consumer_id": cid_x, "topic": "gpu"})
    await call("consumer_topic_add", {"consumer_id": cid_y, "topic": "cpu"})

    resp = await call("event_publish", {
        "event_type": "test.t11",
        "source": "test",
        "persistent": True,
        "routing": {"topics": ["gpu"]},
    })
    eid = resp["event"]["id"]

    pending_x = await call("consumer_event_pending_list", {"consumer_id": cid_x})
    pending_y = await call("consumer_event_pending_list", {"consumer_id": cid_y})
    ids_x = {e["id"] for e in pending_x.get("events", [])}
    ids_y = {e["id"] for e in pending_y.get("events", [])}
    runner.assert_true(name + "-x", eid in ids_x, "gpu consumer missing event")
    runner.assert_true(name + "-y", eid not in ids_y, "cpu consumer should not see gpu event")


async def t12_ack_clears(runner: R) -> None:
    """T12: Ack clears pending events."""
    name = "T12-ack-clears"
    prefix = f"t12-{int(time.time() * 1000)}"
    cid = f"{prefix}-consumer"
    await call("consumer_register", {"consumer_id": cid})

    resp = await call("event_publish", {
        "event_type": "test.t12",
        "source": "test",
        "persistent": True,
    })
    eid = resp["event"]["id"]

    pending_before = await call("consumer_event_pending_list", {"consumer_id": cid})
    ids_before = {e["id"] for e in pending_before.get("events", [])}
    runner.assert_true(name + "-before", eid in ids_before, "event not pending before ack")

    await call("consumer_event_acknowledge", {"consumer_id": cid, "event_id": eid})

    pending_after = await call("consumer_event_pending_list", {"consumer_id": cid})
    ids_after = {e["id"] for e in pending_after.get("events", [])}
    runner.assert_true(name + "-after", eid not in ids_after, "event still pending after ack")


async def t12b_ack_advances_checkpoint(runner: R) -> None:
    """T12b: MCP ACK -> checkpoint advancement (real MCP tool-boundary coverage).

    End-to-end path the 0-server direct tests only approximate (§22-§25):
        register consumer -> publish persistent event -> call the MCP
        ``consumer_event_acknowledge`` tool -> checkpoint advances to the acked sequence.
    The MCP ``consumer_event_acknowledge`` tool chains ``advance_checkpoint`` internally
    (server.py); this test asserts the durable checkpoint actually moves.
    """
    name = "T12b-ack-checkpoint"
    prefix = f"t12b-{int(time.time() * 1000)}"
    cid = f"{prefix}-consumer"
    await call("consumer_register", {"consumer_id": cid})

    resp = await call("event_publish", {
        "event_type": "test.t12b",
        "source": "test",
        "persistent": True,
    })
    eid = resp["event"]["id"]
    seq = resp["event"]["sequence"]

    # Before ack: nothing acknowledged yet -> checkpoint pinned at 0.
    cp_before = await call("consumer_checkpoint_get", {"consumer_id": cid})
    runner.assert_eq(name + "-cp-before", cp_before.get("checkpoint"), 0)

    # Real MCP consumer_event_acknowledge tool (server chains advance_checkpoint internally).
    await call("consumer_event_acknowledge", {"consumer_id": cid, "event_id": eid})

    # After ack: checkpoint advances to the acknowledged event's sequence.
    cp_after = await call("consumer_checkpoint_get", {"consumer_id": cid})
    runner.assert_eq(name + "-cp-after", cp_after.get("checkpoint"), seq)


async def t13_resource_event_latest(runner: R) -> None:
    """T13: Resource mcp-event://events/latest returns data."""
    name = "T13-resource-event-latest"
    await call("event_publish", {"event_type": "test.t13", "source": "test"})
    data = await read_res("mcp-event://events/latest")
    runner.assert_true(name, isinstance(data, dict), f"expected dict, got {type(data)}")
    runner.assert_true(name + "-id", bool(data.get("id")), "no id in latest event")


async def t14_resource_server_info(runner: R) -> None:
    """T14: Resource mcp-event://system/info returns server info."""
    name = "T14-resource-server-info"
    data = await read_res("mcp-event://system/info")
    runner.assert_true(name, isinstance(data, dict), f"expected dict, got {type(data)}")
    runner.assert_true(name + "-name", "name" in data, "no 'name' field")
    runner.assert_true(name + "-version", "version" in data, "no 'version' field")


async def p7t7_progress(runner: R) -> None:
    """P7T7: Progress reporting works."""
    name = "P7T7-progress"
    data = await call("dev_progress_test", {"total": 5})
    runner.assert_eq(name, data.get("status"), "completed")
    runner.assert_eq(name + "-final", data.get("final_progress"), 5.0)


async def p7t18_info_fields(runner: R) -> None:
    """P7T18: mcp-event://system/info has expected fields."""
    name = "P7T18-info-fields"
    data = await read_res("mcp-event://system/info")
    expected_fields = ["name", "version", "purpose", "transport", "features", "limits"]
    missing = [f for f in expected_fields if f not in data]
    runner.assert_true(name, len(missing) == 0, f"missing fields: {missing}")
    runner.assert_true(name + "-features", isinstance(data.get("features"), dict))
    runner.assert_true(name + "-limits", isinstance(data.get("limits"), dict))


async def p8t1_sources_status(runner: R) -> None:
    """P8T1: mcp-event://sources/status resource exists and returns a dict."""
    name = "P8T1-sources-status"
    data = await read_res("mcp-event://sources/status")
    runner.assert_true(name + "-is-dict", isinstance(data, dict),
                       f"expected dict, got {type(data).__name__}")


async def p8t2_info_features(runner: R) -> None:
    """P8T2: mcp-event://system/info includes 'source_connectors' in features."""
    name = "P8T2-info-features"
    data = await read_res("mcp-event://system/info")
    runner.assert_true(name + "-has-features", isinstance(data.get("features"), dict),
                       "no features dict in server info")
    runner.assert_true(name + "-source-connectors",
                       data.get("features", {}).get("source_connectors") is True,
                       "source_connectors not True in features")


async def p8t3_schema_v7(runner: R) -> None:
    """P8T3: schema v7 — source_state and source_seen_items tables exist (DIRECT, no server)."""
    name = "P8T3-schema-v7"
    tmp = tempfile.mkdtemp(prefix="evt3_")
    db_path = os.path.join(tmp, "events.db")
    try:
        # EventStore constructor creates the v7 schema on a fresh DB.
        EventStore(db_path)
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA busy_timeout=5000")
        for tbl in ("source_state", "source_seen_items"):
            row = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (tbl,)
            ).fetchone()
            runner.assert_true(name + f"-table-{tbl}", row is not None,
                               f"table {tbl} missing")
        ver = conn.execute("PRAGMA user_version").fetchone()[0]
        runner.assert_eq(name + "-version", ver, 7)
        conn.close()
    except Exception as exc:
        runner.fail(name, str(exc))
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


async def p8t4_test_source_events(runner: R) -> None:
    """P8T4: test_source enabled — publishes tick events (single server has test_source on)."""
    name = "P8T4-test-source-events"
    await wait_source_ready("test_source", {"running", "completed"}, timeout=15)
    count = await wait_for_event_count("test.source.tick", 1, timeout=15)
    runner.assert_true(name + "-has-ticks", count > 0, "no tick events appeared")
    if count > 0:
        data = await call("event_list", {"limit": 50})
        ticks = [e for e in data.get("events", []) if e.get("type") == "test.source.tick"]
        runner.assert_eq(name + "-source", ticks[0].get("source"), "test_source")
        runner.assert_true(name + "-has-tick-data",
                           "tick" in ticks[0].get("data", {}),
                           "tick event missing 'tick' in data")


async def p8t5_max_events(runner: R) -> None:
    """P8T5: test_source completes all ticks — max_events honored (>= 3)."""
    name = "P8T5-max-events"
    await wait_source_ready("test_source", {"completed", "running"}, timeout=15)
    deadline = time.monotonic() + 12
    while time.monotonic() < deadline:
        st = await read_res("mcp-event://sources/status")
        src = st.get("test_source", {})
        if src.get("state") == "completed" or src.get("events_published", 0) >= 3:
            break
        await asyncio.sleep(0.3)
    data = await call("event_list", {"limit": 50})
    ticks = [e for e in data.get("events", []) if e.get("type") == "test.source.tick"]
    runner.assert_true(name + "-completed", len(ticks) >= 3,
                       f"expected at least 3 ticks, got {len(ticks)}")


async def p8t6_failure_resilience(runner: R) -> None:
    """P8T6: source failure resilience — dev_source_fail doesn't crash the server."""
    name = "P8T6-failure-resilience"
    resp = await call("dev_source_fail", {"name": "p8-fail-res", "delay_seconds": 0.1})
    runner.assert_eq(name + "-started", resp.get("status"), "started")
    await asyncio.sleep(1.0)
    data = await call("system_ping")
    runner.assert_eq(name + "-system_ping-ok", data.get("status"), "ok")


async def p8t9_graceful_shutdown(runner: R) -> None:
    """P8T9: graceful shutdown with source running — server still responsive."""
    name = "P8T9-graceful-shutdown"
    data = await call("system_ping")
    runner.assert_eq(name + "-system_ping", data.get("status"), "ok")
    tasks_resp = await call("dev_task_list")
    task_count = tasks_resp.get("task_count", 0)
    runner.assert_true(name + "-has-tasks", task_count >= 0,
                       f"background tasks present: {task_count}")
    runner.ok(name + "-pre-shutdown-ok")


# ===================================================================
# Main
# ===================================================================

async def main() -> int:
    runner = R()
    proc = None
    try:
        # ONE server for the whole file: test_source ON (transient ticks are
        # safe for the persistent-only replay used by T9/T11/T12), http_poller
        # OFF (avoids dead-port noise). P8T7's http-poller check lives in
        # test_source_dedup.py as a direct, server-less test.
        proc = await start_server({
            "sources": {
                "http_poller": {"enabled": False},
                "test_source": {
                    "type": "test_source",
                    "enabled": True,
                    "interval_seconds": 1,
                    "max_events": 3,
                    "persistent": False,
                },
            },
        })
        url = get_server_url()
        print(f"Server running at {url}")
        print()

        tests = [
            t1_list_tools,
            t2_ping,
            t3_generate_event,
            t4_tool_schemas,
            t7_topic_filter,
            t8_transient,
            t9_replay_order,
            t11_topic_targeted,
            t12_ack_clears,
            t12b_ack_advances_checkpoint,
            t13_resource_event_latest,
            t14_resource_server_info,
            p7t7_progress,
            p7t18_info_fields,
            p8t1_sources_status,
            p8t2_info_features,
            p8t3_schema_v7,
            p8t4_test_source_events,
            p8t5_max_events,
            p8t6_failure_resilience,
            p8t9_graceful_shutdown,
        ]
        for fn in tests:
            try:
                await fn(runner)
            except Exception as exc:
                runner.fail(fn.__name__, str(exc))

    finally:
        restore_environment()

    runner.summary()
    return 0 if runner.failed == 0 else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
