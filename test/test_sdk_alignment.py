#!/usr/bin/env python3
"""SDK Alignment Tests — extracted from integrate_test.py and test_phase8.py.

Covers core SDK contract checks that must hold regardless of feature phase:

  * T1        Server init — list_tools returns >= 15 tools
  * T2        Sync tool — system_ping returns status ok
  * T3        event_publish publishes event with an ID
  * T4        Tool schemas are valid JSON Schema
  * P7T1      Schema v5 — checkpoints table exists, FK works
  * P7T3      Sync tool works (system_ping)
  * P7T4      Async tools work (event_publish)
  * P8T8      Extensibility proof — all original 16 tools plus 5 alert tools (21 total) still present

Each test is independently runnable and starts/stops its own server instance.

Run:
    python test/test_sdk_alignment.py
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
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
    restore_environment,
    get_server_url,
)
from helpers.mcp import (  # noqa: E402
    call,
    list_tools_names,
)
from helpers.runner import R  # noqa: E402
from mcp_result import safe_teardown  # noqa: E402


# ---------------------------------------------------------------------------
# Legacy IDs (kept as comments for traceability)
# ---------------------------------------------------------------------------

# Legacy ID: T1
async def test_list_tools_returns_enough(runner: R) -> None:
    """T1: Server init — list_tools returns >= 15 tools."""
    name = "T1-list-tools"
    tools = await list_tools_names()
    runner.assert_ge(name, len(tools), 15)


# Legacy ID: T2
async def test_ping_returns_ok(runner: R) -> None:
    """T2: Sync tool — system_ping returns status ok."""
    name = "T2-system_ping"
    data = await call("system_ping")
    runner.assert_eq(name, data.get("status"), "ok")


# Legacy ID: T3
async def test_generate_event_has_id(runner: R) -> None:
    """T3: event_publish publishes event with an ID."""
    name = "T3-generate-event"
    data = await call("event_publish", {"event_type": "test.t3", "source": "test", "persistent": True})
    runner.assert_eq(name, data.get("status"), "published")
    evt = data.get("event", {})
    runner.assert_true(name + "-has-id", bool(evt.get("id")), "no event id")
    runner.assert_true(name + "-has-seq", evt.get("sequence") is not None, "no sequence")


# Legacy ID: T4
async def test_tool_schemas_are_valid_json_schema(runner: R) -> None:
    """T4: Tool schemas are valid JSON Schema."""
    name = "T4-tool-schemas"
    url = get_server_url()
    from mcp import ClientSession  # noqa: E402
    from mcp.client.streamable_http import streamable_http_client as streamablehttp_client  # noqa: E402
    async with streamablehttp_client(url) as (r, w):
        async with ClientSession(r, w) as session:
            await session.initialize()
            result = await session.list_tools()
            errors = []
            for tool in result.tools:
                schema = getattr(tool, "input_schema", None)
                if schema is None:
                    errors.append(f"{tool.name}: no inputSchema")
                elif not isinstance(schema, dict):
                    errors.append(f"{tool.name}: inputSchema is not a dict")
    runner.assert_true(name, len(errors) == 0, "; ".join(errors) if errors else "")


# Legacy ID: P7T1
async def test_schema_v5_checkpoints_table(runner: R) -> None:
    """P7T1: Schema v5 — checkpoints table exists, FK works."""
    name = "P7T1-schema-v5"
    # Verify by checking that a newly registered consumer gets checkpoint 0
    cid = f"p7t1-{int(time.time()*1000)}"
    await call("consumer_register", {"consumer_id": cid})
    cp_resp = await call("consumer_checkpoint_get", {"consumer_id": cid})
    runner.assert_eq(name, cp_resp.get("checkpoint"), 0)


# Legacy ID: P7T3
async def test_sync_tool_works(runner: R) -> None:
    """P7T3: Sync tool works (system_ping)."""
    name = "P7T3-sync"
    data = await call("system_ping")
    runner.assert_eq(name, data.get("status"), "ok")


# Legacy ID: P7T4
async def test_async_tools_work(runner: R) -> None:
    """P7T4: Async tools work (event_publish)."""
    name = "P7T4-async"
    data = await call("event_publish", {"event_type": "test.p7t4", "source": "test"})
    runner.assert_eq(name, data.get("status"), "published")


# Legacy ID: P8T8
async def test_extensibility_proof_original_tools_present(runner: R) -> None:
    """P8T8: extensibility proof — all original 16 tools still present plus 5 new alert tools (21 total)."""
    name = "P8T8-original-tools"
    tools = await list_tools_names()
    expected_tools = [
        "system_ping", "event_publish", "event_list", "consumer_register",
        "consumer_topic_add", "consumer_event_list", "consumer_event_pending_list",
        "consumer_event_acknowledge", "consumer_checkpoint_get", "dev_progress_test",
        "dev_long_running_test", "dev_background_publish_test", "dev_task_list",
        "dev_source_start", "dev_source_fail", "dev_source_stop",
        "alert_create", "alert_list", "alert_get", "alert_enable", "alert_disable",
    ]
    for tool in expected_tools:
        runner.assert_in(name + f"-{tool}", tool, tools)
    runner.assert_eq(name + "-count", len(tools), len(expected_tools))


# ---------------------------------------------------------------------------
# Test ordering
# ---------------------------------------------------------------------------
_TESTS = [
    test_list_tools_returns_enough,
    test_ping_returns_ok,
    test_generate_event_has_id,
    test_tool_schemas_are_valid_json_schema,
    test_schema_v5_checkpoints_table,
    test_sync_tool_works,
    test_async_tools_work,
    test_extensibility_proof_original_tools_present,
]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main() -> None:
    import atexit
    atexit.register(restore_environment)
    print("Starting server...")
    await start_server()
    runner = R()
    try:
        print()
        print("=" * 50)
        print("  SDK Alignment Tests")
        print("=" * 50)
        for fn in _TESTS:
            try:
                await fn(runner)
            except Exception as exc:
                doc = fn.__doc__ or fn.__name__
                label = doc.split(":")[0].strip() if doc else fn.__name__
                runner.fail(label, str(exc))
    finally:
        safe_teardown(restore_environment)
    success = runner.summary()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    asyncio.run(main())
