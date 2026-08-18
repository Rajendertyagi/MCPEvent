#!/usr/bin/env python3
"""
Performance sanity tests.

Extracted from integrate_test.py (PERF1, PERF2, PERF3).

Run:
    python test/test_performance.py
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
)
from helpers.mcp import (  # noqa: E402
    call,
    read_res,
)
from helpers.runner import R  # noqa: E402
from mcp import ClientSession  # noqa: E402
from mcp.client.streamable_http import streamable_http_client as streamablehttp_client  # noqa: E402
from helpers.lifecycle import get_server_url  # noqa: E402


# ===================================================================
# Performance Tests
# ===================================================================


async def perf1_publish_storm(runner: R) -> None:
    """PERF1: 50-event publish storm."""
    name = "PERF1-publish-storm"
    start = time.monotonic()
    for i in range(50):
        resp = await call("event_publish", {"event_type": f"test.perf{i}", "source": "test"})
    elapsed = time.monotonic() - start
    runner.assert_true(name, elapsed < 30.0, f"50 events took {elapsed:.2f}s (>30s)")


async def perf2_resource_during_load(runner: R) -> None:
    """PERF2: Read resource during load."""
    name = "PERF2-resource-during-load"
    # Publish some events in background
    async def _publish():
        for i in range(10):
            await call("event_publish", {"event_type": f"test.perf2bg{i}", "source": "test"})
    task = asyncio.create_task(_publish())
    # Read resource concurrently
    data = await read_res("mcp-event://system/info")
    await task
    runner.assert_true(name, isinstance(data, dict), "resource read failed during load")


async def perf3_concurrent_tool_calls(runner: R) -> None:
    """PERF3: 5 concurrent tool calls."""
    name = "PERF3-concurrent-calls"

    async def _ping():
        url = get_server_url()
        async with streamablehttp_client(url) as (r, w):
            async with ClientSession(r, w) as session:
                await session.initialize()
                return await session.call_tool("system_ping", {})

    results = await asyncio.gather(*[_ping() for _ in range(5)])
    all_ok = True
    for res in results:
        ok = any(hasattr(b, "text") and "ok" in b.text for b in res.content)
        if not ok:
            all_ok = False
            break
    runner.assert_true(name, all_ok, "not all concurrent calls succeeded")


# ===================================================================
# Main
# ===================================================================


async def main() -> bool:
    proc = await start_server()
    runner = R()
    try:
        print("  Performance Sanity Tests")
        print("=" * 50)

        tests = [
            perf1_publish_storm,
            perf2_resource_during_load,
            perf3_concurrent_tool_calls,
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
