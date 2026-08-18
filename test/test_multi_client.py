#!/usr/bin/env python3
"""Multi-Client Tests — extracted from integrate_test.py (P7T8).

Covers concurrent client sessions making independent calls against a single
server instance, verifying the server handles multiple simultaneous MCP
connections without data corruption or session leakage.

  * P7T8  Concurrent clients (two sessions via asyncio.gather)

Run independently:
    python test/test_multi_client.py
"""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
import time
from typing import Any

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
from helpers.runner import R  # noqa: E402
from mcp_result import safe_teardown  # noqa: E402


# ---------------------------------------------------------------------------
# Legacy IDs (kept as comments for traceability)
# ---------------------------------------------------------------------------

# Legacy ID: P7T8
async def test_concurrent_clients_gather(runner: R) -> None:
    """P7T8: Concurrent clients (two sessions via asyncio.gather)."""
    name = "P7T8-concurrent"
    url = get_server_url()

    from mcp import ClientSession  # noqa: E402
    from mcp.client.streamable_http import streamable_http_client as streamablehttp_client  # noqa: E402

    async def _ping() -> Any:
        async with streamablehttp_client(url) as (r, w):
            async with ClientSession(r, w) as session:
                await session.initialize()
                return await session.call_tool("system_ping", {})

    r1, r2 = await asyncio.gather(_ping(), _ping())
    ok1 = any(hasattr(b, "text") and "ok" in b.text for b in r1.content)
    ok2 = any(hasattr(b, "text") and "ok" in b.text for b in r2.content)
    runner.assert_true(name, ok1 and ok2, "both concurrent sessions should succeed")


# ---------------------------------------------------------------------------
# Extended multi-client scenarios
# ---------------------------------------------------------------------------

async def test_concurrent_generate_and_ping(runner: R) -> None:
    """P7T8-ext: Two clients issue different tools concurrently."""
    name = "P7T8-ext-different-tools"
    url = get_server_url()

    from mcp import ClientSession  # noqa: E402
    from mcp.client.streamable_http import streamable_http_client as streamablehttp_client  # noqa: E402

    async def _ping_client() -> dict:
        async with streamablehttp_client(url) as (r, w):
            async with ClientSession(r, w) as session:
                await session.initialize()
                result = await session.call_tool("system_ping", {})
                from mcp_result import to_payload  # noqa: E402
                return to_payload(result)

    async def _event_client() -> dict:
        suffix = int(time.time() * 1000)
        async with streamablehttp_client(url) as (r, w):
            async with ClientSession(r, w) as session:
                await session.initialize()
                result = await session.call_tool(
                    "event_publish",
                    {"event_type": f"test.multiclient.{suffix}", "source": "test"},
                )
                from mcp_result import to_payload  # noqa: E402
                return to_payload(result)

    ping_result, event_result = await asyncio.gather(_ping_client(), _event_client())
    runner.assert_eq(name + "-system_ping", ping_result.get("status"), "ok")
    runner.assert_eq(name + "-event", event_result.get("status"), "published")


async def test_three_concurrent_sessions(runner: R) -> None:
    """P7T8-ext3: Three independent clients each register a consumer and publish."""
    name = "P7T8-ext3-three-sessions"
    url = get_server_url()

    from mcp import ClientSession  # noqa: E402
    from mcp.client.streamable_http import streamable_http_client as streamablehttp_client  # noqa: E402
    from mcp_result import to_payload  # noqa: E402

    async def _work(idx: int) -> dict:
        async with streamablehttp_client(url) as (r, w):
            async with ClientSession(r, w) as session:
                await session.initialize()
                cid = f"mc3-{idx}-{int(time.time()*1000)}"
                await session.call_tool("consumer_register", {"consumer_id": cid})
                reg = to_payload(await session.call_tool("consumer_checkpoint_get", {"consumer_id": cid}))
                return {"idx": idx, "checkpoint": reg.get("checkpoint", -1)}

    results = await asyncio.gather(*[_work(i) for i in range(3)])
    for r in results:
        runner.assert_eq(f"{name}-idx{r['idx']}", r["checkpoint"], 0)


# ---------------------------------------------------------------------------
# Test ordering
# ---------------------------------------------------------------------------
_TESTS = [
    test_concurrent_clients_gather,
    test_concurrent_generate_and_ping,
    test_three_concurrent_sessions,
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
        print("  Multi-Client Tests")
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
