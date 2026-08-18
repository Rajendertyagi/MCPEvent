"""
Source management tools: dev_source_start, dev_source_fail, dev_source_stop.
"""

from __future__ import annotations

import asyncio
from typing import Any

import events
from server_modules.contract import (
    TOOL_DEV_SOURCE_START,
    TOOL_DEV_SOURCE_FAIL,
    TOOL_DEV_SOURCE_STOP,
)


def register_source_tools(mcp, services, **kwargs) -> None:
    """Register source management tools."""

    @mcp.tool(name=TOOL_DEV_SOURCE_START)
    async def start_test_source(
        name: str = "test-source",
        event_type: str = "test.source",
        persistent: bool = False,
        delay_seconds: float = 0.5,
    ) -> dict[str, Any]:
        """
        [TESTING] Start a background coroutine that publishes an event after a delay.

        Demonstrates the background task manager and source extension seam.
        """
        async def _source_loop():
            await asyncio.sleep(delay_seconds)
            await events.publish_event(
                event_type=event_type,
                source=f"background:{name}",
                data={"source_name": name, "test": True},
                persistent=persistent,
                store=services.store,
                bus=services.subscription_bus,
            )

        await services.bg_task_manager.start(name, _source_loop())
        return {"status": "started", "name": name}

    @mcp.tool(name=TOOL_DEV_SOURCE_FAIL)
    async def start_failing_source(
        name: str = "failing-source",
        delay_seconds: float = 0.2,
    ) -> dict[str, Any]:
        """
        [TESTING] Start a background coroutine that intentionally fails.

        Proves that one failing background task does not kill the server.
        """
        async def _failing_loop():
            await asyncio.sleep(delay_seconds)
            raise RuntimeError(f"intentional failure in source '{name}'")

        await services.bg_task_manager.start(name, _failing_loop())
        return {"status": "started", "name": name}

    @mcp.tool(name=TOOL_DEV_SOURCE_STOP)
    async def stop_test_source(name: str = "test-source") -> dict[str, Any]:
        """
        [TESTING] Stop a named background source.

        Uses the SourceManager public stop API to signal the source's dedicated
        stop event and cancel its background task. Also cancels the raw test-task
        started by dev_source_start/dev_source_fail (which run under the bare
        task name, outside SourceManager).
        """
        managed = False
        if services.source_manager is not None:
            managed = await services.source_manager.stop_source(name)
        # The [TESTING] dev_source_start/dev_source_fail helpers run under the
        # bare task name (not "source:<name>"), so cancel that too if present.
        await services.bg_task_manager.cancel(name)
        return {"status": "stopped", "name": name, "managed": managed}
