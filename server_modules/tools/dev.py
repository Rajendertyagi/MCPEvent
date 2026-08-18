"""
Development/test tools: dev_progress_test, dev_long_running_test.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

from errors import ValidationError
from mcp.server.mcpserver.context import Context
from server_modules.contract import (
    TOOL_DEV_PROGRESS_TEST,
    TOOL_DEV_LONG_RUNNING_TEST,
)


def register_dev_tools(mcp, services, **kwargs) -> None:
    """Register development/testing tools."""

    @mcp.tool(name=TOOL_DEV_PROGRESS_TEST)
    async def progress_report_test(
        total: int = 10,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        """
        [TESTING] Report progress updates to demonstrate the progress API.

        total: Number of progress steps to report.
        ctx:   Injected MCP context (auto-detected by SDK).

        Returns final progress value.
        """
        if total < 1 or total > 100:
            raise ValidationError("total must be between 1 and 100")

        for i in range(1, total + 1):
            if ctx is not None:
                await ctx.report_progress(float(i), float(total), f"Step {i}/{total}")
            await asyncio.sleep(0.01)
        return {
            "status": "completed",
            "total": total,
            "final_progress": float(total),
        }

    @mcp.tool(name=TOOL_DEV_LONG_RUNNING_TEST)
    async def long_running_test(
        duration_seconds: float = 5.0,
        cancel_check_interval: float = 0.1,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        """
        [TESTING] A cancellable long-running operation for testing timeout/cancellation.

        duration_seconds: How long to run (default 5).
        cancel_check_interval: How often to check for cancellation (default 0.1s).
        ctx: Injected MCP context.

        Returns status with elapsed time.
        """
        if duration_seconds <= 0:
            raise ValidationError("duration_seconds must be positive")
        if cancel_check_interval <= 0:
            raise ValidationError("cancel_check_interval must be positive")

        start = time.monotonic()
        while True:
            elapsed = time.monotonic() - start
            if elapsed >= duration_seconds:
                break
            await asyncio.sleep(cancel_check_interval)

        return {
            "status": "completed",
            "elapsed_seconds": round(time.monotonic() - start, 3),
            "duration_requested": duration_seconds,
        }
