"""
Background task tools: dev_background_publish_test, dev_task_list.
"""

from __future__ import annotations

import asyncio
from typing import Any

import events
from server_modules.contract import (
    TOOL_DEV_BACKGROUND_PUBLISH_TEST,
    TOOL_DEV_TASK_LIST,
)


def register_background_tools(mcp, services, **kwargs) -> None:
    """Register background task related tools."""

    @mcp.tool(name=TOOL_DEV_BACKGROUND_PUBLISH_TEST)
    async def background_publish_test(
        event_type: str = "test.background",
        persistent: bool = True,
        routing: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        [TESTING] Publish an event from the background (simulates a source connector).
        """
        event = await events.publish_event(
            event_type=event_type,
            source="background-test",
            data={"phase": 7, "test": True},
            persistent=persistent,
            routing=routing,
            store=services.store,
            bus=services.subscription_bus,
        )
        return {
            "status": "published",
            "event": event,
        }

    @mcp.tool(name=TOOL_DEV_TASK_LIST)
    def list_background_tasks() -> dict[str, Any]:
        """
        [TESTING] List all registered background tasks and their status.
        """
        return {
            "task_count": services.bg_task_manager.active_count,
            "tasks": services.bg_task_manager.status(),
        }
