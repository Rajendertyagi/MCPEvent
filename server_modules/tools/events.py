"""
Event tools: event_publish, event_list.
"""

from __future__ import annotations

import asyncio
from typing import Any

import events
from server_modules.contract import TOOL_EVENT_PUBLISH, TOOL_EVENT_LIST


def register_event_tools(mcp, services, **kwargs) -> None:
    """Register event-related tools."""

    @mcp.tool(name=TOOL_EVENT_PUBLISH)
    async def generate_event(
        event_type: str,
        source: str = "manual-test",
        data: dict[str, Any] | None = None,
        persistent: bool = False,
        routing: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Publish an event into the server.

        This tool calls the same internal publish_event() that future
        external sources will use.
        """
        event = await events.publish_event(
            event_type=event_type,
            source=source,
            data=data or {},
            persistent=persistent,
            routing=routing,
            store=services.store,
            bus=services.subscription_bus,
        )
        return {
            "status": "published",
            "event": event,
        }

    @mcp.tool(name=TOOL_EVENT_LIST)
    def list_events(limit: int = 10) -> dict[str, Any]:
        """
        List recent events from the in-memory history buffer.

        limit: Maximum number of events to return (default 10, max 50).
        """
        event_list = events.get_event_history(limit=limit)
        return {
            "total_events": events.get_event_count(),
            "returned": len(event_list),
            "events": event_list,
        }
