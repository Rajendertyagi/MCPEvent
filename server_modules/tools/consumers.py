"""
Consumer tools: consumer_register, consumer_topic_add, consumer_event_list.
"""

from __future__ import annotations

import asyncio
from typing import Any

from errors import ValidationError, StorageError, ConsumerNotFoundError
from server_modules.contract import (
    TOOL_CONSUMER_REGISTER,
    TOOL_CONSUMER_TOPIC_ADD,
    TOOL_CONSUMER_EVENT_LIST,
)


def register_consumer_tools(mcp, services, **kwargs) -> None:
    """Register consumer-related tools."""

    @mcp.tool(name=TOOL_CONSUMER_REGISTER)
    def register_consumer(consumer_id: str) -> dict[str, Any]:
        """Register a consumer identity. Idempotent — safe to call repeatedly."""
        consumer_id = consumer_id.strip()
        if not consumer_id:
            raise ValidationError("consumer_id must not be empty after trimming")
        try:
            services.store.register_consumer(consumer_id)
        except Exception as exc:
            raise StorageError("consumer registration failed", exc) from exc
        return {"status": "registered", "consumer_id": consumer_id}

    @mcp.tool(name=TOOL_CONSUMER_TOPIC_ADD)
    def add_consumer_topic(consumer_id: str, topic: str) -> dict[str, Any]:
        """Assign a topic to a consumer for topic-based routing."""
        consumer_id = consumer_id.strip()
        topic = topic.strip()
        if not consumer_id:
            raise ValidationError("consumer_id must not be empty after trimming")
        if not topic:
            raise ValidationError("topic must not be empty after trimming")
        try:
            services.store.add_topic(consumer_id, topic)
        except ConsumerNotFoundError:
            raise
        except Exception as exc:
            raise StorageError("topic addition failed", exc) from exc
        return {"status": "added", "consumer_id": consumer_id, "topic": topic}

    @mcp.tool(name=TOOL_CONSUMER_EVENT_LIST)
    async def list_relevant_events(
        consumer_id: str,
        after_sequence: int | None = None,
        limit: int = 50,
    ) -> dict[str, Any]:
        """
        List persistent events relevant to a consumer, ordered by sequence ascending.

        Events are filtered using materialized per-consumer state (created at publish time),
        so routing history is stable even if consumer topics change later.
        """
        consumer_id = consumer_id.strip()
        if not consumer_id:
            raise ValidationError("consumer_id must not be empty after trimming")

        effective_limit = min(limit, services.replay_cfg["max_limit"])
        event_list = await asyncio.to_thread(
            services.store.list_relevant_events,
            consumer_id=consumer_id,
            after_sequence=after_sequence,
            limit=effective_limit,
        )
        # Mark delivery for returned events
        for event in event_list:
            await asyncio.to_thread(services.store.mark_delivered, consumer_id, event["id"])
        return {
            "consumer_id": consumer_id,
            "returned": len(event_list),
            "events": event_list,
        }
