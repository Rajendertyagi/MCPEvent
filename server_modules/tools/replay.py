"""
Replay tools: consumer_event_pending_list, consumer_event_acknowledge, consumer_checkpoint_get.
"""

from __future__ import annotations

import asyncio
from typing import Annotated, Any

from errors import ValidationError
from pydantic import Field, StrictInt
from server_modules.contract import (
    TOOL_CONSUMER_EVENT_PENDING_LIST,
    TOOL_CONSUMER_EVENT_ACKNOWLEDGE,
    TOOL_CONSUMER_CHECKPOINT_GET,
)
from server_modules.lifecycle import run_with_timeout


def register_replay_tools(mcp, services, **kwargs) -> None:
    """Register replay/checkpoint-related tools."""

    @mcp.tool(name=TOOL_CONSUMER_EVENT_PENDING_LIST)
    async def get_pending_events(
        consumer_id: str,
        limit: int = 50,
        after_sequence: Annotated[StrictInt, Field(ge=0)] | None = None,
    ) -> dict[str, Any]:
        """
        Get pending (unacknowledged) persistent events for a consumer,
        starting from the consumer's durable checkpoint or an explicit
        after_sequence for pagination.
        """
        consumer_id = consumer_id.strip()
        if not consumer_id:
            raise ValidationError("consumer_id must not be empty after trimming")

        # Validate after_sequence: None or non-negative integer (reject bool, float, string, negative)
        if after_sequence is not None:
            if isinstance(after_sequence, bool) or not isinstance(after_sequence, int):
                raise ValidationError("after_sequence must be a non-negative integer or null")
            if after_sequence < 0:
                raise ValidationError("after_sequence must be a non-negative integer or null")

        effective_limit = min(limit, services.replay_cfg["max_limit"])
        return await run_with_timeout(
            asyncio.to_thread(
                services.store.replay_events,
                consumer_id=consumer_id,
                limit=effective_limit,
                after_sequence=after_sequence,
            ),
            operation=f"get_pending_events({consumer_id})",
            timeout_seconds=services.timeouts["database_seconds"],
        )

    @mcp.tool(name=TOOL_CONSUMER_EVENT_ACKNOWLEDGE)
    async def acknowledge_event(
        consumer_id: str,
        event_id: str,
    ) -> dict[str, Any]:
        """
        Acknowledge that a consumer has successfully processed a persistent event.

        Acknowledgement is idempotent — repeated calls succeed silently.
        The first acknowledgement timestamp is preserved.

        After acknowledgement, the server attempts to advance the consumer's
        durable checkpoint to the highest safe sequence.
        """
        consumer_id = consumer_id.strip()
        event_id = event_id.strip()
        if not consumer_id:
            raise ValidationError("consumer_id must not be empty after trimming")
        if not event_id:
            raise ValidationError("event_id must not be empty after trimming")

        acknowledged = await run_with_timeout(
            asyncio.to_thread(services.store.acknowledge_event, consumer_id=consumer_id, event_id=event_id),
            operation=f"acknowledge_event({consumer_id},{event_id[:8]}...)",
            timeout_seconds=services.timeouts["database_seconds"],
        )

        # Attempt checkpoint advancement
        new_cp = await run_with_timeout(
            asyncio.to_thread(services.store.advance_checkpoint, consumer_id),
            operation=f"advance_checkpoint({consumer_id})",
            timeout_seconds=services.timeouts["database_seconds"],
        )

        return {
            "status": "acknowledged",
            "consumer_id": consumer_id,
            "event_id": event_id,
            "checkpoint": new_cp,
        }

    @mcp.tool(name=TOOL_CONSUMER_CHECKPOINT_GET)
    async def get_consumer_checkpoint(consumer_id: str) -> dict[str, Any]:
        """Get the current durable checkpoint for a consumer."""
        consumer_id = consumer_id.strip()
        if not consumer_id:
            raise ValidationError("consumer_id must not be empty after trimming")

        cp = await asyncio.to_thread(services.store.get_checkpoint, consumer_id)
        return {
            "consumer_id": consumer_id,
            "checkpoint": cp,
        }
