"""
Alert tools: alert_create, alert_list, alert_get, alert_enable, alert_disable.

Public MCP surface for the generic alert engine (v1.1.0-candidate).
No broker-specific logic; these tools only manage alert definitions and
delegate evaluation to the AlertEvaluator via the canonical publish path.
"""

from __future__ import annotations

import asyncio
import uuid
from typing import Any

from errors import (
    AlertNotFoundError,
    ConsumerNotFoundError,
    StorageError,
    ValidationError,
)
from server_modules import alerts as alert_engine
from server_modules.contract import (
    TOOL_ALERT_CREATE,
    TOOL_ALERT_DISABLE,
    TOOL_ALERT_ENABLE,
    TOOL_ALERT_GET,
    TOOL_ALERT_LIST,
)


def register_alert_tools(mcp, services, **kwargs) -> None:
    """Register alert-related tools."""

    @mcp.tool(name=TOOL_ALERT_CREATE)
    async def create_alert(
        consumer_id: str,
        source: str,
        field_path: str,
        operator: str,
        value: Any,
        name: str | None = None,
        event_type: str | None = None,
        one_shot: bool = True,
    ) -> dict[str, Any]:
        """
        Create a generic alert definition for a consumer.

        The alert fires when a published event from `source` satisfies the
        condition `field_path <operator> value` (optionally restricted to
        `event_type`). When it fires, an `alert.triggered` event is published
        to the owning consumer.

        one_shot: if true (default), the alert auto-disables after one trigger.
        """
        consumer_id = (consumer_id or "").strip()
        source = (source or "").strip()
        field_path = (field_path or "").strip()
        if event_type is not None:
            event_type = event_type.strip() or None
        if name is not None:
            name = name.strip() or None

        # Validate domain rules before touching storage.
        alert_engine.validate_alert_definition(
            consumer_id=consumer_id,
            source=source,
            field_path=field_path,
            operator=operator,
            value=value,
            name=name,
            event_type=event_type,
            one_shot=one_shot,
        )

        alert_id = uuid.uuid4().hex
        try:
            alert = await asyncio.to_thread(
                services.store.create_alert,
                alert_id=alert_id,
                consumer_id=consumer_id,
                name=name,
                source=source,
                event_type=event_type,
                field_path=field_path,
                operator=operator,
                value=value,
                one_shot=one_shot,
            )
        except ConsumerNotFoundError:
            raise
        except Exception as exc:
            raise StorageError("alert creation failed", exc) from exc

        return {"status": "created", "alert": alert}

    @mcp.tool(name=TOOL_ALERT_LIST)
    def list_alerts(
        consumer_id: str,
        enabled: bool | None = None,
    ) -> dict[str, Any]:
        """
        List alert definitions owned by a consumer.

        enabled: if null, return all; if true/false, filter by enabled state.
        """
        consumer_id = (consumer_id or "").strip()
        if not consumer_id:
            raise ValidationError("consumer_id must not be empty after trimming")
        try:
            alerts = services.store.list_alerts(consumer_id, enabled)
        except Exception as exc:
            raise StorageError("alert listing failed", exc) from exc
        return {
            "consumer_id": consumer_id,
            "returned": len(alerts),
            "alerts": alerts,
        }

    @mcp.tool(name=TOOL_ALERT_GET)
    def get_alert(consumer_id: str, alert_id: str) -> dict[str, Any]:
        """
        Get a single alert definition owned by a consumer.

        Raises AlertNotFoundError if the alert does not exist or is not owned
        by the consumer.
        """
        consumer_id = (consumer_id or "").strip()
        alert_id = (alert_id or "").strip()
        if not consumer_id:
            raise ValidationError("consumer_id must not be empty after trimming")
        if not alert_id:
            raise ValidationError("alert_id must not be empty after trimming")
        try:
            alert = services.store.get_alert(consumer_id, alert_id)
        except Exception as exc:
            raise StorageError("alert fetch failed", exc) from exc
        if alert is None:
            raise AlertNotFoundError(alert_id)
        return {"alert": alert}

    @mcp.tool(name=TOOL_ALERT_ENABLE)
    def enable_alert(consumer_id: str, alert_id: str) -> dict[str, Any]:
        """
        Enable a previously disabled alert.

        Returns changed=true only if the alert was actually disabled before.
        Raises AlertNotFoundError if the alert does not exist or is not owned
        by the consumer.
        """
        consumer_id = (consumer_id or "").strip()
        alert_id = (alert_id or "").strip()
        if not consumer_id:
            raise ValidationError("consumer_id must not be empty after trimming")
        if not alert_id:
            raise ValidationError("alert_id must not be empty after trimming")
        try:
            changed = services.store.enable_alert(consumer_id, alert_id)
        except (AlertNotFoundError, ConsumerNotFoundError):
            raise
        except Exception as exc:
            raise StorageError("alert enable failed", exc) from exc
        return {"status": "enabled", "alert_id": alert_id, "changed": changed}

    @mcp.tool(name=TOOL_ALERT_DISABLE)
    def disable_alert(consumer_id: str, alert_id: str) -> dict[str, Any]:
        """
        Disable an alert (stops evaluation without deleting it).

        Returns changed=true only if the alert was actually enabled before.
        Raises AlertNotFoundError if the alert does not exist or is not owned
        by the consumer.
        """
        consumer_id = (consumer_id or "").strip()
        alert_id = (alert_id or "").strip()
        if not consumer_id:
            raise ValidationError("consumer_id must not be empty after trimming")
        if not alert_id:
            raise ValidationError("alert_id must not be empty after trimming")
        try:
            changed = services.store.disable_alert(consumer_id, alert_id)
        except (AlertNotFoundError, ConsumerNotFoundError):
            raise
        except Exception as exc:
            raise StorageError("alert disable failed", exc) from exc
        return {"status": "disabled", "alert_id": alert_id, "changed": changed}
