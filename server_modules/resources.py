"""
MCP resource registration.

Resources are registered after MCPServer construction via explicit
registration functions rather than import-time decorators, avoiding
circular import risks.
"""

from __future__ import annotations

import json
import platform
from datetime import datetime, timezone
from typing import Any

import events
from server_modules.contract import (
    RESOURCE_EVENT_LATEST,
    RESOURCE_EVENTS_PENDING,
    RESOURCE_SYSTEM_INFO,
    RESOURCE_SOURCES_STATUS,
)


def register_resources(mcp, services: "Services", constants: dict[str, str]) -> None:
    """Register all MCP resources on the given server instance."""
    SERVER_VERSION = constants["SERVER_VERSION"]
    CONTRACT_VERSION = constants["CONTRACT_VERSION"]
    MCP_SPEC = constants["MCP_SPEC"]
    TIMEOUTS = services.timeouts
    REPLAY_CFG = services.replay_cfg

    @mcp.resource(RESOURCE_EVENT_LATEST)
    def event_latest() -> str:
        """Return the latest event as compact JSON."""
        return json.dumps(events.get_latest_event(), ensure_ascii=False)

    @mcp.resource(RESOURCE_EVENTS_PENDING)
    def events_pending() -> str:
        """Return all persistent events, newest first."""
        pending_events = services.store.list_pending(limit=100)
        return json.dumps(pending_events, ensure_ascii=False)

    @mcp.resource(RESOURCE_SYSTEM_INFO)
    def server_info() -> str:
        """Return server metadata including feature capabilities."""
        uptime_seconds = (
            datetime.now(timezone.utc) - events.get_server_start_time()
        ).total_seconds()

        info: dict[str, Any] = {
            "name": constants.get("SERVER_NAME", "MCP Event Server"),
            "version": SERVER_VERSION,
            "contract_version": CONTRACT_VERSION,
            "purpose": "Generic self-hosted MCP event server",
            "transport": "streamable-http",
            "endpoint": "http://{0}:{1}/mcp".format(
                constants.get("LISTEN_HOST", "127.0.0.1"),
                constants.get("LISTEN_PORT", 8000),
            ),
            "python": platform.python_version(),
            "mcp_sdk": "mcp==2.0.0",
            "mcp_spec": MCP_SPEC,
            "event_resource": RESOURCE_EVENT_LATEST,
            "events_pending_resource": RESOURCE_EVENTS_PENDING,
            "info_resource": RESOURCE_SYSTEM_INFO,
            "event_count": events.get_event_count(),
            "persistent_event_count": services.store.count(),
            "consumer_count": len(services.store.list_consumers()),
            "uptime_seconds": round(uptime_seconds, 1),
            "started_at": events.get_server_start_time().isoformat(),
            "features": {
                "live_events": True,
                "persistent_events": True,
                "routing": True,
                "acknowledgement": True,
                "replay": True,
                "checkpoints": True,
                "timeouts": True,
                "cancellation": True,
                "progress": True,
                "background_runtime": True,
                "structured_errors": True,
                "source_connectors": True,
            },
            "limits": {
                "replay_default_limit": REPLAY_CFG["default_limit"],
                "replay_max_limit": REPLAY_CFG["max_limit"],
                "timeout_default_seconds": TIMEOUTS["default_tool_seconds"],
                "timeout_database_seconds": TIMEOUTS["database_seconds"],
            },
        }

        return json.dumps(info, indent=2, ensure_ascii=False)

    @mcp.resource(RESOURCE_SOURCES_STATUS)
    def sources_status() -> str:
        """Return status of all registered source connectors."""
        status = services.source_manager.get_status()
        return json.dumps(status, indent=2, ensure_ascii=False)
