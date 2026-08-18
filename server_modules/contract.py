"""
Public MCP contract identifiers — single source of truth for external-facing names.

All production code MUST import from this module rather than redefining literals.
External clients depend on these exact values, not on internal Python names.

Ownership model:
    MCP public contract identifiers  -> this module
    Runtime/deployment defaults       -> server_modules/config.py
    Schema/migration version          -> store_modules/schema.py
    Domain exceptions                 -> errors.py
    Event-core behavior               -> events.py
    Source type registry              -> sources/registry.py
"""

from __future__ import annotations

# ─── Contract version ─────────────────────────────────────────────────────────
# Bumps only when the MCP tool/resource/event contract changes in a breaking way.
# Distinct from MCP spec version, app version, Python version, and SDK version.
CONTRACT_VERSION = "1.0.0"

# ─── Resource URIs ─────────────────────────────────────────────────────────────
# mcp-event:// is an APPLICATION-PRIVATE URI scheme. It is NOT an official
# MCP-defined scheme. MCP permits application-defined resource URIs subject to
# valid URI syntax (RFC 3986). Do not describe it as standardized by MCP.
RESOURCE_EVENT_LATEST = "mcp-event://events/latest"
RESOURCE_EVENTS_PENDING = "mcp-event://events/pending"
RESOURCE_SYSTEM_INFO = "mcp-event://system/info"
RESOURCE_SOURCES_STATUS = "mcp-event://sources/status"

# ─── Production tool names ─────────────────────────────────────────────────────
TOOL_SYSTEM_PING = "system_ping"

TOOL_EVENT_PUBLISH = "event_publish"
TOOL_EVENT_LIST = "event_list"

TOOL_CONSUMER_REGISTER = "consumer_register"
TOOL_CONSUMER_TOPIC_ADD = "consumer_topic_add"
TOOL_CONSUMER_EVENT_LIST = "consumer_event_list"
TOOL_CONSUMER_EVENT_PENDING_LIST = "consumer_event_pending_list"
TOOL_CONSUMER_EVENT_ACKNOWLEDGE = "consumer_event_acknowledge"
TOOL_CONSUMER_CHECKPOINT_GET = "consumer_checkpoint_get"

# ─── Dev/test tool names ───────────────────────────────────────────────────────
TOOL_DEV_PROGRESS_TEST = "dev_progress_test"
TOOL_DEV_LONG_RUNNING_TEST = "dev_long_running_test"
TOOL_DEV_BACKGROUND_PUBLISH_TEST = "dev_background_publish_test"
TOOL_DEV_TASK_LIST = "dev_task_list"
TOOL_DEV_SOURCE_START = "dev_source_start"
TOOL_DEV_SOURCE_FAIL = "dev_source_fail"
TOOL_DEV_SOURCE_STOP = "dev_source_stop"
