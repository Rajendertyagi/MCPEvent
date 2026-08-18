"""
Server lifecycle: timeout wrapper and banner/shutdown helpers.

Extracted from server.py to keep the main module focused on MCP wiring.
"""

from __future__ import annotations

import asyncio
import logging
import sys
from typing import Any

from errors import OperationTimeoutError

logger = logging.getLogger("event_server")


async def run_with_timeout(
    coro: Any,
    operation: str,
    timeout_seconds: float,
) -> Any:
    """Run a coroutine with a timeout. Propagates cancellation correctly."""
    try:
        return await asyncio.wait_for(coro, timeout=timeout_seconds)
    except asyncio.TimeoutError:
        raise OperationTimeoutError(operation, timeout_seconds)
    except asyncio.CancelledError:
        # Re-raise cancellation — don't swallow it
        raise


def print_banner(
    mcp_spec: str,
    listen_host: str,
    listen_port: int,
    event_resource_uri: str,
    events_pending_uri: str,
    info_resource_uri: str,
    sources_resource_uri: str,
    log_level: str,
    data_dir: str,
    timeouts: dict[str, Any],
) -> None:
    print()
    print("=" * 56)
    print("  MCP Event Server")
    print("=" * 56)
    print()
    print("  MCP SDK           : 2.0.0")
    print("  MCP Spec          : {0}".format(mcp_spec))
    print("  Transport         : Streamable HTTP")
    print("  Endpoint          : http://{0}:{1}/mcp".format(listen_host, listen_port))
    print("  Event Resource    : {0}".format(event_resource_uri))
    print("  Pending Events    : {0}".format(events_pending_uri))
    print("  Info Resource     : {0}".format(info_resource_uri))
    print("  Sources Resource  : {0}".format(sources_resource_uri))
    print("  Log Level         : {0}".format(log_level))
    print("  Data Directory    : {0}".format(data_dir))
    print("  Timeout (tool)    : {0}s".format(timeouts["default_tool_seconds"]))
    print("  Timeout (DB)      : {0}s".format(timeouts["database_seconds"]))
    print("  Timeout (shutdown): {0}s".format(timeouts["shutdown_seconds"]))
    print()
    print("  Press CTRL+C to stop.")
    print()
    logger.info(
        "started  host={0}  port={1}  endpoint=http://{0}:{1}/mcp".format(
            listen_host, listen_port
        )
    )


def print_shutdown() -> None:
    logger.info("shutting down")
    print()
    print("Stopped.")
    print()
