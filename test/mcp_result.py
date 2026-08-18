#!/usr/bin/env python3
"""
Shared, test-only MCP result + lifecycle helpers for the event-server harness.

This module is STRICTLY test/verification infrastructure.  It must NEVER be
imported by production code (server.py, events.py, store.py, runtime.py, ...).

Design goals (per the Phase-8.2 harness-repair spec):
  * Normalize an MCP ``CallToolResult`` into a uniform shape WITHOUT ever
    blindly JSON-decoding error text (§4/§5).
  * Preserve the native SDK contract: is_error / structured_content / content.
  * On success, prefer ``structured_content`` when populated; otherwise parse
    JSON from the text block only when the text is actually JSON.
  * On error, keep the raw text and expose ``is_error`` so assertions can use
    ``assert result["is_error"]`` / ``assert "sub" in result["text"]``.
  * Provide a structured-output observation helper for the later SDK-alignment
    verification (§6).
"""

from __future__ import annotations

import asyncio
import json
import os
import socket
import sys
from typing import Any

# Make sure the project root is importable when this file is loaded directly.
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_DIR = os.path.dirname(_SCRIPT_DIR)
for _p in (_PROJECT_DIR, _SCRIPT_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)


def normalize_tool_result(result: Any) -> dict[str, Any]:
    """Normalize an MCP ``CallToolResult`` into a uniform dict.

    Returns a dict with these keys (never raises on a well-formed result):
      * is_error          (bool)   — native SDK top-level error flag.
      * structured_content(dict|None) — SDK-native structured output, when set.
      * text              (str)    — first text content block, raw (NOT decoded).
      * parsed            (dict|None) — JSON decoded from ``text`` ONLY when the
                                       text is valid JSON.  Errors are NOT decoded.
      * content           (list)  — the raw content blocks (for deep inspection).

    Rules:
      * We never call ``json.loads`` on error text.  If the tool errored, the
        text is kept verbatim and ``parsed`` is left as ``None``.
      * On success we attempt to JSON-decode the text purely as a convenience for
        tests that still assert on the tool's JSON payload (e.g. ``status=="ok"``).
    """
    is_error = bool(getattr(result, "is_error", False))

    structured = getattr(result, "structured_content", None)
    if not isinstance(structured, dict):
        structured = None

    content = getattr(result, "content", None)
    text = ""
    if content:
        for block in content:
            block_text = getattr(block, "text", None)
            if isinstance(block_text, str):
                text = block_text
                break

    parsed: dict[str, Any] | None = None
    if text:
        try:
            loaded = json.loads(text)
        except (json.JSONDecodeError, TypeError, ValueError):
            loaded = None
        if isinstance(loaded, dict):
            parsed = loaded

    return {
        "is_error": is_error,
        "structured_content": structured,
        "text": text,
        "parsed": parsed,
        "content": content,
    }


def to_payload(result: Any) -> dict[str, Any]:
    """Convert a ``CallToolResult`` into the value a ``call()`` helper returns.

    Success path (backward compatible with tests that read the tool's JSON):
      * prefer ``structured_content`` when it is a populated dict,
      * else the JSON-decoded text payload when text is JSON,
      * else a small ``{"text": ...}`` wrapper for non-JSON success text.

    Error path (native MCP semantics):
      * return the full normalized dict so callers can assert ``is_error`` and
        inspect ``text`` / ``structured_content`` / ``parsed``.
    """
    norm = normalize_tool_result(result)
    if norm["is_error"]:
        return norm
    if isinstance(norm["structured_content"], dict):
        return norm["structured_content"]
    if norm["parsed"] is not None:
        return norm["parsed"]
    return {"text": norm["text"]}


async def observe_structured_output(
    url: str,
    tool_name: str,
    arguments: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Collect structured-output metadata for later SDK-alignment verification (§6).

    Returns a dict:
      * tool              — the tool name probed.
      * output_schema     — the tool's declared output schema (or None).
      * is_error          — whether the call errored.
      * structured_content— the structured_content returned (or None).
      * content           — the raw content blocks.

    Uses only public MCP client APIs.  No server process is started here; the
    caller is responsible for a running server at ``url``.
    """
    from mcp import ClientSession
    from mcp.client.streamable_http import streamable_http_client as streamablehttp_client

    async with streamablehttp_client(url) as (r, w):
        async with ClientSession(r, w) as session:
            await session.initialize()
            tools = await session.list_tools()
            schema = None
            for t in getattr(tools, "tools", []):
                if getattr(t, "name", None) == tool_name:
                    schema = getattr(t, "outputSchema", None) or getattr(t, "output_schema", None)
                    break
            result = await session.call_tool(tool_name, arguments or {})
            norm = normalize_tool_result(result)
            return {
                "tool": tool_name,
                "output_schema": schema,
                "is_error": norm["is_error"],
                "structured_content": norm["structured_content"],
                "content": norm["content"],
            }


def reserve_free_port(host: str = "127.0.0.1") -> int:
    """Reserve an OS-assigned free TCP port and return it.

    The socket is closed before returning, so there is a tiny window where the
    port could be grabbed by another process; the caller should write the port
    into the server config and start the server immediately, then detect an
    early exit (port-already-in-use) via the readiness probe.  This is the
    documented §13 free-port strategy and is far safer than a hardcoded global
    port that collides across runs/processes.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((host, 0))
        s.listen(1)
        return s.getsockname()[1]


def _is_expected_teardown_error(exc: BaseException) -> bool:
    """Return True if *exc* is routine teardown noise that should be suppressed.

    Covers ONLY exceptions that are genuinely normal during server/source
    cleanup.  Generic ``OSError`` is intentionally excluded — individual
    OSError subclasses (BrokenPipeError, ProcessLookupError, etc.) are checked
    first, and any remaining OSError (PermissionError, unexpected errno, …)
    is treated as unexpected.

    Does NOT cover assertion failures, runtime bugs, or unexpected structural
    errors.
    """
    if isinstance(exc, asyncio.CancelledError):
        return True
    if isinstance(exc, (EOFError, BrokenPipeError)):
        return True
    if isinstance(exc, ConnectionError):
        return True
    if isinstance(exc, ProcessLookupError):
        return True
    # FileNotFoundError is only expected for explicitly idempotent removal ops;
    # callers should pass expected=() if they need it.
    if isinstance(exc, RuntimeError):
        msg = str(exc).lower()
        if "event loop is closed" in msg or "cannot send data" in msg:
            return True
    return False


def _log_unexpected_cleanup(func_name: str, exc: BaseException) -> None:
    """Log an unexpected cleanup failure to stderr for diagnostics."""
    try:
        print(
            f"[safe_teardown] unexpected error in {func_name}: "
            f"{type(exc).__name__}: {exc}",
            file=sys.stderr,
            flush=True,
        )
    except Exception:
        pass  # don't fail while reporting


def safe_teardown(
    func,
    *args,
    primary_failure: BaseException | None = None,
    **kwargs,
) -> None:
    """Run a cleanup callable with narrowly-controlled exception handling.

    Behaviour depends on whether a primary test failure is already active:

    * **KeyboardInterrupt / SystemExit** — always propagate immediately,
      regardless of context.
    * **Expected teardown noise** (CancelledError, closed connections,
      already-stopped processes, known RuntimeError messages) — suppressed
      silently in all cases.
    * **Unexpected exceptions** when NO primary failure is active — re-raised.
      The test harness must fail; a false-green result is unacceptable.
    * **Unexpected exceptions** when a primary failure IS already active —
      logged to stderr for diagnostics but NOT re-raised, so the original
      failure remains primary and is not masked.

    ``ExceptionGroup`` members are classified individually.  If all members
    are expected, the group is suppressed.  If at least one member is
    unexpected and no primary failure is active, a new ``ExceptionGroup``
    containing only the unexpected members is raised.  If a primary failure
    is active, unexpected members are logged but the group is suppressed.

    ``BaseExceptionGroup`` (Python 3.14+) is handled the same way; any
    KeyboardInterrupt/SystemExit nested inside would have already been caught
    by the top-level handler above, so this path only sees Exception members.
    """
    # Detect whether we are currently inside an exception-handling block
    # (i.e. a primary test failure is already active).
    import sys as _sys
    _active_exc = _sys.exc_info()[1]
    _has_primary = _active_exc is not None or primary_failure is not None

    try:
        func(*args, **kwargs)
    except (KeyboardInterrupt, SystemExit):
        raise  # never suppress
    except ExceptionGroup as exc_group:
        unexpected = tuple(
            e for e in exc_group.exceptions if not _is_expected_teardown_error(e)
        )
        if unexpected:
            if _has_primary:
                for e in unexpected:
                    _log_unexpected_cleanup(func.__qualname__, e)
                return
            # Re-raise unexpected members so the test fails.
            raise ExceptionGroup("cleanup", unexpected) from None
        return  # all members were expected
    except BaseException as exc:
        if _is_expected_teardown_error(exc):
            return  # expected teardown noise — silently suppress
        if _has_primary:
            _log_unexpected_cleanup(func.__qualname__, exc)
            return  # preserve primary failure
        raise  # no primary — test must fail


__all__ = [
    "normalize_tool_result",
    "to_payload",
    "observe_structured_output",
    "reserve_free_port",
    "safe_teardown",
]
