#!/usr/bin/env python3
"""
Shared server lifecycle helpers for the MCP event server test suite.

Each test file that needs a server should:
  1. Call ``start_server(config_overrides, data_dir_name)`` before tests
  2. Use ``get_server_url()`` to get the current URL
  3. Call ``stop_server()`` and ``restore_environment()`` in finally/atexit

The helper captures the ORIGINAL config bytes at start time so that the
repository baseline is always restored at the end, even if the test
overwrites config.json multiple times.

Hardening (test-runtime optimization):
  * A server that exits during startup fails FAST (no 20s TCP wait).
  * Graceful shutdown is bounded (3s terminate, then hard kill) so a stuck
    server cannot stall teardown.
  * MCP readiness is MANDATORY (Issue C): a server is only "ready" once a REAL
    MCP operation (initialize + ping) succeeds — TCP-open is only a pre-check,
    never sufficient on its own.
  * The ``atexit`` handler is a best-effort SAME-PROCESS safety net (helps on a
    normal test exit). It is NOT relied upon for cleanup after a runner timeout:
    a force-killed process cannot run atexit, so the RUNNER owns timeout cleanup
    via process-group signaling (see ``test/run_all.py``).
"""

from __future__ import annotations

import atexit
import asyncio
import json
import os
import socket
import subprocess
import sys
import time
from typing import Any

from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client as streamablehttp_client

from mcp_result import reserve_free_port, safe_teardown


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_DIR = os.path.dirname(os.path.dirname(_SCRIPT_DIR))  # go up from test/helpers/ to repo root
_CONFIG_PATH = os.path.join(_PROJECT_DIR, "config.json")
_LOG_DIR = os.path.join(_PROJECT_DIR, ".test_logs")


# ---------------------------------------------------------------------------
# Module-level server state (each test file manages its own lifecycle)
# ---------------------------------------------------------------------------
_server_proc: subprocess.Popen | None = None
_server_url: str = ""
_server_port: int = 0
_server_data_dir: str = ""
_server_original_config: bytes | None = None

# atexit registration guard (registered at most once per process)
_atexit_registered = False


def get_server_url() -> str:
    return _server_url


def get_server_port() -> int:
    return _server_port


def get_original_config() -> bytes | None:
    return _server_original_config


def set_server_state(proc: subprocess.Popen | None, url: str, port: int, data_dir: str) -> None:
    """Set the module-level server state."""
    global _server_proc, _server_url, _server_port, _server_data_dir
    _server_proc = proc
    _server_url = url
    _server_port = port
    _server_data_dir = data_dir


def clear_server_state() -> None:
    global _server_proc, _server_url, _server_port, _server_data_dir
    _server_proc = None
    _server_url = ""
    _server_port = 0
    _server_data_dir = ""


# ---------------------------------------------------------------------------
# atexit cleanup — kill orphaned servers even if the test is terminated
# ---------------------------------------------------------------------------

def _register_atexit() -> None:
    global _atexit_registered
    if not _atexit_registered:
        _atexit_registered = True
        atexit.register(_atexit_cleanup)


def _atexit_cleanup() -> None:
    """Best-effort cleanup of any server/config this process started."""
    try:
        if _server_proc is not None:
            stop_server()
        if _server_original_config is not None:
            safe_teardown(restore_config, _server_original_config)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------

def backup_config() -> bytes | None:
    if os.path.exists(_CONFIG_PATH):
        with open(_CONFIG_PATH, "rb") as f:
            return f.read()
    return None


def restore_config(backup: bytes | None) -> None:
    if backup is not None:
        with open(_CONFIG_PATH, "wb") as f:
            f.write(backup)
    elif os.path.exists(_CONFIG_PATH):
        try:
            os.remove(_CONFIG_PATH)
        except FileNotFoundError:
            pass


# ---------------------------------------------------------------------------
# Port / readiness
# ---------------------------------------------------------------------------

def _port_is_open(host: str, port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(0.5)
        try:
            s.connect((host, port))
            return True
        except (ConnectionRefusedError, OSError):
            return False


async def wait_mcp_ready(
    url: str, proc: subprocess.Popen | None = None, timeout: float = 20.0
) -> None:
    """Mandatory MCP readiness: process-alive + TCP reachable + a REAL MCP op succeeds.

    TCP-open is only a pre-check (Issue C, §19). The server is READY only after a
    genuine MCP operation (``session.initialize()`` + ``session.call_tool("ping")``)
    completes successfully. If the process exits, or TCP never opens, or the real
    MCP op never succeeds before the deadline, this raises so the caller fails fast
    (§14, §15, §18, §20). Runs on a deadline (§17); no fixed sleep after ready (§21).

    The MCP SDK's ``streamable_http_client`` can raise an ``ExceptionGroup`` during
    context-manager teardown on Python 3.14 AFTER a successful call. We set
    ``ping_ok`` only after the op actually completes, so a successful op is not
    masked by teardown noise (§16, Python 3.14 safe).
    """
    deadline = time.monotonic() + timeout
    # URL format: http://host:port/mcp
    try:
        port = int(url.split(":")[2].split("/")[0])
    except (ValueError, IndexError):
        port = None

    # 1) TCP pre-check, with process-alive fast-fail (§18, §19).
    tcp_ready = False
    while time.monotonic() < deadline:
        if proc is not None and proc.poll() is not None:
            raise RuntimeError(
                f"Server process exited (exit={proc.poll()}) before becoming MCP-ready"
            )
        if port is not None and _port_is_open("127.0.0.1", port):
            tcp_ready = True
            break
        await asyncio.sleep(0.2)

    if not tcp_ready:
        raise TimeoutError(f"Server TCP port {port} not open within {timeout}s")

    # 2) REAL MCP operation — mandatory. TCP alone is NOT accepted (§14, §15, §19).
    ping_ok = False
    last_err = ""
    while time.monotonic() < deadline:
        try:
            async with streamablehttp_client(url) as (r, w):
                async with ClientSession(r, w) as session:
                    await session.initialize()
                    await session.call_tool("system_ping", {})  # a genuine MCP tool call
                    ping_ok = True  # set ONLY after the op actually succeeds
        except (KeyboardInterrupt, SystemExit):
            # Never swallow a runner-level interruption (§13).
            raise
        except Exception as exc:
            # Teardown ExceptionGroup on 3.14 can fire AFTER a successful call.
            if ping_ok:
                break
            last_err = f"{type(exc).__name__}: {exc}"
            await asyncio.sleep(0.3)
            continue
        else:
            # clean exit (no exception) -> ready
            break

    if not ping_ok:
        # Classify the failure: TCP was open but the MCP op never succeeded (§20).
        raise RuntimeError(
            f"Server TCP-open but MCP never became ready within {timeout}s "
            f"(real MCP op failed): {last_err}"
        )


# ---------------------------------------------------------------------------
# Server lifecycle
# ---------------------------------------------------------------------------

async def start_server(
    config_overrides: dict[str, Any] | None = None,
    data_dir: str = "data_test",
) -> subprocess.Popen:
    """Start an isolated MCP event server on a dynamic port.

    Args:
        config_overrides: Keys to override in the temporary config.
        data_dir: Isolated data directory (relative to PROJECT_DIR).

    Returns:
        The subprocess.Popen handle.

    Raises:
        RuntimeError: if the server process exits during startup (fast-fail,
        no 20s wait) or fails to become MCP-ready.
    """
    global _server_proc, _server_url, _server_port, _server_data_dir, _server_original_config

    port = reserve_free_port()
    host = "127.0.0.1"
    url = f"http://{host}:{port}/mcp"

    # Capture ORIGINAL config bytes ONCE before any test modifies config.json
    if _server_original_config is None:
        _server_original_config = backup_config()

    # Build temporary config
    base: dict[str, Any] = {}
    if os.path.exists(_CONFIG_PATH):
        try:
            with open(_CONFIG_PATH, "r", encoding="utf-8") as f:
                base = json.load(f)
        except (OSError, ValueError):
            base = {}
    if config_overrides:
        _merge_overrides(base, config_overrides)
    base["host"] = host
    base["port"] = port
    base["data_dir"] = data_dir

    with open(_CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(base, f, indent=2)
        f.flush()
        os.fsync(f.fileno())

    os.makedirs(_LOG_DIR, exist_ok=True)
    out_path = os.path.join(_LOG_DIR, f"server_{port}.out")
    err_path = os.path.join(_LOG_DIR, f"server_{port}.err")
    with open(out_path, "w", encoding="utf-8") as out_f, open(err_path, "w", encoding="utf-8") as err_f:
        proc = subprocess.Popen(
            [sys.executable, "server.py"],
            cwd=_PROJECT_DIR,
            stdout=out_f,
            stderr=err_f,
        )

    _server_url = url
    _server_port = port
    _server_data_dir = data_dir
    _server_proc = proc

    # Wait for server to start (either TCP open or process exits)
    for _ in range(100):
        if _port_is_open(host, port) or proc.poll() is not None:
            break
        time.sleep(0.1)

    # If the server bound to a different port, update our state
    # (can happen if the reserved port was grabbed between reservation and bind)
    actual_port = _detect_server_port(err_path, port)
    if actual_port != port:
        port = actual_port
        url = f"http://{host}:{port}/mcp"
        _server_url = url
        _server_port = port

    # Fast-fail: if the process already exited, do NOT wait 20s for TCP.
    if proc.poll() is not None:
        diag = (
            f"Server process exited during startup on port {port} "
            f"(exit={proc.poll()}).\n"
            f"  data_dir       : {data_dir}\n"
            f"  stdout (tail)  :\n{_read_log_tail(out_path)}\n"
            f"  stderr (tail)  :\n{_read_log_tail(err_path)}\n"
        )
        clear_server_state()
        raise RuntimeError(diag)

    try:
        await wait_mcp_ready(url, proc, timeout=20)
    except Exception as exc:
        already_exited = proc.poll()
        diag = (
            f"Server failed to become MCP-ready on port {port}.\n"
            f"  already_exited : {already_exited is not None} (exit={already_exited})\n"
            f"  data_dir       : {data_dir}\n"
            f"  stdout (tail)  :\n{_read_log_tail(out_path)}\n"
            f"  stderr (tail)  :\n{_read_log_tail(err_path)}\n"
            f"  probe_error    : {exc}"
        )
        stop_server(proc)
        raise RuntimeError(diag) from exc

    # Ensure cleanup runs even if the test is killed by the runner.
    _register_atexit()
    return proc


def _detect_server_port(err_path: str, expected_port: int) -> int:
    """Parse the actual port from server startup logs."""
    try:
        with open(err_path, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                if "port=" in line.lower():
                    # Extract port number from lines like 'port=54321'
                    for part in line.split():
                        if part.startswith("port="):
                            try:
                                return int(part.split("=")[1])
                            except (ValueError, IndexError):
                                pass
    except OSError:
        pass
    return expected_port


def stop_server(proc: subprocess.Popen | None = None) -> None:
    """Stop ONLY the tracked server process. Safe to re-call.

    Graceful terminate is bounded at 3s; if the process is still alive it is
    hard-killed. This prevents a stuck server from stalling teardown.
    """
    target = proc or _server_proc
    if target is None:
        return
    try:
        if target.poll() is None:
            target.terminate()
            try:
                target.wait(timeout=3)
            except subprocess.TimeoutExpired:
                target.kill()
                target.wait(timeout=2)
    except (ProcessLookupError, OSError):
        pass
    finally:
        if proc is None:
            clear_server_state()


async def restart_server(
    config_overrides: dict[str, Any] | None = None,
    data_dir: str = "data_test",
    preserve_data: bool = False,
) -> subprocess.Popen:
    """Stop the current server and start a fresh one (no orphan left behind)."""
    global _server_original_config
    stop_server()
    await asyncio.sleep(0.3)
    # Preserve original config bytes across restarts
    return await start_server(config_overrides, data_dir)


def restore_environment() -> None:
    """Stop server, restore config, clean isolated data + logs."""
    global _server_original_config, _server_data_dir
    import shutil
    stop_server()
    safe_teardown(restore_config, _server_original_config)
    _server_original_config = None
    _server_data_dir = ""
    # Clean isolated data dir
    if os.path.exists(os.path.join(_PROJECT_DIR, "data_test")):
        try:
            shutil.rmtree(os.path.join(_PROJECT_DIR, "data_test"), ignore_errors=True)
        except OSError:
            pass
    # Clean log dir
    try:
        shutil.rmtree(_LOG_DIR, ignore_errors=True)
    except OSError:
        pass


def _read_log_tail(path: str, lines: int = 40) -> str:
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            return "".join(f.readlines()[-lines:])
    except OSError:
        return ""


def _merge_overrides(base: dict, overrides: dict) -> None:
    for k, v in overrides.items():
        if isinstance(v, dict) and k in base and isinstance(base[k], dict):
            base[k].update(v)
        else:
            base[k] = v
