#!/usr/bin/env python3
"""
Regression runner for the MCP event server test suite.

Each feature-focused test file is executed as an ISOLATED SUBPROCESS with a
HARD per-file timeout. A hung or stuck test can therefore never stall the
whole suite for hours: after the hard timeout the RUNNER OWNS cleanup.

PARENT-OWNED CLEANUP (Issue A, §3-§7, §37)
------------------------------------------
A force-killed Python process cannot be relied upon to run its ``atexit``
handler, so we do NOT depend on the child's atexit to reap a spawned server.
Instead the runner terminates the child's PROCESS GROUP:

  * Windows: the child is spawned with ``CREATE_NEW_PROCESS_GROUP``, so it is
    the group leader. A graceful signal is ``CTRL_BREAK_EVENT`` sent to that
    group (which reaches the child AND the ``server.py`` it spawned, because
    the server inherits the group). If the group does not exit within a bounded
    grace window, the runner HARD-KILLS the owned process tree with
    ``taskkill /F /T /PID <child>`` (tree kill of ONLY the owned hierarchy —
    never ``taskkill /IM python.exe`` which would hit unrelated processes).
  * POSIX: the child is spawned with ``start_new_session=True`` (own pgid).
    Graceful = ``SIGTERM`` to the group; hard = ``SIGKILL`` to the group
    (``os.killpg``). No ``psutil``; only the owned hierarchy is touched.

FULLY BOUNDED TERMINATE/KILL (Issue B, §8-§13, §38)
---------------------------------------------------
Every wait is bounded. On timeout: graceful terminate -> bounded grace wait
-> if still alive, hard-kill the group -> bounded final wait -> report. A
timed-out file is reported as TIMEOUT (distinct from FAILED) with PID and
cleanup diagnostics (§10). A cleanup failure is surfaced and never masks a
failure as green (§11). Exception handling is narrow (§12); KeyboardInterrupt
and SystemExit are never swallowed (§13).

Usage:
    python test/run_all.py                 # full regression (group: all)
    python test/run_all.py --group fast    # quick, no-server / single-server files
    python test/run_all.py --group source  # source-focused files
    python test/run_all.py --group mcp     # MCP-protocol / integration files
    python test/run_all.py --group lifecycle
    python test/run_all.py --group consumer
    python test/run_all.py --group unit
    python test/run_all.py --group performance
    python test/run_all.py --group fast mcp --timeout 120

Tips:
    * While debugging ONE feature, run its file directly or ``--group <name>``.
      Do NOT run the full regression — it is the slow path by design.
    * ``--timeout N`` sets the hard kill time (seconds) for each feature file.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import signal
import subprocess
import sys
import time

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_DIR = os.path.dirname(_SCRIPT_DIR)

# Each entry: module key -> test file name (lives in test/).
_TEST_FILES: dict[str, str] = {
    "unit_sources": "test_unit_sources.py",
    "source_dedup": "test_source_dedup.py",
    "source_lifecycle": "test_source_lifecycle.py",
    "events": "test_events.py",
    "consumers": "test_consumers.py",
    "acknowledgement": "test_acknowledgement.py",
    "reconnect": "test_reconnect.py",
    "errors": "test_errors.py",
    "timeouts": "test_timeouts.py",
    "background_tasks": "test_background_tasks.py",
    "subscriptions": "test_subscriptions.py",
    "performance": "test_performance.py",
    "lifespan": "test_lifespan.py",
    "sdk_alignment": "test_sdk_alignment.py",
    "multi_client": "test_multi_client.py",
}

# Group -> ordered list of module keys. Order matters (fast/stable first).
GROUPS: dict[str, list[str]] = {
    "all": list(_TEST_FILES.keys()),
    "fast": [
        "unit_sources",
        "acknowledgement",
        "consumers",
        "background_tasks",
        "source_dedup",
        "events",
    ],
    "source": ["source_lifecycle", "source_dedup"],
    "consumer": ["consumers", "acknowledgement"],
    "mcp": ["subscriptions", "sdk_alignment", "lifespan", "multi_client", "errors", "timeouts"],
    "lifecycle": ["source_lifecycle", "reconnect"],
    "unit": ["unit_sources"],
    "performance": ["performance"],
}

# Hard cap per feature file. A file that exceeds this is killed and reported TIMEOUT.
DEFAULT_PER_FILE_TIMEOUT = 300.0

# Bounded windows (seconds) for the graceful grace wait and the final hard-kill wait.
_GRACE_WAIT = 10.0
_FINAL_WAIT = 10.0


# ---------------------------------------------------------------------------
# Process-group cleanup (parent-owned)
# ---------------------------------------------------------------------------

def _graceful_terminate(proc) -> str:
    """Ask the child's process GROUP to exit (bounded; caller waits after).

    Windows: CTRL_BREAK_EVENT to the group (reaches child + its server.py).
    POSIX: SIGTERM to the group (pgid == child pid via start_new_session).

    ``proc`` is an ``asyncio.subprocess.Process`` (from ``_run_one``); it exposes
    ``.returncode`` (None while running) rather than ``.poll()``.
    """
    if proc.returncode is not None:
        return "already_exited"
    if os.name == "nt":
        try:
            os.kill(proc.pid, signal.CTRL_BREAK_EVENT)
            return "CTRL_BREAK_EVENT->group"
        except (ProcessLookupError, OSError) as exc:
            return f"CTRL_BREAK_EVENT_failed:{type(exc).__name__}"
    try:
        pgid = os.getpgid(proc.pid)
        os.killpg(pgid, signal.SIGTERM)
        return "SIGTERM->group"
    except (ProcessLookupError, OSError) as exc:
        try:
            proc.terminate()
            return f"SIGTERM->group_failed:{type(exc).__name__};proc.terminate()"
        except Exception:
            return f"SIGTERM->group_failed:{type(exc).__name__}"


def _hard_kill_group(proc) -> str:
    """Hard-kill the OWNED process group (child + any server.py it spawned).

    Windows: ``taskkill /F /T /PID <child>`` — tree kill of ONLY the owned
    hierarchy (never ``/IM python.exe``). POSIX: SIGKILL to the group.
    """
    if proc.returncode is not None:
        return "already_exited"
    if os.name == "nt":
        try:
            subprocess.run(
                ["taskkill", "/F", "/T", "/PID", str(proc.pid)],
                capture_output=True,
                timeout=_FINAL_WAIT,
                check=False,
            )
            return "taskkill /F /T /PID (tree)"
        except Exception as exc:
            try:
                proc.kill()
                return f"taskkill_failed:{type(exc).__name__};proc.kill()"
            except Exception:
                return f"taskkill_failed:{type(exc).__name__}"
    try:
        pgid = os.getpgid(proc.pid)
        os.killpg(pgid, signal.SIGKILL)
        return "SIGKILL->group"
    except (ProcessLookupError, OSError) as exc:
        try:
            proc.kill()
            return f"SIGKILL->group_failed:{type(exc).__name__};proc.kill()"
        except Exception:
            return f"SIGKILL->group_failed:{type(exc).__name__}"


# ---------------------------------------------------------------------------
# Per-file runner
# ---------------------------------------------------------------------------

async def _run_one(display: str, filename: str, timeout: float) -> tuple[str, float, str, dict]:
    """Run one feature file as a subprocess.

    Returns (status, elapsed_seconds, output_text, diagnostics) where status is
    one of "PASS", "FAIL", "TIMEOUT". diagnostics carries pid / returncode /
    timed_out / cleanup_result for §10 / §11 reporting.
    """
    path = os.path.join(_SCRIPT_DIR, filename)
    creationflags = 0
    start_new_session = False
    if os.name == "nt":
        creationflags = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
    else:
        start_new_session = True

    t0 = time.monotonic()
    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        path,
        cwd=_PROJECT_DIR,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        creationflags=creationflags,
        start_new_session=start_new_session,
    )
    pid = proc.pid
    timed_out = False
    cleanup_result = "n/a (no timeout)"
    out = b""

    try:
        out, _ = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        # ---- Issue A + B: parent owns cleanup, fully bounded ----
        timed_out = True
        grace = _graceful_terminate(proc)  # graceful terminate to the group
        try:
            out, _ = await asyncio.wait_for(proc.communicate(), timeout=_GRACE_WAIT)
        except asyncio.TimeoutError:
            # Still alive -> hard-kill the OWNED process group (child + server.py).
            kill = _hard_kill_group(proc)
            try:
                out, _ = await asyncio.wait_for(proc.communicate(), timeout=_FINAL_WAIT)
                cleanup_result = f"graceful={grace}; hardkill={kill}; exited_after_hardkill"
            except asyncio.TimeoutError:
                # Bounded final wait elapsed and the group is STILL alive.
                # Report the cleanup failure; this must NOT mask the timeout as green.
                cleanup_result = f"graceful={grace}; hardkill={kill}; STILL_ALIVE_AFTER_HARDKILL"
        else:
            cleanup_result = f"graceful={grace}; exited_within_grace"
    except (KeyboardInterrupt, SystemExit):
        # Never swallow a runner-level interruption (§13). Leave the child to the
        # OS process group; re-raise so the user's Ctrl+C aborts the whole run.
        raise

    elapsed = time.monotonic() - t0
    text = (out or b"").decode(errors="replace")

    if timed_out:
        status = "TIMEOUT"  # distinct from FAILED (§10)
    else:
        status = "PASS" if proc.returncode == 0 else "FAIL"

    diag = {
        "pid": pid,
        "returncode": proc.returncode,
        "timed_out": timed_out,
        "cleanup_result": cleanup_result,
    }
    return status, elapsed, text, diag


def _resolve_keys(groups: list[str]) -> list[str]:
    keys: list[str] = []
    seen: set[str] = set()
    for g in groups:
        for k in GROUPS.get(g, []):
            if k not in seen and k in _TEST_FILES:
                seen.add(k)
                keys.append(k)
    return keys


async def run_all(groups: list[str], timeout: float) -> int:
    keys = _resolve_keys(groups)
    if not keys:
        print(f"[error] no modules resolved for groups: {groups}")
        return 2

    total_start = time.monotonic()
    results: list[tuple[str, str, float, str, dict]] = []

    for key in keys:
        filename = _TEST_FILES[key]
        print()
        print("=" * 66)
        print(f"  Running [{key}]  ({filename})")
        print("=" * 66)
        status, elapsed, text, diag = await _run_one(key, filename, timeout)
        results.append((key, status, elapsed, text, diag))

        if status == "TIMEOUT":
            print(f"  [TIMEOUT] {key:<18} {elapsed:6.1f}s  pid={diag['pid']}")
            print(f"           cleanup: {diag['cleanup_result']}")
            if "STILL_ALIVE" in diag["cleanup_result"]:
                # §11: cleanup failure must not false-green — surface it loudly.
                print("  [CLEANUP-FAILURE] owned process/group NOT reaped after hard kill")
        else:
            print(f"  [{status}] {key:<18} {elapsed:6.1f}s")
        if status != "PASS":
            print("  ---- output tail ----")
            tail = text.strip().splitlines()[-40:]
            print("\n".join(tail))

    total_el = time.monotonic() - total_start
    print()
    print("=" * 66)
    print("  Regression Summary")
    print("=" * 66)
    for key, status, elapsed, _, _ in results:
        print(f"  [{status}] {key:<18} {elapsed:6.1f}s")

    # Slowest files — helps focus optimization effort.
    slow = sorted(results, key=lambda r: r[2], reverse=True)[:3]
    if slow:
        print("\n  Slowest files:")
        for key, _, elapsed, _, _ in slow:
            print(f"    {key:<18} {elapsed:6.1f}s")

    print(f"\n  Total wall time: {total_el:.1f}s")
    all_ok = all(status == "PASS" for _, status, _, _, _ in results)
    print(f"  Result: {'ALL PASSED' if all_ok else 'SOME FAILED'}")
    return 0 if all_ok else 1


def main() -> None:
    ap = argparse.ArgumentParser(description="MCP event server regression runner")
    ap.add_argument(
        "--group",
        action="append",
        default=[],
        help="group name(s): all, fast, source, consumer, mcp, lifecycle, unit, performance "
             "(repeatable; default: all)",
    )
    ap.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_PER_FILE_TIMEOUT,
        help="hard per-file timeout in seconds (default: 300)",
    )
    ap.add_argument(
        "--list-groups",
        action="store_true",
        help="print available groups and exit",
    )
    args = ap.parse_args()

    if args.list_groups:
        for name, mods in GROUPS.items():
            print(f"  {name:<12} {', '.join(mods)}")
        return

    groups = args.group or ["all"]
    rc = asyncio.run(run_all(groups, args.timeout))
    sys.exit(rc)


if __name__ == "__main__":
    main()
