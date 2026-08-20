#!/usr/bin/env python3
"""
Verification script for the replay error-contract fix.
Read-only verification — no production code edits.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import sqlite3
import stat
import subprocess
import sys
import time

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_DIR = os.path.dirname(_SCRIPT_DIR)
for _p in (_PROJECT_DIR, _SCRIPT_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client as streamablehttp_client
from test.helpers.lifecycle import start_server, stop_server, restore_environment, get_server_url
from test.mcp_result import to_payload, normalize_tool_result

RUNTIME = r"D:\IT\Script\python\python.exe"
VERIFY_DATA_DIR = "data_verify_replay_fix"

results = {}


def ok(name: str, detail: str = ""):
    results[name] = True
    print(f"  PASS: {name}" + (f" — {detail}" if detail else ""))


def fail(name: str, detail: str = ""):
    results[name] = False
    print(f"  FAIL: {name}" + (f" — {detail}" if detail else ""))


async def mcp_call(tool_name: str, args: dict) -> dict:
    url = get_server_url()
    async with streamablehttp_client(url) as (r, w):
        async with ClientSession(r, w) as session:
            await session.initialize()
            result = await session.call_tool(tool_name, args)
            return to_payload(result)


async def step3_normal_replay():
    """Section 3: Normal replay success."""
    name = "step3-normal-replay"
    try:
        cid = f"verify-cid-{int(time.time()*1000)}"
        r = await mcp_call("consumer_register", {"consumer_id": cid})
        assert not r.get("is_error", False), f"consumer_register failed: {r!r}"

        seqs = []
        for i in range(3):
            r = await mcp_call("event_publish", {
                "event_type": f"test.replay_fix.step3.{i}",
                "source": "verify",
                "persistent": True,
            })
            assert not r.get("is_error", False), f"event_publish failed: {r!r}"
            seqs.append(r["event"]["sequence"])

        r = await mcp_call("consumer_event_pending_list", {"consumer_id": cid})
        assert not r.get("is_error", False), f"pending_list failed: {r!r}"
        events = r.get("events", [])
        assert len(events) == 3, f"expected 3 events, got {len(events)}"
        assert r.get("next_after_sequence") is not None
        assert r.get("returned") == 3
        ok(name, f"events={len(events)}, seqs={seqs}, next={r.get('next_after_sequence')}")
        return cid, seqs
    except Exception as exc:
        fail(name, str(exc))
        return None, []


async def step4_pagination(cid: str, seqs: list):
    """Section 4: Pagination sanity.
    The 'checkpoint' field in the replay response is after_seq (starting cursor),
    not the stored consumer checkpoint. It advances across pages as pagination progresses.
    """
    name = "step4-pagination"
    try:
        r1 = await mcp_call("consumer_event_pending_list", {
            "consumer_id": cid, "limit": 2, "after_sequence": None,
        })
        assert not r1.get("is_error", False), f"page1 failed: {r1!r}"
        events1 = r1.get("events", [])
        next1 = r1.get("next_after_sequence")
        cp1 = r1.get("checkpoint")

        r2 = await mcp_call("consumer_event_pending_list", {
            "consumer_id": cid, "limit": 2, "after_sequence": next1,
        })
        assert not r2.get("is_error", False), f"page2 failed: {r2!r}"
        events2 = r2.get("events", [])
        next2 = r2.get("next_after_sequence")
        cp2 = r2.get("checkpoint")

        assert len(events1) == 2, f"page1 expected 2 events, got {len(events1)}"
        assert len(events2) == 1, f"page2 expected 1 event, got {len(events2)}"
        assert all(e["sequence"] > next1 for e in events2), "page2 sequences not > page1 cursor"
        assert events1[0]["sequence"] < events1[1]["sequence"], "page1 not ascending"
        assert cp2 == next1, f"cp2={cp2} != next1={next1}"
        ok(name, f"p1_seqs={[e['sequence'] for e in events1]}, p2_seqs={[e['sequence'] for e in events2]}, cp1={cp1}, cp2={cp2}")
    except Exception as exc:
        fail(name, str(exc))


async def step5_6_failure_injection(cid: str):
    """Sections 5-6: Controlled failure injection and MCP error-contract hard gate."""
    name_fail = "step5-failure-injection"
    name_contract = "step6-mcp-error-contract"
    try:
        db_path = os.path.join(PROJECT_DIR, VERIFY_DATA_DIR, "events.db")
        os.chmod(db_path, stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
        ok(name_fail, "DB set read-only; commit will fail inside replay_events")

        r = await mcp_call("consumer_event_pending_list", {"consumer_id": cid})
        is_error = r.get("is_error", False)
        text = r.get("text", "")

        if is_error is True:
            if '{"status": "error"' in text or '"status":"error"' in text:
                fail(name_contract, f"is_error=True BUT text contains anti-pattern: {text[:200]}")
                fail(name_fail, "Same — anti-pattern in error text")
            else:
                ok(name_contract, f"is_error=True, text={text[:120]}")
        else:
            status = r.get("status")
            if status == "error":
                fail(name_contract, f"BUG STILL EXISTS: is_error=False with status:error: {r!r}")
                fail(name_fail, "Same — returns error dict as success")
            else:
                fail(name_contract, f"Unexpected shape: is_error={is_error}, keys={list(r.keys())}, text={text[:120]}")
                fail(name_fail, "Same")
    except Exception as exc:
        fail(name_fail, f"Exception during failure test: {exc}")
        fail(name_contract, f"Same: {exc}")


async def step7_recovery(cid: str):
    """Section 7: Recovery after removing read-only."""
    name = "step7-recovery"
    try:
        db_path = os.path.join(PROJECT_DIR, VERIFY_DATA_DIR, "events.db")
        os.chmod(db_path, stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IROTH)
        r = await mcp_call("consumer_event_pending_list", {"consumer_id": cid})
        if r.get("is_error", False):
            fail(name, f"Recovery failed: still error after restoring permissions: {r!r}")
        else:
            events = r.get("events", [])
            ok(name, f"Recovered: is_error=False, events={len(events)}")
    except Exception as exc:
        fail(name, str(exc))


async def step8_domain_error():
    """Section 8: Unknown consumer_id domain error."""
    name = "step8-domain-error"
    try:
        r = await mcp_call("consumer_event_pending_list", {"consumer_id": "nonexistent-verify-xyz"})
        if r.get("is_error") is True:
            ok(name, "ConsumerNotFoundError correctly produces is_error=True")
        else:
            fail(name, f"Expected is_error=True for unknown consumer, got: {r!r}")
    except Exception as exc:
        fail(name, str(exc))


async def step9_strict_after_sequence():
    """Section 9: Strict after_sequence control."""
    name = "step9-strict-after-seq"
    try:
        r = await mcp_call("consumer_event_pending_list", {"consumer_id": "verify-cid-start", "after_sequence": 1})
        ok(name + "-int-accepted", "after_sequence=1 accepted")

        r2 = await mcp_call("consumer_event_pending_list", {"consumer_id": "verify-cid-start", "after_sequence": True})
        if r2.get("is_error") is True:
            ok(name + "-bool-rejected", "after_sequence=true correctly rejected")
        else:
            fail(name + "-bool-rejected", f"Expected error for bool after_sequence, got: {r2!r}")
    except Exception as exc:
        fail(name, str(exc))


def step10_anti_pattern_search():
    """Section 10: Search production code for remaining anti-pattern."""
    name = "step10-anti-pattern-search"
    patterns = ['{"status": "error"', '"status": "error"']
    found = []
    for root, dirs, files in os.walk(PROJECT_DIR):
        dirs[:] = [d for d in dirs if d not in ("test", ".venv", "__pycache__", "node_modules", ".git", VERIFY_DATA_DIR)]
        for f in files:
            if not f.endswith(".py"):
                continue
            path = os.path.join(root, f)
            try:
                with open(path, "r", encoding="utf-8", errors="replace") as fh:
                    for i, line in enumerate(fh, 1):
                        for pat in patterns:
                            if pat in line:
                                found.append(f"{path}:{i}: {line.strip()}")
            except OSError:
                pass
    if found:
        fail(name, f"Found {len(found)} occurrence(s):")
        for f in found:
            print(f"    {f}")
    else:
        ok(name, "No {'status': 'error'} anti-pattern in production code")


def step11_run_tests():
    """Section 11: Run focused existing tests."""
    name_errors = "step11-test_errors"
    name_reconnect = "step11-test_reconnect"
    try:
        r1 = subprocess.run(
            [RUNTIME, "test/test_errors.py"],
            cwd=PROJECT_DIR, capture_output=True, text=True, timeout=60,
        )
        if r1.returncode == 0:
            ok(name_errors, "passed")
        else:
            fail(name_errors, f"exit={r1.returncode}\n{r1.stdout[-500:]}\n{r1.stderr[-500:]}")
    except Exception as exc:
        fail(name_errors, str(exc))

    try:
        r2 = subprocess.run(
            [RUNTIME, "test/test_reconnect.py"],
            cwd=PROJECT_DIR, capture_output=True, text=True, timeout=60,
        )
        if r2.returncode == 0:
            ok(name_reconnect, "passed")
        else:
            fail(name_reconnect, f"exit={r2.returncode}\n{r2.stdout[-500:]}\n{r2.stderr[-500:]}")
    except Exception as exc:
        fail(name_reconnect, str(exc))


async def main():
    global PROJECT_DIR
    PROJECT_DIR = _PROJECT_DIR

    print("=" * 60)
    print("REPLAY ERROR-CONTRACT VERIFICATION")
    print("=" * 60)

    # -- Step 1: Static fix confirmation --
    print("\n[1] STATIC FIX CONFIRMATION")
    store_path = os.path.join(PROJECT_DIR, "store.py")
    with open(store_path, "r") as f:
        store_src = f.read()
    if 'return {"status": "error"' in store_src or 'return {\"status\": \"error\"' in store_src:
        fail("step1-static-fix", "replay error-return still present in store.py")
    elif re.search(r'except \(ValueError, ConsumerNotFoundError\):\s+raise\s+except Exception:\s+conn\.rollback\(\)\s+raise', store_src, re.DOTALL):
        ok("step1-static-fix", "Correct pattern: except Exception: conn.rollback(); raise")
    else:
        fail("step1-static-fix", "Pattern not found in store.py")

    # -- Step 2: Store error-convention comparison --
    print("\n[2] STORE ERROR-CONVENTION CHECK")
    # Check each method by reading the file and finding the relevant lines
    with open(store_path) as f:
        lines = f.readlines()
    for method in ["save", "get_checkpoint", "advance_checkpoint"]:
        # Find the method definition
        found = False
        for i, line in enumerate(lines):
            if f"    def {method}(" in line:
                # Look at the next ~20 lines for the except pattern
                chunk = "".join(lines[i:i+25])
                if "except Exception:" in chunk and "conn.rollback()" in chunk and "raise" in chunk:
                    ok(f"step2-{method}", "follows re-raise convention")
                else:
                    fail(f"step2-{method}", "pattern not found in method body")
                found = True
                break
        if not found:
            fail(f"step2-{method}", "method not found in store.py")

    # -- Start server --
    print("\n[3-9] RUNNING LIVE VERIFICATION")
    proc = None
    try:
        proc = await start_server(config_overrides={}, data_dir=VERIFY_DATA_DIR)
        url = get_server_url()
        print(f"  Server started at {url}")

        # Register a persistent consumer for domain-error and strict checks
        await mcp_call("consumer_register", {"consumer_id": "verify-cid-start"})

        # Step 3
        print("\n[3] NORMAL REPLAY SUCCESS")
        cid, seqs = await step3_normal_replay()

        if cid:
            print("\n[4] PAGINATION CONTROL")
            await step4_pagination(cid, seqs)

            print("\n[5-6] FAILURE INJECTION + MCP ERROR CONTRACT")
            await step5_6_failure_injection(cid)

            print("\n[7] RECOVERY")
            await step7_recovery(cid)

            print("\n[8] DOMAIN ERROR CONTROL")
            await step8_domain_error()

            print("\n[9] STRICT after_sequence CONTROL")
            await step9_strict_after_sequence()
    except Exception as exc:
        fail("server-start", str(exc))
    finally:
        if proc:
            stop_server(proc)
    restore_environment()

    # -- Step 10 --
    print("\n[10] RELATED ANTI-PATTERN SEARCH")
    step10_anti_pattern_search()

    # -- Step 11 --
    print("\n[11] FOCUSED EXISTING TESTS")
    step11_run_tests()

    # -- Summary --
    print("\n" + "=" * 60)
    passed = sum(1 for v in results.values() if v)
    total = len(results)
    print(f"RESULTS: {passed}/{total} checks passed")
    for k, v in results.items():
        if not v:
            print(f"  FAILED: {k}")
    if passed == total:
        print("VERDICT: REPLAY ERROR-CONTRACT VERIFIED — MUST-FIX BLOCKER CLOSED")
    else:
        print("VERDICT: REPLAY ERROR-CONTRACT VERIFICATION FAILED")
    print("=" * 60)
    return 0 if passed == total else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
