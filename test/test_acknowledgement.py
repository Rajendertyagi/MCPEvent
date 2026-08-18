#!/usr/bin/env python3
"""
Acknowledgement and checkpoint tests.

OPTIMIZATION (2026-08): previously every test drove the MCP server
(``event_publish`` / ``consumer_event_acknowledge`` / ``consumer_checkpoint_get`` tool
calls). The checkpoint/ACK logic lives entirely in ``EventStore`` and
``events.publish_event``, so these tests now run DIRECTLY against an isolated
``EventStore`` + an injectable stub bus — ZERO server subprocesses, sub-second
runtime. Tool-boundary coverage for these paths is preserved by the D-level
files (e.g. test_sdk_alignment / test_events).

Extracted from integrate_test.py and test_phase8.py.

Run:
    python test/test_acknowledgement.py
"""

from __future__ import annotations

import asyncio
import os
import shutil
import sys
import tempfile

# Ensure project root is importable.
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_DIR = os.path.dirname(_SCRIPT_DIR)
for _p in (_PROJECT_DIR, _SCRIPT_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import events  # noqa: E402
from store import EventStore  # noqa: E402
from helpers.runner import R  # noqa: E402


class _StubBus:
    """Minimal subscription bus: records the last notification, does nothing else."""

    def __init__(self) -> None:
        self.last = None

    async def publish(self, item: object) -> None:
        self.last = item


async def _publish(store, bus, event_type: str, source: str = "test", routing=None) -> dict:
    """Publish a persistent event directly through the canonical path."""
    return await events.publish_event(
        event_type=event_type,
        source=source,
        data={},
        persistent=True,
        routing=routing,
        store=store,
        bus=bus,
    )


def _ack(store, cid: str, eid: str) -> None:
    """
    Faithfully replicate the MCP ``consumer_event_acknowledge`` tool (server.py:633 / 661-662).

    The tool does TWO things: marks the event acknowledged AND advances the
    consumer's durable checkpoint. A direct test that calls ``consumer_event_acknowledge``
    alone leaves ``get_checkpoint`` pinned at its initial value, so the checkpoint
    assertions below would never see advancement. We therefore chain the advance
    exactly like the real tool does.
    """
    store.acknowledge_event(cid, eid)
    store.advance_checkpoint(cid)


# ===================================================================
# Checkpoint Tests (CP1–CP6)
# ===================================================================


async def cp1_checkpoint_init(runner: R, store: EventStore, bus: _StubBus) -> None:
    name = "CP1-checkpoint-init"
    cid = "cp1-c1"
    store.register_consumer(cid)
    cp = store.get_checkpoint(cid)
    runner.assert_eq(name, cp, 0)


async def cp2_ack_advances(runner: R, store: EventStore, bus: _StubBus) -> None:
    name = "CP2-ack-advances"
    cid = "cp2-c2"
    store.register_consumer(cid)
    evts = [await _publish(store, bus, "test.cp") for _ in range(3)]
    _ack(store,cid, evts[0]["id"])
    cp = store.get_checkpoint(cid)
    runner.assert_eq(name, cp, evts[0]["sequence"])


async def cp3_gap_blocks(runner: R, store: EventStore, bus: _StubBus) -> None:
    name = "CP3-gap-blocks"
    cid = "cp3-c3"
    store.register_consumer(cid)
    evts = [await _publish(store, bus, "test.cp") for _ in range(3)]
    _ack(store,cid, evts[2]["id"])
    cp = store.get_checkpoint(cid)
    expected = evts[0]["sequence"] - 1
    runner.assert_eq(name, cp, expected)


async def cp4_fill_gap(runner: R, store: EventStore, bus: _StubBus) -> None:
    name = "CP4-fill-gap"
    cid = "cp4-c4"
    store.register_consumer(cid)
    evts = [await _publish(store, bus, "test.cp") for _ in range(3)]
    _ack(store,cid, evts[2]["id"])
    _ack(store,cid, evts[0]["id"])
    cp = store.get_checkpoint(cid)
    expected = evts[0]["sequence"]
    runner.assert_eq(name, cp, expected)


async def cp5_all_acked(runner: R, store: EventStore, bus: _StubBus) -> None:
    name = "CP5-all-acked"
    cid = "cp5-c5"
    store.register_consumer(cid)
    evts = [await _publish(store, bus, "test.cp") for _ in range(3)]
    max_seq = max(e["sequence"] for e in evts)
    for e in evts:
        _ack(store,cid, e["id"])
    cp = store.get_checkpoint(cid)
    runner.assert_eq(name, cp, max_seq)


async def cp6_monotonic(runner: R, store: EventStore, bus: _StubBus) -> None:
    name = "CP6-monotonic"
    cid = "cp6-c6"
    store.register_consumer(cid)
    evts = [await _publish(store, bus, "test.cp") for _ in range(2)]
    _ack(store,cid, evts[0]["id"])
    cp1 = store.get_checkpoint(cid)
    _ack(store,cid, evts[1]["id"])
    cp2 = store.get_checkpoint(cid)
    runner.assert_true(name, cp2 >= cp1, f"regression: {cp2} < {cp1}")


# ===================================================================
# Legacy ID Tests
# ===================================================================


async def t5_out_of_order_ack(runner: R, store: EventStore, bus: _StubBus) -> None:
    name = "T5-out-of-order-ack"
    cid = "t5"
    store.register_consumer(cid)
    evts = [await _publish(store, bus, "test.t5") for _ in range(3)]

    _ack(store,cid, evts[1]["id"])
    cp_after_2 = store.get_checkpoint(cid)
    expected_cp = evts[0]["sequence"] - 1
    runner.assert_eq(name + "-ack2-cp", cp_after_2, expected_cp)

    _ack(store,cid, evts[0]["id"])
    cp_after_0 = store.get_checkpoint(cid)
    runner.assert_true(name + "-ack0-cp", cp_after_0 >= evts[1]["sequence"],
                       f"expected >= {evts[1]['sequence']}, got {cp_after_0}")


async def t12_ack_clears_pending(runner: R, store: EventStore, bus: _StubBus) -> None:
    name = "T12-ack-clears"
    cid = "t12"
    store.register_consumer(cid)
    eid = (await _publish(store, bus, "test.t12"))["id"]

    pending_before = store.replay_events(cid, limit=20)
    ids_before = {e["id"] for e in pending_before.get("events", [])}
    runner.assert_true(name + "-before", eid in ids_before, "event not pending before ack")

    _ack(store,cid, eid)

    pending_after = store.replay_events(cid, limit=20)
    ids_after = {e["id"] for e in pending_after.get("events", [])}
    runner.assert_true(name + "-after", eid not in ids_after, "event still pending after ack")


async def p7t22_checkpoint_suite(runner: R, store: EventStore, bus: _StubBus) -> None:
    """
    Combined checkpoint suite.

    Runs the full CP1–CP6 sequence against an ISOLATED store. The standalone
    CP tests above share the main() store and deliberately leave some events
    unacknowledged (e.g. cp2 acks only evts[0]); replaying those same consumer
    IDs here would let stale unacked events clamp ``advance_checkpoint`` and
    produce sequence drift. A fresh DB per suite keeps each sub-test's
    expectations (computed from its own published sequences) valid.
    """
    name = "P7T22-checkpoint-suite"
    tmp = tempfile.mkdtemp(prefix="p7t22_")
    suite_db = os.path.join(tmp, "events.db")
    suite_store = EventStore(suite_db)
    suite_bus = _StubBus()
    try:
        await cp1_checkpoint_init(runner, suite_store, suite_bus)
        await cp2_ack_advances(runner, suite_store, suite_bus)
        await cp3_gap_blocks(runner, suite_store, suite_bus)
        await cp4_fill_gap(runner, suite_store, suite_bus)
        await cp5_all_acked(runner, suite_store, suite_bus)
        await cp6_monotonic(runner, suite_store, suite_bus)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


# ===================================================================
# Main
# ===================================================================


async def main() -> int:
    runner = R()
    tmp = tempfile.mkdtemp(prefix="ack_")
    db_path = os.path.join(tmp, "events.db")
    store = EventStore(db_path)
    bus = _StubBus()
    try:
        print("  Acknowledgement & Checkpoint Tests (direct, no server)")
        print("=" * 50)

        tests = [
            cp1_checkpoint_init,
            cp2_ack_advances,
            cp3_gap_blocks,
            cp4_fill_gap,
            cp5_all_acked,
            cp6_monotonic,
            t5_out_of_order_ack,
            t12_ack_clears_pending,
            p7t22_checkpoint_suite,
        ]
        for fn in tests:
            try:
                await fn(runner, store, bus)
            except Exception as exc:
                runner.fail(fn.__name__, str(exc))
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

    success = runner.summary()
    sys.exit(0 if success else 1)
    return success


if __name__ == "__main__":
    asyncio.run(main())
