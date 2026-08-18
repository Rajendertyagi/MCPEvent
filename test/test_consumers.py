#!/usr/bin/env python3
"""
Consumer/topic behavior tests — extracted from integrate_test.py.

OPTIMIZATION (2026-08): these tests previously went through the MCP server
(``consumer_register`` / ``consumer_topic_add`` / ``event_publish`` /
``consumer_event_pending_list`` tool calls). The consumer registry, topic routing and
replay logic all live in ``EventStore`` + ``events.publish_event``, so they now
run DIRECTLY against an isolated ``EventStore`` + stub bus — ZERO server
subprocesses. Tool-boundary coverage is preserved by the D-level files.

Covers:
  * Out-of-order ack — gap blocks, filling gap advances (T5)
  * Broadcast — two consumers get same event (T6)
  * Topic filtering (T7)
  * Topic assignment + targeted delivery (T11)
  * Broadcast semantics — publish-time consumers only (P7T2)
  * Subscription isolation — two consumers with different topics (P7T9)

Run:
    python test/test_consumers.py
"""

from __future__ import annotations

import asyncio
import os
import shutil
import sys
import tempfile

# Add project root and test dir to path
_PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_TEST_DIR = os.path.dirname(os.path.abspath(__file__))
for _p in (_PROJECT_DIR, _TEST_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import events  # noqa: E402
from store import EventStore  # noqa: E402
from helpers.runner import R  # noqa: E402


class _StubBus:
    def __init__(self) -> None:
        self.last = None

    async def publish(self, item: object) -> None:
        self.last = item


async def _publish(store, bus, event_type: str, routing=None) -> dict:
    return await events.publish_event(
        event_type=event_type,
        source="test",
        data={},
        persistent=True,
        routing=routing,
        store=store,
        bus=bus,
    )


def _ack(store, cid: str, eid: str) -> None:
    """Faithfully replicate the MCP consumer_event_acknowledge tool: ack AND advance checkpoint."""
    store.acknowledge_event(cid, eid)
    store.advance_checkpoint(cid)


async def t5_out_of_order_ack(runner: R, store: EventStore, bus: _StubBus) -> None:
    name = "T5-out-of-order-ack"
    cid = "t5"
    store.register_consumer(cid)
    evts = [await _publish(store, bus, "test.t5") for _ in range(3)]

    _ack(store, cid, evts[1]["id"])
    cp_after_2 = store.get_checkpoint(cid)
    expected_cp = evts[0]["sequence"] - 1
    runner.assert_eq(name + "-ack2-cp", cp_after_2, expected_cp)

    _ack(store, cid, evts[0]["id"])
    cp_after_0 = store.get_checkpoint(cid)
    runner.assert_true(name + "-ack0-cp", cp_after_0 >= evts[1]["sequence"],
                       f"expected >= {evts[1]['sequence']}, got {cp_after_0}")


async def t6_broadcast(runner: R, store: EventStore, bus: _StubBus) -> None:
    name = "T6-broadcast"
    cid_a = "t6a"
    cid_b = "t6b"
    store.register_consumer(cid_a)
    store.register_consumer(cid_b)
    eid = (await _publish(store, bus, "test.t6"))["id"]

    ids_a = {e["id"] for e in store.replay_events(cid_a, limit=20).get("events", [])}
    ids_b = {e["id"] for e in store.replay_events(cid_b, limit=20).get("events", [])}
    runner.assert_true(name + "-a", eid in ids_a, f"consumer A missing {eid}")
    runner.assert_true(name + "-b", eid in ids_b, f"consumer B missing {eid}")


async def t7_topic_filter(runner: R, store: EventStore, bus: _StubBus) -> None:
    name = "T7-topic-filter"
    cid_match = "t7m"
    cid_nomatch = "t7n"
    store.register_consumer(cid_match)
    store.register_consumer(cid_nomatch)
    store.add_topic(cid_match, "alpha")

    eid = (await _publish(store, bus, "test.t7", routing={"topics": ["alpha"]}))["id"]

    ids_m = {e["id"] for e in store.replay_events(cid_match, limit=20).get("events", [])}
    ids_n = {e["id"] for e in store.replay_events(cid_nomatch, limit=20).get("events", [])}
    runner.assert_true(name + "-match", eid in ids_m, "matching consumer missing event")
    runner.assert_true(name + "-nomatch", eid not in ids_n, "non-matching consumer got event")


async def t11_topic_targeted(runner: R, store: EventStore, bus: _StubBus) -> None:
    name = "T11-topic-targeted"
    cid_x = "t11x"
    cid_y = "t11y"
    store.register_consumer(cid_x)
    store.register_consumer(cid_y)
    store.add_topic(cid_x, "gpu")
    store.add_topic(cid_y, "cpu")

    eid = (await _publish(store, bus, "test.t11", routing={"topics": ["gpu"]}))["id"]

    ids_x = {e["id"] for e in store.replay_events(cid_x, limit=20).get("events", [])}
    ids_y = {e["id"] for e in store.replay_events(cid_y, limit=20).get("events", [])}
    runner.assert_true(name + "-x", eid in ids_x, "gpu consumer missing event")
    runner.assert_true(name + "-y", eid not in ids_y, "cpu consumer should not see gpu event")


async def p7t2_broadcast_semantics(runner: R, store: EventStore, bus: _StubBus) -> None:
    name = "P7T2-broadcast-semantics"
    cid_before = "p7t2before"
    cid_after = "p7t2after"
    store.register_consumer(cid_before)

    eid = (await _publish(store, bus, "test.p7t2"))["id"]

    # Register consumer AFTER publish — materialization already happened.
    store.register_consumer(cid_after)

    ids_b = {e["id"] for e in store.replay_events(cid_before, limit=20).get("events", [])}
    ids_a = {e["id"] for e in store.replay_events(cid_after, limit=20).get("events", [])}
    runner.assert_true(name + "-before", eid in ids_b, "pre-existing consumer should see event")
    runner.assert_true(name + "-after", eid not in ids_a, "post-publish consumer should not see event")


async def p7t9_subscription_isolation(runner: R, store: EventStore, bus: _StubBus) -> None:
    name = "P7T9-subscription-isolation"
    cid_a = "p7t9a"
    cid_b = "p7t9b"
    store.register_consumer(cid_a)
    store.register_consumer(cid_b)
    store.add_topic(cid_a, "topic-x")
    store.add_topic(cid_b, "topic-y")

    eid = (await _publish(store, bus, "test.p7t9", routing={"topics": ["topic-x"]}))["id"]

    ids_a = {e["id"] for e in store.replay_events(cid_a, limit=20).get("events", [])}
    ids_b = {e["id"] for e in store.replay_events(cid_b, limit=20).get("events", [])}
    runner.assert_true(name + "-a", eid in ids_a, "topic-x consumer missing event")
    runner.assert_true(name + "-b", eid not in ids_b, "topic-y consumer got cross-topic event")


# ===================================================================
# Main
# ===================================================================


async def main() -> int:
    runner = R()
    tmp = tempfile.mkdtemp(prefix="cons_")
    db_path = os.path.join(tmp, "events.db")
    store = EventStore(db_path)
    bus = _StubBus()
    try:
        print("  Consumer / Topic Tests (direct, no server)")
        print("=" * 50)

        tests = [
            t5_out_of_order_ack,
            t6_broadcast,
            t7_topic_filter,
            t11_topic_targeted,
            p7t2_broadcast_semantics,
            p7t9_subscription_isolation,
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
