"""
Checkpoint management and event replay persistence.

Functions operate on an explicit sqlite3.Connection and are called
by EventStore methods during normal operation.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone
from typing import Any

from errors import ConsumerNotFoundError

logger = logging.getLogger(__name__)


def get_checkpoint(conn: sqlite3.Connection, consumer_id: str) -> int:
    """Return the consumer's current checkpoint sequence (0 if registered)."""
    row = conn.execute(
        "SELECT last_sequence FROM consumer_checkpoints WHERE consumer_id = ?",
        (consumer_id,),
    ).fetchone()
    if row is None:
        raise ConsumerNotFoundError(consumer_id)
    return row[0]


def advance_checkpoint(conn: sqlite3.Connection, consumer_id: str) -> int:
    """
    Advance the consumer's checkpoint to the highest safe sequence.

    Safe sequence = the highest sequence N such that there is no relevant
    unacknowledged persistent event with sequence <= N.

    Irrelevant events (not in consumer_event_state for this consumer) are
    skipped — they don't block checkpoint advancement.

    Algorithm:
      1. Find the first unacknowledged relevant event AFTER current checkpoint.
      2. If found at sequence N, candidate = N - 1.
      3. If not found, candidate = max(sequence) for this consumer.
      4. new_checkpoint = MAX(current, candidate) — monotonic guard.
    """
    conn.execute("BEGIN IMMEDIATE")

    current = conn.execute(
        "SELECT last_sequence FROM consumer_checkpoints WHERE consumer_id = ?",
        (consumer_id,),
    ).fetchone()
    if not current:
        conn.rollback()
        return 0

    from_seq = current[0]

    # Find the first unacknowledged relevant event after current checkpoint
    next_unacked = conn.execute("""
        SELECT MIN(pe.sequence)
        FROM persistent_events pe
        JOIN consumer_event_state ces ON ces.event_id = pe.id
        WHERE ces.consumer_id = ?
          AND pe.sequence > ?
          AND ces.acknowledged_at IS NULL
        ORDER BY pe.sequence ASC
        LIMIT 1
    """, (consumer_id, from_seq)).fetchone()

    if next_unacked and next_unacked[0] is not None:
        # Advance to just before the first unacknowledged event
        candidate = next_unacked[0] - 1
    else:
        # All relevant events up to the max are acknowledged
        max_seq = conn.execute("""
            SELECT MAX(pe.sequence)
            FROM persistent_events pe
            JOIN consumer_event_state ces ON ces.event_id = pe.id
            WHERE ces.consumer_id = ?
        """, (consumer_id,)).fetchone()
        candidate = max_seq[0] if max_seq and max_seq[0] is not None else from_seq

    # Monotonic guard: never regress
    new_checkpoint = max(from_seq, candidate)

    if new_checkpoint > from_seq:
        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            "UPDATE consumer_checkpoints SET last_sequence = ?, updated_at = ? WHERE consumer_id = ?",
            (new_checkpoint, now, consumer_id),
        )
        conn.commit()
        logger.debug("checkpoint advanced for %s: %d -> %d", consumer_id, from_seq, new_checkpoint)
        return new_checkpoint
    else:
        conn.commit()
        return from_seq


def replay_events(
    conn: sqlite3.Connection,
    consumer_id: str,
    limit: int,
    max_replay_limit: int,
    row_to_event,
    after_sequence: int | None = None,
) -> dict[str, Any]:
    """
    Replay events for a consumer starting from their durable checkpoint,
    or from an explicit after_sequence if provided.

    If after_sequence is None, falls back to the consumer's stored checkpoint.
    """
    conn.row_factory = sqlite3.Row

    # Verify consumer
    consumer = conn.execute(
        "SELECT 1 FROM consumers WHERE consumer_id = ?", (consumer_id,)
    ).fetchone()
    if not consumer:
        raise ConsumerNotFoundError(consumer_id)

    # Determine starting sequence
    if after_sequence is not None:
        after_seq = after_sequence
    else:
        cp = conn.execute(
            "SELECT last_sequence FROM consumer_checkpoints WHERE consumer_id = ?",
            (consumer_id,),
        ).fetchone()
        after_seq = cp[0] if cp else 0

    # Fetch relevant unacknowledged events
    effective_limit = min(limit, max_replay_limit)
    rows = conn.execute("""
        SELECT pe.sequence, pe.id, pe.type, pe.source, pe.timestamp,
               pe.data, pe.routing, pe.created_at
        FROM persistent_events pe
        JOIN consumer_event_state ces ON ces.event_id = pe.id
        WHERE ces.consumer_id = ?
          AND pe.sequence > ?
          AND ces.acknowledged_at IS NULL
        ORDER BY pe.sequence ASC
        LIMIT ?
    """, (consumer_id, after_seq, effective_limit)).fetchall()

    events = [row_to_event(r) for r in rows]

    # Mark all returned events as delivered (preserving first delivery time)
    now = datetime.now(timezone.utc).isoformat()
    for event in events:
        conn.execute(
            "INSERT INTO consumer_event_state (consumer_id, event_id, delivered_at) "
            "VALUES (?, ?, ?) "
            "ON CONFLICT(consumer_id, event_id) DO UPDATE SET "
            "  delivered_at = CASE WHEN consumer_event_state.delivered_at IS NULL "
            "                       THEN excluded.delivered_at "
            "                       ELSE consumer_event_state.delivered_at END",
            (consumer_id, event["id"], now),
        )
    conn.commit()

    # Compute next_after_sequence for pagination
    next_after = events[-1]["sequence"] if events else after_seq
    has_more = len(events) == effective_limit

    return {
        "consumer_id": consumer_id,
        "checkpoint": after_seq,
        "returned": len(events),
        "has_more": has_more,
        "next_after_sequence": next_after,
        "events": events,
    }
