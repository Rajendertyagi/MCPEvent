"""
Per-consumer delivery state and acknowledgement persistence.

Functions operate on an explicit sqlite3.Connection and are called
by EventStore methods during normal operation.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

from errors import ConsumerNotFoundError


def mark_delivered(
    conn: sqlite3.Connection, consumer_id: str, event_id: str
) -> None:
    """
    Mark an event as delivered to a consumer. Preserves first delivery time.
    """
    now = datetime.now(timezone.utc).isoformat()
    conn.execute("BEGIN")
    conn.execute(
        "INSERT INTO consumer_event_state (consumer_id, event_id, delivered_at) "
        "VALUES (?, ?, ?) "
        "ON CONFLICT(consumer_id, event_id) DO UPDATE SET "
        "  delivered_at = CASE WHEN consumer_event_state.delivered_at IS NULL "
        "                       THEN excluded.delivered_at "
        "                       ELSE consumer_event_state.delivered_at END",
        (consumer_id, event_id, now),
    )
    conn.commit()


def acknowledge_event(
    conn: sqlite3.Connection, consumer_id: str, event_id: str
) -> bool:
    """
    Acknowledge an event for a consumer. Idempotent — preserves first ack time.
    Returns True if the event was acknowledged (or was already acknowledged).
    Raises ValueError if consumer or event doesn't exist, or event is not relevant.
    """
    now = datetime.now(timezone.utc).isoformat()
    conn.execute("BEGIN IMMEDIATE")

    # Verify consumer exists
    consumer = conn.execute(
        "SELECT 1 FROM consumers WHERE consumer_id = ?", (consumer_id,)
    ).fetchone()
    if not consumer:
        conn.rollback()
        raise ConsumerNotFoundError(consumer_id)

    # Verify event exists and is persistent
    evt = conn.execute(
        "SELECT 1 FROM persistent_events WHERE id = ?", (event_id,)
    ).fetchone()
    if not evt:
        conn.rollback()
        raise ValueError(f"event not found: {event_id}")

    # Verify the event is in this consumer's state (relevant)
    state = conn.execute(
        "SELECT 1 FROM consumer_event_state WHERE consumer_id = ? AND event_id = ?",
        (consumer_id, event_id),
    ).fetchone()
    if not state:
        conn.rollback()
        raise ValueError(
            f"event {event_id} is not relevant to consumer {consumer_id}"
        )

    # Mark acknowledged — preserve first ack time
    conn.execute(
        "UPDATE consumer_event_state SET "
        "  acknowledged_at = CASE WHEN acknowledged_at IS NULL THEN ? ELSE acknowledged_at END "
        "WHERE consumer_id = ? AND event_id = ?",
        (now, consumer_id, event_id),
    )

    conn.commit()
    return True


def get_delivered_event_ids(
    conn: sqlite3.Connection, consumer_id: str
) -> set[str]:
    rows = conn.execute(
        "SELECT event_id FROM consumer_event_state WHERE consumer_id = ?", (consumer_id,)
    ).fetchall()
    return {r[0] for r in rows}
