"""
Consumer registry and topic management.

Functions operate on an explicit sqlite3.Connection and are called
by EventStore methods during normal operation.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

from errors import ConsumerNotFoundError


def register_consumer(
    conn: sqlite3.Connection, consumer_id: str
) -> None:
    """
    Idempotently register a consumer.
    Also creates an initial checkpoint at sequence 0.
    """
    now = datetime.now(timezone.utc).isoformat()
    conn.execute("BEGIN")
    conn.execute(
        "INSERT INTO consumers (consumer_id, created_at, updated_at) "
        "VALUES (?, ?, ?) "
        "ON CONFLICT(consumer_id) DO UPDATE SET updated_at = excluded.updated_at",
        (consumer_id, now, now),
    )
    # Initialize checkpoint if not exists
    conn.execute(
        "INSERT OR IGNORE INTO consumer_checkpoints (consumer_id, last_sequence, updated_at) "
        "VALUES (?, 0, ?)",
        (consumer_id, now),
    )
    conn.commit()


def list_consumers(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        "SELECT consumer_id FROM consumers ORDER BY consumer_id"
    ).fetchall()
    return [r[0] for r in rows]


def add_topic(conn: sqlite3.Connection, consumer_id: str, topic: str) -> None:
    # Verify consumer exists before adding topic
    exists = conn.execute(
        "SELECT 1 FROM consumers WHERE consumer_id = ?", (consumer_id,)
    ).fetchone()
    if not exists:
        raise ConsumerNotFoundError(consumer_id)
    conn.execute("BEGIN")
    conn.execute(
        "INSERT OR IGNORE INTO consumer_topics (consumer_id, topic) VALUES (?, ?)",
        (consumer_id, topic),
    )
    conn.commit()


def get_consumer_topics(conn: sqlite3.Connection, consumer_id: str) -> set[str]:
    rows = conn.execute(
        "SELECT topic FROM consumer_topics WHERE consumer_id = ?", (consumer_id,)
    ).fetchall()
    return {r[0] for r in rows}
