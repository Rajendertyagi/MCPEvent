"""
Source state (durable cursors) and source deduplication persistence.

These functions operate on an explicit sqlite3.Connection and are called
by EventStore methods during normal operation.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone


def get_source_state(conn: sqlite3.Connection, source_name: str, key: str) -> str | None:
    """Read a single source-state value. Returns None if not set."""
    row = conn.execute(
        "SELECT value FROM source_state WHERE source_name = ? AND key = ?",
        (source_name, key),
    ).fetchone()
    return row[0] if row else None


def set_source_state(
    conn: sqlite3.Connection, source_name: str, key: str, value: str
) -> None:
    """Write a source-state value (upsert)."""
    now = datetime.now(timezone.utc).isoformat()
    conn.execute("BEGIN")
    conn.execute(
        "INSERT INTO source_state (source_name, key, value, updated_at) "
        "VALUES (?, ?, ?, ?) "
        "ON CONFLICT(source_name, key) DO UPDATE SET "
        "  value = excluded.value, updated_at = excluded.updated_at",
        (source_name, key, value, now),
    )
    conn.commit()


def get_all_source_state(conn: sqlite3.Connection, source_name: str) -> dict[str, str]:
    """Read all key-value pairs for a source."""
    rows = conn.execute(
        "SELECT key, value FROM source_state WHERE source_name = ?",
        (source_name,),
    ).fetchall()
    return {r[0]: r[1] for r in rows}


def source_item_seen(
    conn: sqlite3.Connection, source_name: str, external_id: str
) -> bool:
    """
    Return True if (source_name, external_id) was already marked as seen.

    Used by sources for durable, restart-safe deduplication. The key is the
    composite (source_name, external_id) so two different sources may each
    track the same external ID independently.
    """
    row = conn.execute(
        "SELECT 1 FROM source_seen_items WHERE source_name = ? AND external_id = ?",
        (source_name, external_id),
    ).fetchone()
    return row is not None


def mark_source_item_seen(
    conn: sqlite3.Connection, source_name: str, external_id: str, seen_at: str
) -> None:
    """Record an external ID as seen (idempotent upsert)."""
    conn.execute("BEGIN")
    conn.execute(
        "INSERT OR IGNORE INTO source_seen_items "
        "(source_name, external_id, seen_at) VALUES (?, ?, ?)",
        (source_name, external_id, seen_at),
    )
    conn.commit()


def prune_source_seen_items(
    conn: sqlite3.Connection, source_name: str, max_items: int
) -> int:
    """
    Delete the oldest seen IDs for a source when over the configured limit.

    Keeps the most recent ``max_items`` rows (ordered by seen_at, then rowid).
    Returns the number of rows deleted. No-op when already at/under the limit.
    """
    if max_items < 1:
        max_items = 1
    conn.execute("BEGIN IMMEDIATE")
    cnt = conn.execute(
        "SELECT COUNT(*) FROM source_seen_items WHERE source_name = ?",
        (source_name,),
    ).fetchone()[0]
    deleted = 0
    if cnt > max_items:
        excess = cnt - max_items
        conn.execute(
            """
            DELETE FROM source_seen_items
            WHERE source_name = ?
              AND rowid IN (
                SELECT rowid FROM source_seen_items
                WHERE source_name = ?
                ORDER BY seen_at ASC, rowid ASC
                LIMIT ?
              )
            """,
            (source_name, source_name, excess),
        )
        deleted = excess
    conn.commit()
    return deleted
