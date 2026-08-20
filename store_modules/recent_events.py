"""
Durable recent-event observational journal.

This module owns ONLY the insert/query/prune logic for the `recent_events`
table. DDL lives in store_modules/schema.py (create_recent_events_table).

The journal is observational durability only: it records both persistent and
nonpersistent published events for bounded restart-safe inspection. It is NOT
consumer delivery state, NOT pending, NOT replay, NOT ACKable, and NOT
checkpoint input.
"""

from __future__ import annotations

import json
import sqlite3
from typing import Any


def append_recent_event(
    conn: sqlite3.Connection,
    event: dict[str, Any],
    capacity: int,
) -> None:
    """Append one published event to the recent journal and prune beyond capacity.

    Runs in a single short transaction. Pruning is deterministic: oldest rows
    by recent_sequence ASC are removed first.
    """
    data_json = json.dumps(event.get("data", {}), ensure_ascii=False)
    routing_json = (
        json.dumps(event.get("routing"), ensure_ascii=False)
        if event.get("routing") is not None
        else None
    )
    persistent = bool(event.get("persistent"))
    persistent_sequence = event.get("sequence")

    conn.execute("BEGIN IMMEDIATE")
    conn.execute(
        """
        INSERT INTO recent_events
            (event_id, type, source, timestamp, data_json,
             persistent, persistent_sequence, routing_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            event["id"],
            event["type"],
            event["source"],
            event["timestamp"],
            data_json,
            1 if persistent else 0,
            persistent_sequence,
            routing_json,
        ),
    )

    # Deterministic prune: drop oldest rows beyond capacity.
    if capacity and capacity > 0:
        cnt = conn.execute("SELECT COUNT(*) FROM recent_events").fetchone()[0]
        if cnt > capacity:
            excess = cnt - capacity
            conn.execute(
                """
                DELETE FROM recent_events
                WHERE recent_sequence IN (
                    SELECT recent_sequence FROM recent_events
                    ORDER BY recent_sequence ASC
                    LIMIT ?
                )
                """,
                (excess,),
            )
    conn.commit()


def list_recent_events(
    conn: sqlite3.Connection,
    limit: int,
    newest_first: bool = False,
) -> list[dict[str, Any]]:
    """Return reconstructed recent-event dicts.

    newest_first=False -> ORDER BY recent_sequence ASC (oldest -> newest)
    newest_first=True  -> ORDER BY recent_sequence DESC (newest -> oldest)
    """
    conn.row_factory = sqlite3.Row
    order = "DESC" if newest_first else "ASC"
    rows = conn.execute(
        """
        SELECT event_id, type, source, timestamp, data_json,
               persistent, persistent_sequence, routing_json
        FROM recent_events
        ORDER BY recent_sequence {order}
        LIMIT ?
        """.format(order=order),
        (limit,),
    ).fetchall()
    return [row_to_recent_event(r) for r in rows]


def row_to_recent_event(row: sqlite3.Row) -> dict[str, Any]:
    """Reconstruct a public event dict from a recent_events row."""
    event: dict[str, Any] = {
        "id": row["event_id"],
        "type": row["type"],
        "source": row["source"],
        "timestamp": row["timestamp"],
        "data": json.loads(row["data_json"]) if row["data_json"] else {},
        "persistent": bool(row["persistent"]),
    }
    if row["persistent_sequence"] is not None:
        event["sequence"] = row["persistent_sequence"]
    if row["routing_json"] is not None:
        event["routing"] = json.loads(row["routing_json"])
    return event
