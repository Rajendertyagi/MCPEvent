"""
Persistent event storage and routing/materialization logic.

Functions operate on an explicit sqlite3.Connection and are called
by EventStore methods during normal operation.
"""

from __future__ import annotations

import json
import sqlite3
from typing import Any

from errors import ConsumerNotFoundError


def save(
    conn: sqlite3.Connection,
    event_id: str,
    event_type: str,
    source: str,
    timestamp: str,
    data: dict[str, Any],
    routing: dict[str, Any] | None,
    materialize_fn,
) -> int:
    """
    Persist a single event and materialize per-consumer state rows.
    Returns the assigned SQLite sequence number.
    """
    data_json = json.dumps(data, ensure_ascii=False)
    routing_json = json.dumps(routing, ensure_ascii=False) if routing is not None else None

    conn.execute("BEGIN IMMEDIATE")
    conn.execute(
        "INSERT INTO persistent_events "
        "(id, type, source, timestamp, data, routing, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (event_id, event_type, source, timestamp, data_json, routing_json, timestamp),
    )
    seq = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    # Materialize per-consumer state for relevant consumers
    materialize_fn(conn, event_id, seq, routing)

    conn.commit()
    return seq


def materialize_event_state(
    conn: sqlite3.Connection,
    event_id: str,
    sequence: int,
    routing: dict[str, Any] | None,
    is_relevant_fn,
) -> None:
    """
    For each registered consumer, determine relevance and create a
    consumer_event_state row if relevant.

    Routing semantics (frozen at publication):
    - NULL routing = broadcast → relevant to ALL registered consumers
    - targets list → relevant to listed consumers
    - topics list → relevant to consumers whose topics intersect
    """
    # Get all registered consumers
    consumers = conn.execute(
        "SELECT consumer_id FROM consumers ORDER BY consumer_id"
    ).fetchall()

    if not consumers:
        return

    # Get all consumer topics in one query
    consumer_topics_map: dict[str, set[str]] = {}
    topic_rows = conn.execute(
        "SELECT consumer_id, topic FROM consumer_topics"
    ).fetchall()
    for cid, topic in topic_rows:
        consumer_topics_map.setdefault(cid, set()).add(topic)

    for (cid,) in consumers:
        topics = consumer_topics_map.get(cid, set())
        if is_relevant_fn(routing, cid, topics):
            # Create state row if not already exists (idempotent)
            conn.execute(
                "INSERT OR IGNORE INTO consumer_event_state "
                "(consumer_id, event_id) VALUES (?, ?)",
                (cid, event_id),
            )


def list_pending(conn: sqlite3.Connection, limit: int, row_to_event_fn) -> list[dict[str, Any]]:
    """Return the most recent persistent events, newest first."""
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT sequence, id, type, source, timestamp, data, routing, created_at "
        "FROM persistent_events "
        "ORDER BY created_at DESC "
        "LIMIT ?",
        (limit,),
    ).fetchall()
    return [row_to_event_fn(r) for r in rows]


def list_relevant_events(
    conn: sqlite3.Connection,
    consumer_id: str,
    after_sequence: int | None,
    limit: int,
    max_replay_limit: int,
    row_to_event_fn,
) -> list[dict[str, Any]]:
    """
    Return persistent events relevant to a consumer, ordered by sequence ascending.
    Uses materialized consumer_event_state for relevance filtering.
    """
    conn.row_factory = sqlite3.Row

    # Verify consumer exists
    exist = conn.execute(
        "SELECT 1 FROM consumers WHERE consumer_id = ?", (consumer_id,)
    ).fetchone()
    if not exist:
        raise ConsumerNotFoundError(consumer_id)

    # Build query: join with consumer_event_state to get only relevant events
    # Filter out acknowledged events (already processed)
    where = "WHERE ces.consumer_id = ? AND ces.acknowledged_at IS NULL"
    params: list[Any] = [consumer_id]
    if after_sequence is not None:
        where += " AND pe.sequence > ?"
        params.append(after_sequence)
    where += " ORDER BY pe.sequence ASC LIMIT ?"
    params.append(min(limit, max_replay_limit))

    rows = conn.execute("""
        SELECT pe.sequence, pe.id, pe.type, pe.source, pe.timestamp,
               pe.data, pe.routing, pe.created_at
        FROM persistent_events pe
        JOIN consumer_event_state ces ON ces.event_id = pe.id
        {where}
    """.format(where=where), params).fetchall()

    return [row_to_event_fn(r) for r in rows]


def count(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT COUNT(*) FROM persistent_events").fetchone()
    return row[0] if row else 0


def row_to_event(row: sqlite3.Row) -> dict[str, Any]:
    routing = json.loads(row["routing"]) if row["routing"] else None
    return {
        "sequence": row["sequence"],
        "id": row["id"],
        "type": row["type"],
        "source": row["source"],
        "timestamp": row["timestamp"],
        "data": json.loads(row["data"]),
        "routing": routing,
        "created_at": row["created_at"],
    }


def is_event_relevant(
    routing: dict[str, Any] | None,
    consumer_id: str,
    consumer_topics: set[str],
) -> bool:
    """
    Determine whether an event is relevant to a consumer.
    Used for non-materialized queries (e.g. list_relevant_events with custom filters).
    """
    if routing is None:
        return True

    targets = routing.get("targets", [])
    if consumer_id in targets:
        return True

    topics = routing.get("topics", [])
    if topics and set(topics) & consumer_topics:
        return True

    return False


def is_event_relevant_internal(
    routing: dict[str, Any] | None,
    consumer_id: str,
    consumer_topics: set[str],
) -> bool:
    """Internal relevance check used during materialization."""
    return is_event_relevant(routing, consumer_id, consumer_topics)
