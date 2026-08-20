"""
Alert persistence layer — SQLite operations for alert definitions.

Owns: INSERT, SELECT, UPDATE, and row conversion for the `alerts` table.
Does NOT own DDL (see store_modules/schema.py) or alert-matching/business logic
(see server_modules/alerts.py).

Functions operate on an explicit sqlite3.Connection and are called by the
EventStore facade (store.py) during normal operation.
"""

from __future__ import annotations

import json
import sqlite3
from typing import Any

from errors import AlertNotFoundError, ConsumerNotFoundError


def insert_alert(
    conn: sqlite3.Connection,
    alert_id: str,
    consumer_id: str,
    name: str | None,
    source: str,
    event_type: str | None,
    field_path: str,
    operator: str,
    value_json: str,
    one_shot: bool,
    now: str,
) -> None:
    """Persist a new alert definition. Raises ConsumerNotFoundError if consumer missing."""
    consumer = conn.execute(
        "SELECT 1 FROM consumers WHERE consumer_id = ?", (consumer_id,)
    ).fetchone()
    if not consumer:
        raise ConsumerNotFoundError(consumer_id)

    conn.execute("BEGIN")
    try:
        conn.execute(
            """
            INSERT INTO alerts
                (alert_id, consumer_id, name, source, event_type,
                 field_path, operator, value_json, enabled, one_shot,
                 created_at, updated_at, last_triggered_at, trigger_count)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, NULL, 0)
            """,
            (
                alert_id,
                consumer_id,
                name,
                source,
                event_type,
                field_path,
                operator,
                value_json,
                1 if one_shot else 0,
                now,
                now,
            ),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def list_alerts_by_consumer(
    conn: sqlite3.Connection,
    consumer_id: str,
    enabled_filter: bool | None = None,
) -> list[dict[str, Any]]:
    """List alerts owned by a consumer, optionally filtered by enabled state.

    Ordered by created_at ASC (deterministic).
    """
    where = "WHERE consumer_id = ?"
    params: list[Any] = [consumer_id]
    if enabled_filter is not None:
        where += " AND enabled = ?"
        params.append(1 if enabled_filter else 0)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        f"SELECT * FROM alerts {where} ORDER BY created_at ASC", params
    ).fetchall()
    conn.row_factory = None
    return [row_to_alert_dict(r) for r in rows]


def list_alerts_by_source_enabled(
    conn: sqlite3.Connection,
    source: str,
) -> list[dict[str, Any]]:
    """Evaluation candidate query: enabled alerts for a given source."""
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT * FROM alerts WHERE source = ? AND enabled = 1", (source,)
    ).fetchall()
    conn.row_factory = None
    return [row_to_alert_dict(r) for r in rows]


def get_alert(
    conn: sqlite3.Connection,
    consumer_id: str,
    alert_id: str,
) -> dict[str, Any] | None:
    """Return a single alert, ownership-checked. Returns None if not found."""
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT * FROM alerts WHERE consumer_id = ? AND alert_id = ?",
        (consumer_id, alert_id),
    ).fetchone()
    conn.row_factory = None
    return row_to_alert_dict(row) if row else None


def update_alert_enabled(
    conn: sqlite3.Connection,
    consumer_id: str,
    alert_id: str,
    enabled: bool,
    now: str,
) -> bool:
    """Enable/disable an alert. Returns True only if state actually changed.

    Raises ConsumerNotFoundError for unknown consumer,
    AlertNotFoundError for unknown/non-owned alert.
    """
    consumer = conn.execute(
        "SELECT 1 FROM consumers WHERE consumer_id = ?", (consumer_id,)
    ).fetchone()
    if not consumer:
        raise ConsumerNotFoundError(consumer_id)

    row = conn.execute(
        "SELECT enabled FROM alerts WHERE consumer_id = ? AND alert_id = ?",
        (consumer_id, alert_id),
    ).fetchone()
    if not row:
        raise AlertNotFoundError(alert_id)

    current = bool(row[0])
    if current == enabled:
        return False  # idempotent — no change

    conn.execute("BEGIN")
    try:
        conn.execute(
            "UPDATE alerts SET enabled = ?, updated_at = ? "
            "WHERE consumer_id = ? AND alert_id = ?",
            (1 if enabled else 0, now, consumer_id, alert_id),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return True


def record_alert_trigger(
    conn: sqlite3.Connection,
    alert_id: str,
    now: str,
) -> None:
    """Atomic post-success trigger state update.

    Increments trigger_count, sets last_triggered_at and updated_at, and
    disables the alert if one_shot. Raises AlertNotFoundError if missing.
    """
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute(
            "SELECT one_shot FROM alerts WHERE alert_id = ?", (alert_id,)
        ).fetchone()
        if not row:
            raise AlertNotFoundError(alert_id)

        one_shot = bool(row[0])
        if one_shot:
            conn.execute(
                """
                UPDATE alerts SET
                    trigger_count = trigger_count + 1,
                    last_triggered_at = ?,
                    updated_at = ?,
                    enabled = 0
                WHERE alert_id = ?
                """,
                (now, now, alert_id),
            )
        else:
            conn.execute(
                """
                UPDATE alerts SET
                    trigger_count = trigger_count + 1,
                    last_triggered_at = ?,
                    updated_at = ?
                WHERE alert_id = ?
                """,
                (now, now, alert_id),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def row_to_alert_dict(row: sqlite3.Row) -> dict[str, Any]:
    """Convert a alerts-table row into the public alert dict (parses value_json)."""
    return {
        "alert_id": row["alert_id"],
        "consumer_id": row["consumer_id"],
        "name": row["name"],
        "source": row["source"],
        "event_type": row["event_type"],
        "field_path": row["field_path"],
        "operator": row["operator"],
        "value": json.loads(row["value_json"]) if row["value_json"] is not None else None,
        "enabled": bool(row["enabled"]),
        "one_shot": bool(row["one_shot"]),
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "last_triggered_at": row["last_triggered_at"],
        "trigger_count": row["trigger_count"],
    }
