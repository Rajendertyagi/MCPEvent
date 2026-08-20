"""
SQLite persistence layer for persistent alerts.

Separate from MCP transport — this module knows nothing about subscriptions,
resources, or the MCP protocol. It only handles durable storage of events,
consumers, routing, acknowledgements, and checkpoints.

Schema evolution:
  v1 — id TEXT PRIMARY KEY, type, source, timestamp, data, created_at
  v2 — sequence INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT UNIQUE
  v3 — add routing JSON column + consumers/consumer_topics/consumer_event_state
  v4 — remove redundant sequence from consumer_event_state, add FK to event_id
  v5 — add consumer_checkpoints, materialize per-consumer state at publish time
  v6 — add source_state (durable source cursors)
  v7 — add source_seen_items (durable restart-safe source deduplication)
"""

from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from store_modules.schema import (
    create_v3_schema_partial,
    create_v7_schema,
    get_schema_version,
    migrate_v1_to_v3,
    migrate_v2_to_v3,
    migrate_v3_to_v4,
    migrate_v4_to_v5,
    migrate_v5_to_v6,
    migrate_v6_to_v7,
    migrate_v7_to_v8,
    migrate_v8_to_v9,
    SCHEMA_VERSION,
)
from store_modules import alerts as _alerts
from store_modules import consumers as _consumers
from store_modules import delivery as _delivery
from store_modules import events as _events
from store_modules import recent_events as _recent_events
from store_modules import replay as _replay
from store_modules import source_state as _source_state
from errors import ConsumerNotFoundError

logger = logging.getLogger(__name__)

# Maximum events a single replay/GetPending call can return.
MAX_REPLAY_LIMIT = 500


class EventStore:
    """Thread-safe SQLite backend for persistent events, consumers, routing, and checkpoints."""

    def __init__(self, db_path: str) -> None:
        self._db_path = str(Path(db_path).resolve())
        self._ensure_directory()
        self._init_db()

    # ─── Connection helper ────────────────────────────────────────────────────

    @staticmethod
    def _open(db_path: str) -> sqlite3.Connection:
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=5000")
        return conn

    def _ensure_directory(self) -> None:
        Path(self._db_path).parent.mkdir(parents=True, exist_ok=True)

    # ─── Schema initialization & migration ───────────────────────────────────

    def _init_db(self) -> None:
        conn = self._open(self._db_path)
        try:
            current_version = get_schema_version(conn)

            if current_version == 0:
                # Fresh database: create the full current schema and set version
                create_v7_schema(conn)
                conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
                conn.commit()
                logger.info("event store initialized (fresh v%d): %s", SCHEMA_VERSION, self._db_path)
            elif current_version < SCHEMA_VERSION:
                # Migrate sequentially: one version at a time, re-reading version each step
                while current_version < SCHEMA_VERSION:
                    if current_version == 1:
                        migrate_v1_to_v3(conn)
                    elif current_version == 2:
                        migrate_v2_to_v3(conn)
                    elif current_version == 3:
                        migrate_v3_to_v4(conn)
                    elif current_version == 4:
                        migrate_v4_to_v5(conn)
                    elif current_version == 5:
                        migrate_v5_to_v6(conn)
                    elif current_version == 6:
                        migrate_v6_to_v7(conn)
                    elif current_version == 7:
                        migrate_v7_to_v8(conn)
                    elif current_version == 8:
                        migrate_v8_to_v9(conn)
                    else:
                        raise RuntimeError(
                            f"unsupported schema version {current_version}; "
                            f"expected between 1 and {SCHEMA_VERSION - 1}"
                        )
                    # Re-read version after migration (each migration sets its own target)
                    current_version = get_schema_version(conn)
                logger.info("event store migrated to v%d: %s", SCHEMA_VERSION, self._db_path)
            elif current_version == SCHEMA_VERSION:
                # Already current — ensure indexes exist (defensive idempotency)
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idxCES_ack
                    ON consumer_event_state(consumer_id, acknowledged_at)
                """)
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idxCES_consumer
                    ON consumer_event_state(consumer_id, event_id)
                """)
                conn.commit()
                logger.info("event store ready (v%d): %s", current_version, self._db_path)
            else:
                raise RuntimeError(
                    f"schema version {current_version} exceeds supported max {SCHEMA_VERSION}; "
                    "upgrade the application first"
                )
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    # ─── Event persistence with materialization ───────────────────────────────

    def save(
        self,
        event_id: str,
        event_type: str,
        source: str,
        timestamp: str,
        data: dict[str, Any],
        routing: dict[str, Any] | None = None,
    ) -> int:
        """
        Persist a single event and materialize per-consumer state rows.
        Returns the assigned SQLite sequence number.
        """
        created_at = datetime.now(timezone.utc).isoformat()
        conn = self._open(self._db_path)
        try:
            return _events.save(
                conn, event_id, event_type, source, timestamp, data, routing,
                lambda c, eid, seq, rt: _events.materialize_event_state(
                    c, eid, seq, rt, self.is_event_relevant_internal),
            )
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    @staticmethod
    def is_event_relevant_internal(
        routing: dict[str, Any] | None,
        consumer_id: str,
        consumer_topics: set[str],
    ) -> bool:
        """Internal relevance check used during materialization."""
        return _events.is_event_relevant_internal(routing, consumer_id, consumer_topics)

    # ─── Query helpers ────────────────────────────────────────────────────────

    def list_pending(self, limit: int = 100) -> list[dict[str, Any]]:
        """Return the most recent persistent events, newest first."""
        conn = self._open(self._db_path)
        try:
            return _events.list_pending(conn, limit, _events.row_to_event)
        finally:
            conn.close()

    def list_relevant_events(
        self,
        consumer_id: str,
        after_sequence: int | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """
        Return persistent events relevant to a consumer, ordered by sequence ascending.
        Uses materialized consumer_event_state for relevance filtering.
        """
        conn = self._open(self._db_path)
        try:
            return _events.list_relevant_events(
                conn, consumer_id, after_sequence, limit, MAX_REPLAY_LIMIT,
                _events.row_to_event,
            )
        finally:
            conn.close()

    def count(self) -> int:
        conn = self._open(self._db_path)
        try:
            return _events.count(conn)
        finally:
            conn.close()

    # ─── Consumer registry ────────────────────────────────────────────────────

    def register_consumer(self, consumer_id: str) -> None:
        """
        Idempotently register a consumer.
        Also creates an initial checkpoint at sequence 0.
        """
        conn = self._open(self._db_path)
        try:
            _consumers.register_consumer(conn, consumer_id)
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def list_consumers(self) -> list[str]:
        conn = self._open(self._db_path)
        try:
            return _consumers.list_consumers(conn)
        finally:
            conn.close()

    def add_topic(self, consumer_id: str, topic: str) -> None:
        conn = self._open(self._db_path)
        try:
            _consumers.add_topic(conn, consumer_id, topic)
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def get_consumer_topics(self, consumer_id: str) -> set[str]:
        conn = self._open(self._db_path)
        try:
            return _consumers.get_consumer_topics(conn, consumer_id)
        finally:
            conn.close()

    # ─── Per-consumer event state ─────────────────────────────────────────────

    def mark_delivered(self, consumer_id: str, event_id: str) -> None:
        """
        Mark an event as delivered to a consumer. Preserves first delivery time.
        """
        conn = self._open(self._db_path)
        try:
            _delivery.mark_delivered(conn, consumer_id, event_id)
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def acknowledge_event(self, consumer_id: str, event_id: str) -> bool:
        """
        Acknowledge an event for a consumer. Idempotent — preserves first ack time.
        Returns True if the event was acknowledged (or was already acknowledged).
        Raises ValueError if consumer or event doesn't exist, or event is not relevant.
        """
        conn = self._open(self._db_path)
        try:
            return _delivery.acknowledge_event(conn, consumer_id, event_id)
        except (ValueError, sqlite3.Error, ConsumerNotFoundError):
            conn.rollback()
            raise
        finally:
            conn.close()

    def get_delivered_event_ids(self, consumer_id: str) -> set[str]:
        conn = self._open(self._db_path)
        try:
            return _delivery.get_delivered_event_ids(conn, consumer_id)
        finally:
            conn.close()

    # ─── Checkpoint management ────────────────────────────────────────────────

    def get_checkpoint(self, consumer_id: str) -> int:
        """Return the consumer's current checkpoint sequence (0 if registered)."""
        conn = self._open(self._db_path)
        try:
            return _replay.get_checkpoint(conn, consumer_id)
        except (ValueError, ConsumerNotFoundError):
            raise
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def advance_checkpoint(self, consumer_id: str) -> int:
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
        conn = self._open(self._db_path)
        try:
            return _replay.advance_checkpoint(conn, consumer_id)
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def replay_events(
        self,
        consumer_id: str,
        limit: int = 50,
        after_sequence: int | None = None,
    ) -> dict[str, Any]:
        """
        Replay events for a consumer starting from their durable checkpoint.

        Returns events that are:
        - Relevant to the consumer (via materialized consumer_event_state)
        - After the consumer's checkpoint (or after_sequence if provided)
        - Not yet acknowledged
        - Ordered by sequence ASC
        """
        conn = self._open(self._db_path)
        try:
            result = _replay.replay_events(
                conn, consumer_id, limit, MAX_REPLAY_LIMIT,
                _events.row_to_event, after_sequence,
            )
            return result
        except (ValueError, ConsumerNotFoundError):
            raise
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    # ─── Routing decision ─────────────────────────────────────────────────────

    @staticmethod
    def is_event_relevant(
        event: dict[str, Any],
        consumer_id: str,
        consumer_topics: set[str],
    ) -> bool:
        """
        Determine whether an event is relevant to a consumer.
        Used for non-materialized queries (e.g. list_relevant_events with custom filters).
        """
        return _events.is_event_relevant(
            event.get("routing"), consumer_id, consumer_topics)

    # ─── Alert definitions (generic alert engine) ────────────────────────────

    def create_alert(
        self,
        alert_id: str,
        consumer_id: str,
        name: str | None,
        source: str,
        event_type: str | None,
        field_path: str,
        operator: str,
        value: Any,
        one_shot: bool,
    ) -> dict[str, Any]:
        """Persist a new alert definition and return it. Opens/commits its own txn."""
        now = datetime.now(timezone.utc).isoformat()
        conn = self._open(self._db_path)
        try:
            _alerts.insert_alert(
                conn, alert_id, consumer_id, name, source, event_type,
                field_path, operator, json.dumps(value, ensure_ascii=False), one_shot, now,
            )
            return _alerts.get_alert(conn, consumer_id, alert_id)
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def list_alerts(
        self, consumer_id: str, enabled: bool | None = None
    ) -> list[dict[str, Any]]:
        """List alerts owned by a consumer (optional enabled filter)."""
        conn = self._open(self._db_path)
        try:
            return _alerts.list_alerts_by_consumer(conn, consumer_id, enabled)
        finally:
            conn.close()

    def list_alerts_by_source_enabled(self, source: str) -> list[dict[str, Any]]:
        """Evaluation candidate query: enabled alerts for a source."""
        conn = self._open(self._db_path)
        try:
            return _alerts.list_alerts_by_source_enabled(conn, source)
        finally:
            conn.close()

    def get_alert(self, consumer_id: str, alert_id: str) -> dict[str, Any] | None:
        """Return a single alert, ownership-checked. None if not found."""
        conn = self._open(self._db_path)
        try:
            return _alerts.get_alert(conn, consumer_id, alert_id)
        finally:
            conn.close()

    def enable_alert(self, consumer_id: str, alert_id: str) -> bool:
        """Enable an alert. Returns True only if state actually changed."""
        now = datetime.now(timezone.utc).isoformat()
        conn = self._open(self._db_path)
        try:
            return _alerts.update_alert_enabled(conn, consumer_id, alert_id, True, now)
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def disable_alert(self, consumer_id: str, alert_id: str) -> bool:
        """Disable an alert. Returns True only if state actually changed."""
        now = datetime.now(timezone.utc).isoformat()
        conn = self._open(self._db_path)
        try:
            return _alerts.update_alert_enabled(conn, consumer_id, alert_id, False, now)
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def record_alert_trigger(self, alert_id: str) -> None:
        """Atomically record a successful alert trigger (state update)."""
        now = datetime.now(timezone.utc).isoformat()
        conn = self._open(self._db_path)
        try:
            _alerts.record_alert_trigger(conn, alert_id, now)
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    # ─── Source state (durable cursors) ──────────────────────────────────────

    def get_source_state(self, source_name: str, key: str) -> str | None:
        """Read a single source-state value. Returns None if not set."""
        conn = self._open(self._db_path)
        try:
            return _source_state.get_source_state(conn, source_name, key)
        finally:
            conn.close()

    def set_source_state(self, source_name: str, key: str, value: str) -> None:
        """Write a source-state value (upsert)."""
        conn = self._open(self._db_path)
        try:
            _source_state.set_source_state(conn, source_name, key, value)
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def get_all_source_state(self, source_name: str) -> dict[str, str]:
        """Read all key-value pairs for a source."""
        conn = self._open(self._db_path)
        try:
            return _source_state.get_all_source_state(conn, source_name)
        finally:
            conn.close()

    @property
    def db_path(self) -> str:
        return self._db_path

    # ─── Source deduplication (durable, restart-safe) ─────────────────────────

    def source_item_seen(self, source_name: str, external_id: str) -> bool:
        """
        Return True if (source_name, external_id) was already marked as seen.

        Used by sources for durable, restart-safe deduplication. The key is the
        composite (source_name, external_id) so two different sources may each
        track the same external ID independently.
        """
        conn = self._open(self._db_path)
        try:
            return _source_state.source_item_seen(conn, source_name, external_id)
        finally:
            conn.close()

    def mark_source_item_seen(
        self, source_name: str, external_id: str, seen_at: str
    ) -> None:
        """Record an external ID as seen (idempotent upsert)."""
        conn = self._open(self._db_path)
        try:
            _source_state.mark_source_item_seen(conn, source_name, external_id, seen_at)
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def prune_source_seen_items(self, source_name: str, max_items: int) -> int:
        """
        Delete the oldest seen IDs for a source when over the configured limit.

        Keeps the most recent ``max_items`` rows (ordered by seen_at, then rowid).
        Returns the number of rows deleted. No-op when already at/under the limit.
        """
        conn = self._open(self._db_path)
        try:
            return _source_state.prune_source_seen_items(conn, source_name, max_items)
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    # ─── Recent observational history ──────────────────────────────────────────

    def append_recent_event(self, event: dict[str, Any], capacity: int) -> None:
        """Append a published event to the durable recent journal.

        Capacity is supplied by the caller (events.RECENT_HISTORY_CAPACITY).
        Failure isolation: the caller is responsible for catching exceptions so
        an observational-journal failure never fails the authoritative event.
        """
        conn = self._open(self._db_path)
        try:
            _recent_events.append_recent_event(conn, event, capacity)
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def get_recent_events(
        self, limit: int, newest_first: bool = False
    ) -> list[dict[str, Any]]:
        """Return reconstructed recent events (observational journal)."""
        conn = self._open(self._db_path)
        try:
            return _recent_events.list_recent_events(conn, limit, newest_first)
        finally:
            conn.close()

    # ─── System metric aggregates (cheap global queries) ───────────────────────

    def persistent_event_count(self) -> int:
        """Return COUNT(*) from persistent_events."""
        conn = self._open(self._db_path)
        try:
            return _events.count(conn)
        finally:
            conn.close()

    def persistent_high_water(self) -> int:
        """Return MAX(sequence) from persistent_events, or 0 if empty."""
        conn = self._open(self._db_path)
        try:
            row = conn.execute(
                "SELECT MAX(sequence) FROM persistent_events"
            ).fetchone()
            return row[0] if row and row[0] is not None else 0
        finally:
            conn.close()

    def count_consumers(self) -> int:
        """Return COUNT(*) from consumers table."""
        conn = self._open(self._db_path)
        try:
            row = conn.execute("SELECT COUNT(*) FROM consumers").fetchone()
            return row[0] if row else 0
        finally:
            conn.close()

    def count_unacked_deliveries(self) -> int:
        """Return COUNT(*) of unacknowledged consumer_event_state rows."""
        conn = self._open(self._db_path)
        try:
            row = conn.execute(
                "SELECT COUNT(*) FROM consumer_event_state "
                "WHERE acknowledged_at IS NULL"
            ).fetchone()
            return row[0] if row else 0
        finally:
            conn.close()
