"""
Schema creation and migration functions for the event store.

These functions operate on an explicit sqlite3.Connection and are called
by EventStore._init_db() during database initialization and migration.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime

logger = logging.getLogger(__name__)

SCHEMA_VERSION = 9


def get_schema_version(conn: sqlite3.Connection) -> int:
    row = conn.execute("PRAGMA user_version").fetchone()
    return row[0] if row else 0


def create_v7_schema(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE persistent_events (
            sequence   INTEGER PRIMARY KEY AUTOINCREMENT,
            id         TEXT NOT NULL UNIQUE,
            type       TEXT NOT NULL,
            source     TEXT NOT NULL,
            timestamp  TEXT NOT NULL,
            data       TEXT NOT NULL,
            routing    TEXT,
            created_at TEXT NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE consumers (
            consumer_id TEXT PRIMARY KEY,
            created_at  TEXT NOT NULL,
            updated_at  TEXT NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE consumer_topics (
            consumer_id TEXT NOT NULL,
            topic       TEXT NOT NULL,
            PRIMARY KEY (consumer_id, topic),
            FOREIGN KEY (consumer_id) REFERENCES consumers(consumer_id)
        )
    """)
    conn.execute("""
        CREATE TABLE consumer_event_state (
            consumer_id     TEXT NOT NULL,
            event_id        TEXT NOT NULL,
            delivered_at    TEXT,
            acknowledged_at TEXT,
            PRIMARY KEY (consumer_id, event_id),
            FOREIGN KEY (consumer_id) REFERENCES consumers(consumer_id),
            FOREIGN KEY (event_id)    REFERENCES persistent_events(id)
        )
    """)
    conn.execute("""
        CREATE TABLE consumer_checkpoints (
            consumer_id   TEXT PRIMARY KEY,
            last_sequence INTEGER NOT NULL DEFAULT 0,
            updated_at    TEXT NOT NULL,
            FOREIGN KEY (consumer_id) REFERENCES consumers(consumer_id)
        )
    """)
    conn.execute("""
        CREATE INDEX idx_persistent_events_created_at
        ON persistent_events(created_at)
    """)
    conn.execute("""
        CREATE INDEX idxCES_ack
        ON consumer_event_state(consumer_id, acknowledged_at)
    """)
    conn.execute("""
        CREATE INDEX idxCES_consumer
        ON consumer_event_state(consumer_id, event_id)
    """)
    conn.execute("""
        CREATE TABLE source_state (
            source_name TEXT NOT NULL,
            key         TEXT NOT NULL,
            value       TEXT NOT NULL,
            updated_at  TEXT NOT NULL,
            PRIMARY KEY (source_name, key)
        )
    """)
    conn.execute("""
        CREATE TABLE source_seen_items (
            source_name TEXT NOT NULL,
            external_id TEXT NOT NULL,
            seen_at     TEXT NOT NULL,
            PRIMARY KEY (source_name, external_id)
        )
    """)
    conn.execute("""
        CREATE INDEX idx_seen_source_seen_at
        ON source_seen_items(source_name, seen_at)
    """)
    create_alerts_table(conn)
    create_recent_events_table(conn)


def create_alerts_table(conn: sqlite3.Connection) -> None:
    """Create the generic alert-definition table and its indexes (idempotent)."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS alerts (
            alert_id          TEXT PRIMARY KEY,
            consumer_id       TEXT NOT NULL,
            name              TEXT,
            source            TEXT NOT NULL,
            event_type        TEXT,
            field_path        TEXT NOT NULL,
            operator          TEXT NOT NULL,
            value_json        TEXT NOT NULL,
            enabled           INTEGER NOT NULL DEFAULT 1,
            one_shot          INTEGER NOT NULL DEFAULT 1,
            created_at        TEXT NOT NULL,
            updated_at        TEXT NOT NULL,
            last_triggered_at TEXT,
            trigger_count     INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY (consumer_id) REFERENCES consumers(consumer_id)
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_alerts_consumer_enabled
        ON alerts(consumer_id, enabled)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_alerts_source_enabled
        ON alerts(source, enabled)
    """)


def migrate_v4_to_v5(conn: sqlite3.Connection) -> None:
    """Add consumer_checkpoints table and replay indexes."""
    cols = {row[1] for row in conn.execute(
        "PRAGMA table_info(consumer_checkpoints)"
    ).fetchall()}

    if "consumer_checkpoints" not in cols:
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.execute("""
                CREATE TABLE consumer_checkpoints (
                    consumer_id   TEXT PRIMARY KEY,
                    last_sequence INTEGER NOT NULL DEFAULT 0,
                    updated_at    TEXT NOT NULL,
                    FOREIGN KEY (consumer_id) REFERENCES consumers(consumer_id)
                )
            """)
            # Initialize checkpoint for every existing consumer
            conn.execute("""
                INSERT INTO consumer_checkpoints (consumer_id, last_sequence, updated_at)
                SELECT c.consumer_id, 0, datetime('now')
                FROM consumers c
                LEFT JOIN consumer_checkpoints cp ON c.consumer_id = cp.consumer_id
                WHERE cp.consumer_id IS NULL
            """)
            conn.execute("PRAGMA user_version = 5")
            conn.commit()
            logger.info("migrated v4→v5: added consumer_checkpoints")
        except Exception:
            conn.rollback()
            raise
    else:
        conn.execute("PRAGMA user_version = 5")
        conn.commit()
        logger.info("event store already at v5 schema")

    # Ensure indexes exist
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idxCES_ack
        ON consumer_event_state(consumer_id, acknowledged_at)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idxCES_consumer
        ON consumer_event_state(consumer_id, event_id)
    """)
    conn.commit()


def migrate_v5_to_v6(conn: sqlite3.Connection) -> None:
    """Add source_state table for durable source cursors."""
    cols = {row[1] for row in conn.execute(
        "PRAGMA table_info(source_state)"
    ).fetchall()}

    if "source_state" not in cols:
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.execute("""
                CREATE TABLE source_state (
                    source_name TEXT NOT NULL,
                    key         TEXT NOT NULL,
                    value       TEXT NOT NULL,
                    updated_at  TEXT NOT NULL,
                    PRIMARY KEY (source_name, key)
                )
            """)
            conn.execute("PRAGMA user_version = 6")
            conn.commit()
            logger.info("migrated v5→v6: added source_state")
        except Exception:
            conn.rollback()
            raise
    else:
        conn.execute("PRAGMA user_version = 6")
        conn.commit()
        logger.info("event store already at v6 schema")


def migrate_v6_to_v7(conn: sqlite3.Connection) -> None:
    """Add source_seen_items table for durable restart-safe source deduplication."""
    cols = {row[1] for row in conn.execute(
        "PRAGMA table_info(source_seen_items)"
    ).fetchall()}

    if "source_seen_items" not in cols:
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.execute("""
                CREATE TABLE source_seen_items (
                    source_name TEXT NOT NULL,
                    external_id TEXT NOT NULL,
                    seen_at     TEXT NOT NULL,
                    PRIMARY KEY (source_name, external_id)
                )
            """)
            conn.execute("""
                CREATE INDEX idx_seen_source_seen_at
                ON source_seen_items(source_name, seen_at)
            """)
            conn.execute("PRAGMA user_version = 7")
            conn.commit()
            logger.info("migrated v6→v7: added source_seen_items")
        except Exception:
            conn.rollback()
            raise
    else:
        conn.execute("PRAGMA user_version = 7")
        conn.commit()
        logger.info("event store already at v7 schema")


def migrate_v7_to_v8(conn: sqlite3.Connection) -> None:
    """Add alerts table for generic alert-definition persistence."""
    create_alerts_table(conn)
    conn.execute("PRAGMA user_version = 8")
    conn.commit()
    logger.info("migrated v7→v8: added alerts table")


def create_recent_events_table(conn: sqlite3.Connection) -> None:
    """Create the durable recent-event observational journal (idempotent)."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS recent_events (
            recent_sequence      INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id            TEXT NOT NULL UNIQUE,
            type                TEXT NOT NULL,
            source              TEXT NOT NULL,
            timestamp           TEXT NOT NULL,
            data_json           TEXT NOT NULL,
            persistent          INTEGER NOT NULL,
            persistent_sequence INTEGER,
            routing_json        TEXT
        )
    """)


def migrate_v8_to_v9(conn: sqlite3.Connection) -> None:
    """Add recent_events observational journal table."""
    create_recent_events_table(conn)
    conn.execute("PRAGMA user_version = 9")
    conn.commit()
    logger.info("migrated v8→v9: added recent_events table")


def migrate_v1_to_v3(conn: sqlite3.Connection) -> None:
    rows = conn.execute(
        "SELECT id, type, source, timestamp, data, created_at "
        "FROM persistent_events ORDER BY rowid"
    ).fetchall()
    old_count = len(rows)

    conn.execute("BEGIN IMMEDIATE")
    try:
        conn.execute("DROP TABLE persistent_events")
        create_v3_schema_partial(conn)
        if rows:
            conn.executemany(
                "INSERT INTO persistent_events "
                "(id, type, source, timestamp, data, routing, created_at) "
                "VALUES (?, ?, ?, ?, ?, NULL, ?)",
                [(r[0], r[1], r[2], r[3], r[4], r[5]) for r in rows],
            )
        new_count = conn.execute("SELECT COUNT(*) FROM persistent_events").fetchone()[0]
        if new_count != old_count:
            raise RuntimeError(f"migration row count mismatch: {old_count} vs {new_count}")
        dupes = conn.execute(
            "SELECT id, COUNT(*) as cnt FROM persistent_events GROUP BY id HAVING cnt > 1"
        ).fetchall()
        if dupes:
            raise RuntimeError(f"migration produced duplicate IDs: {dupes}")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS consumers (
                consumer_id TEXT PRIMARY KEY,
                created_at  TEXT NOT NULL,
                updated_at  TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS consumer_topics (
                consumer_id TEXT NOT NULL,
                topic       TEXT NOT NULL,
                PRIMARY KEY (consumer_id, topic),
                FOREIGN KEY (consumer_id) REFERENCES consumers(consumer_id)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS consumer_event_state (
                consumer_id     TEXT NOT NULL,
                event_id        TEXT NOT NULL,
                delivered_at    TEXT,
                acknowledged_at TEXT,
                PRIMARY KEY (consumer_id, event_id),
                FOREIGN KEY (consumer_id) REFERENCES consumers(consumer_id),
                FOREIGN KEY (event_id)    REFERENCES persistent_events(id)
            )
        """)
        conn.execute("PRAGMA user_version = 3")
        conn.commit()
        logger.info("migrated %d rows v1→v3", old_count)
    except Exception:
        conn.rollback()
        raise


def migrate_v2_to_v3(conn: sqlite3.Connection) -> None:
    cols = {row[1] for row in conn.execute(
        "PRAGMA table_info(persistent_events)"
    ).fetchall()}
    if "routing" not in cols:
        conn.execute("ALTER TABLE persistent_events ADD COLUMN routing TEXT")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS consumers (
            consumer_id TEXT PRIMARY KEY,
            created_at  TEXT NOT NULL,
            updated_at  TEXT NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS consumer_topics (
            consumer_id TEXT NOT NULL,
            topic       TEXT NOT NULL,
            PRIMARY KEY (consumer_id, topic),
            FOREIGN KEY (consumer_id) REFERENCES consumers(consumer_id)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS consumer_event_state (
            consumer_id     TEXT NOT NULL,
            event_id        TEXT NOT NULL,
            delivered_at    TEXT,
            acknowledged_at TEXT,
            PRIMARY KEY (consumer_id, event_id),
            FOREIGN KEY (consumer_id) REFERENCES consumers(consumer_id),
            FOREIGN KEY (event_id)    REFERENCES persistent_events(id)
        )
    """)
    conn.execute("PRAGMA user_version = 3")
    conn.commit()
    logger.info("migrated v2→v3")


def migrate_v3_to_v4(conn: sqlite3.Connection) -> None:
    cols = {row[1] for row in conn.execute(
        "PRAGMA table_info(consumer_event_state)"
    ).fetchall()}
    needs_migration = "sequence" in cols
    if needs_migration:
        rows = conn.execute(
            "SELECT consumer_id, event_id, delivered_at, acknowledged_at "
            "FROM consumer_event_state"
        ).fetchall()
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.execute("DROP TABLE consumer_event_state")
            conn.execute("""
                CREATE TABLE consumer_event_state (
                    consumer_id     TEXT NOT NULL,
                    event_id        TEXT NOT NULL,
                    delivered_at    TEXT,
                    acknowledged_at TEXT,
                    PRIMARY KEY (consumer_id, event_id),
                    FOREIGN KEY (consumer_id) REFERENCES consumers(consumer_id),
                    FOREIGN KEY (event_id)    REFERENCES persistent_events(id)
                )
            """)
            if rows:
                conn.executemany(
                    "INSERT INTO consumer_event_state "
                    "(consumer_id, event_id, delivered_at, acknowledged_at) "
                    "VALUES (?, ?, ?, ?)",
                    rows,
                )
            conn.execute("PRAGMA user_version = 4")
            conn.commit()
            logger.info("migrated v3→v4: cleaned consumer_event_state (%d rows)", len(rows))
        except Exception:
            conn.rollback()
            raise
    else:
        conn.execute("PRAGMA user_version = 4")
        conn.commit()
        logger.info("event store already at v4 schema (no v3 cleanup needed)")


def create_v3_schema_partial(conn: sqlite3.Connection) -> None:
    """Create core tables without the consumer_checkpoints table (for legacy migrations)."""
    conn.execute("""
        CREATE TABLE persistent_events (
            sequence   INTEGER PRIMARY KEY AUTOINCREMENT,
            id         TEXT NOT NULL UNIQUE,
            type       TEXT NOT NULL,
            source     TEXT NOT NULL,
            timestamp  TEXT NOT NULL,
            data       TEXT NOT NULL,
            routing    TEXT,
            created_at TEXT NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE consumers (
            consumer_id TEXT PRIMARY KEY,
            created_at  TEXT NOT NULL,
            updated_at  TEXT NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE consumer_topics (
            consumer_id TEXT NOT NULL,
            topic       TEXT NOT NULL,
            PRIMARY KEY (consumer_id, topic),
            FOREIGN KEY (consumer_id) REFERENCES consumers(consumer_id)
        )
    """)
    conn.execute("""
        CREATE TABLE consumer_event_state (
            consumer_id     TEXT NOT NULL,
            event_id        TEXT NOT NULL,
            delivered_at    TEXT,
            acknowledged_at TEXT,
            PRIMARY KEY (consumer_id, event_id),
            FOREIGN KEY (consumer_id) REFERENCES consumers(consumer_id),
            FOREIGN KEY (event_id)    REFERENCES persistent_events(id)
        )
    """)
    conn.execute("""
        CREATE INDEX idx_persistent_events_created_at
        ON persistent_events(created_at)
    """)
    conn.execute("""
        CREATE INDEX idxCES_ack
        ON consumer_event_state(consumer_id, acknowledged_at)
    """)
    conn.execute("""
        CREATE INDEX idxCES_consumer
        ON consumer_event_state(consumer_id, event_id)
    """)
