"""
Process-wide runtime metrics for the MCP Event Server.

Owns the RuntimeMetrics TYPE and counter/aggregation logic only. It does NOT
create an application singleton — server.py constructs exactly one canonical
instance and injects it into Services, events, the alert evaluator, and the
source Publisher infrastructure.

All counters are guarded by a threading.Lock because some MCP sync tools may
execute in worker threads. Metric operations are intentionally cheap and must
never break the operation they instrument.
"""

from __future__ import annotations

import threading
from datetime import datetime, timezone
from typing import Any


class RuntimeMetrics:
    """Process-wide operational metrics (single canonical instance)."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._started_at = datetime.now(timezone.utc)

        # EVENTS
        self._events_published_total = 0
        self._events_persistent_total = 0
        self._events_nonpersistent_total = 0
        self._events_publication_failures_total = 0
        self._events_alert_triggered_total = 0

        # ALERTS
        self._alerts_evaluations_total = 0
        self._alerts_matches_total = 0
        self._alerts_failures_total = 0

        # NOTIFICATIONS
        self._notifications_attempted_total = 0
        self._notifications_failed_total = 0

        # SOURCES
        self._sources_published_total = 0
        self._sources_failures_total = 0

        # RECENT HISTORY (observational journal)
        self._recent_history_failures_total = 0

    # ─── EVENTS ────────────────────────────────────────────────────────────────

    def record_event_published(self, persistent: bool) -> None:
        with self._lock:
            self._events_published_total += 1
            if persistent:
                self._events_persistent_total += 1
            else:
                self._events_nonpersistent_total += 1

    def record_publication_failure(self) -> None:
        with self._lock:
            self._events_publication_failures_total += 1

    def record_alert_triggered(self) -> None:
        with self._lock:
            self._events_alert_triggered_total += 1

    # ─── ALERTS ─────────────────────────────────────────────────────────────────

    def record_alert_evaluation(self) -> None:
        with self._lock:
            self._alerts_evaluations_total += 1

    def record_alert_match(self) -> None:
        with self._lock:
            self._alerts_matches_total += 1

    def record_alert_failure(self) -> None:
        with self._lock:
            self._alerts_failures_total += 1

    # ─── NOTIFICATIONS ───────────────────────────────────────────────────────────

    def record_notification_attempted(self) -> None:
        with self._lock:
            self._notifications_attempted_total += 1

    def record_notification_failed(self) -> None:
        with self._lock:
            self._notifications_failed_total += 1

    # ─── SOURCES ────────────────────────────────────────────────────────────────

    def record_source_published(self) -> None:
        with self._lock:
            self._sources_published_total += 1

    def record_source_failure(self) -> None:
        with self._lock:
            self._sources_failures_total += 1

    # ─── RECENT HISTORY ─────────────────────────────────────────────────────────

    def record_recent_history_failure(self) -> None:
        with self._lock:
            self._recent_history_failures_total += 1

    # ─── SNAPSHOT ───────────────────────────────────────────────────────────────

    def snapshot(self) -> dict[str, Any]:
        """Return a JSON-compatible primitive snapshot of all counters."""
        with self._lock:
            return {
                "events": {
                    "published_total": self._events_published_total,
                    "persistent_total": self._events_persistent_total,
                    "nonpersistent_total": self._events_nonpersistent_total,
                    "publication_failures_total": self._events_publication_failures_total,
                    "alert_triggered_total": self._events_alert_triggered_total,
                },
                "alerts": {
                    "evaluations_total": self._alerts_evaluations_total,
                    "matches_total": self._alerts_matches_total,
                    "failures_total": self._alerts_failures_total,
                },
                "notifications": {
                    "attempted_total": self._notifications_attempted_total,
                    "failed_total": self._notifications_failed_total,
                },
                "sources": {
                    "published_total": self._sources_published_total,
                    "failures_total": self._sources_failures_total,
                },
                "recent_history": {
                    "failures_total": self._recent_history_failures_total,
                },
            }

    @property
    def started_at(self) -> datetime:
        return self._started_at
