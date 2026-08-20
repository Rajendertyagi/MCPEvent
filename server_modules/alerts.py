"""
Generic alert engine — domain / service layer.

Owns:
- alert definition validation
- dotted field_path resolution (with missing-vs-null distinction)
- condition comparison (eq/ne scalar, gt/gte/lt/lte numeric)
- event evaluation against candidate alerts
- alert.triggered event construction
- AlertEvaluator (process-wide, Context-free)

Does NOT own:
- SQLite DDL (store_modules/schema.py)
- SQL / persistence (store_modules/alerts.py)
- MCP tool registration (server_modules/tools/alerts.py)

The evaluator calls the canonical events.publish_event() for trigger emission,
so there is no second event-publication path. It depends on no MCP Context,
ClientSession, request state, or consumer session.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import events
from errors import AlertNotFoundError, ConsumerNotFoundError, ValidationError

logger = logging.getLogger(__name__)

# Stable generic source-instance identifier owned by the alert engine.
# The trigger event is produced BY the alert engine, not by the matched source.
ALERT_ENGINE_SOURCE = "alert_engine"

# Supported comparison operators (MVP only).
SUPPORTED_OPERATORS = ("eq", "ne", "gt", "gte", "lt", "lte")

# Internal sentinel for "field path absent" (distinct from JSON null).
_MISSING = object()


# ─── Validation ───────────────────────────────────────────────────────────────

def validate_alert_definition(
    consumer_id: str,
    source: str,
    field_path: str,
    operator: str,
    value: Any,
    name: str | None,
    event_type: str | None,
    one_shot: bool,
) -> None:
    """Validate alert creation inputs. Raises ValidationError on failure."""
    if not consumer_id or not isinstance(consumer_id, str) or not consumer_id.strip():
        raise ValidationError("consumer_id must be a non-empty string")
    if not source or not isinstance(source, str) or not source.strip():
        raise ValidationError("source must be a non-empty string")
    if not field_path or not isinstance(field_path, str):
        raise ValidationError("field_path must be a non-empty string")
    # Dotted path must have no empty components.
    if any(not part for part in field_path.split(".")):
        raise ValidationError("field_path must not contain empty components")
    if operator not in SUPPORTED_OPERATORS:
        raise ValidationError(
            "operator must be one of: {0}".format(", ".join(SUPPORTED_OPERATORS))
        )
    # value must be a JSON scalar (not dict/list).
    if isinstance(value, (dict, list)):
        raise ValidationError("value must be a JSON scalar (string, number, bool, or null)")
    # Ordering operators require a numeric value (bool is NOT numeric).
    if operator in ("gt", "gte", "lt", "lte"):
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValidationError(
                "operator '{0}' requires a numeric value".format(operator)
            )
    if name is not None and not isinstance(name, str):
        raise ValidationError("name must be a string or null")
    if event_type is not None and (
        not isinstance(event_type, str) or not event_type.strip()
    ):
        raise ValidationError("event_type must be a non-empty string or null")
    if not isinstance(one_shot, bool):
        raise ValidationError("one_shot must be a boolean")


# ─── Field path resolution ────────────────────────────────────────────────────

def resolve_field_path(data: dict[str, Any], field_path: str) -> Any:
    """Return the value at a dotted path inside event data.

    Returns _MISSING if any path segment is absent (field does not exist).
    Returns None if the field exists but holds JSON null (a legitimate value).
    """
    cur: Any = data
    for part in field_path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return _MISSING
        cur = cur[part]
    return cur


# ─── Comparison ───────────────────────────────────────────────────────────────

def _is_numeric(v: Any) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def _eq(a: Any, b: Any) -> bool:
    """Type-aware equality. bool is treated separately from numeric."""
    # bool is NOT numeric
    if isinstance(a, bool) or isinstance(b, bool):
        return type(a) is type(b) and a == b
    # numbers
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        return a == b
    # strings
    if isinstance(a, str) and isinstance(b, str):
        return a == b
    # None
    if a is None and b is None:
        return True
    # mixed types never equal
    return False


def compare_values(observed: Any, operator: str, expected: Any) -> bool:
    """Evaluate a single condition. Returns True if the condition matches."""
    if operator == "eq":
        return _eq(observed, expected)
    if operator == "ne":
        return not _eq(observed, expected)
    # Ordering operators: numeric only. Non-numeric → no match (not an error).
    if not (_is_numeric(observed) and _is_numeric(expected)):
        return False
    if operator == "gt":
        return observed > expected
    if operator == "gte":
        return observed >= expected
    if operator == "lt":
        return observed < expected
    if operator == "lte":
        return observed <= expected
    return False


# ─── Condition matching ───────────────────────────────────────────────────────

def alert_matches(alert: dict[str, Any], event: dict[str, Any]) -> bool:
    """Check whether an event satisfies an alert's condition."""
    # event_type filter (null = any type from the configured source)
    event_type_filter = alert.get("event_type")
    if event_type_filter is not None and event.get("type") != event_type_filter:
        return False
    # source is already filtered by the candidate query (source = event["source"])
    observed = resolve_field_path(event.get("data") or {}, alert["field_path"])
    if observed is _MISSING:
        return False  # missing field → condition does NOT match
    return compare_values(observed, alert["operator"], alert["value"])


# ─── Trigger event construction ──────────────────────────────────────────────

def build_trigger_data(alert: dict[str, Any], event: dict[str, Any]) -> dict[str, Any]:
    """Construct the data payload for an alert.triggered event."""
    data: dict[str, Any] = {
        "alert_id": alert["alert_id"],
        "consumer_id": alert["consumer_id"],
        "matched_event_id": event.get("id"),
        "matched_event_type": event.get("type"),
        "matched_source": event.get("source"),
        "field_path": alert["field_path"],
        "operator": alert["operator"],
        "expected_value": alert["value"],
        "observed_value": resolve_field_path(event.get("data") or {}, alert["field_path"]),
        "one_shot": alert["one_shot"],
    }
    if alert.get("name"):
        data["name"] = alert["name"]
    return data


# ─── Evaluator ────────────────────────────────────────────────────────────────

class AlertEvaluator:
    """Process-wide alert evaluator. Context-free, no MCP/session dependencies."""

    def __init__(self, store: Any, subscription_bus: Any, metrics: Any = None) -> None:
        self._store = store
        self._bus = subscription_bus
        self._metrics = metrics
        # Per-alert asyncio locks, process-lifetime registry. Bounded by the
        # number of distinct alert_ids seen during the process lifetime.
        self._locks: dict[str, asyncio.Lock] = {}
        self._locks_guard = asyncio.Lock()

    def _metric(self, method: str, *args) -> None:
        """Call a RuntimeMetrics method, never letting a metric fault break the caller."""
        if self._metrics is None:
            return
        try:
            getattr(self._metrics, method)(*args)
        except Exception:
            logger.debug("metrics.%s raised; ignored", method, exc_info=True)

    async def _get_lock(self, alert_id: str) -> asyncio.Lock:
        async with self._locks_guard:
            lock = self._locks.get(alert_id)
            if lock is None:
                lock = asyncio.Lock()
                self._locks[alert_id] = lock
            return lock

    async def evaluate(self, event: dict[str, Any]) -> None:
        """Evaluate a published event against enabled alerts for its source."""
        source = event.get("source")
        if not source:
            return

        # Count this as an evaluation (only non-alert.triggered events reach here
        # due to the recursion guard in events._maybe_evaluate_alerts).
        self._metric("record_alert_evaluation")

        # Candidate query: enabled alerts for this source (indexed).
        candidates = await asyncio.to_thread(
            self._store.list_alerts_by_source_enabled, source
        )

        for alert in candidates:
            # Cheap pre-filter before acquiring the per-alert lock.
            if not alert_matches(alert, event):
                continue

            self._metric("record_alert_match")

            lock = await self._get_lock(alert["alert_id"])
            async with lock:
                # Re-read fresh state under the lock (prevents double-trigger).
                fresh = await asyncio.to_thread(
                    self._store.get_alert, alert["consumer_id"], alert["alert_id"]
                )
                if fresh is None or not fresh["enabled"]:
                    continue
                # Re-check condition against the fresh definition (defense in depth).
                if not alert_matches(fresh, event):
                    continue

                # Publish the trigger via the canonical event path.
                await events.publish_event(
                    event_type="alert.triggered",
                    source=ALERT_ENGINE_SOURCE,
                    data=build_trigger_data(fresh, event),
                    persistent=True,
                    routing={"targets": [fresh["consumer_id"]]},
                    store=self._store,
                    bus=self._bus,
                )

                # Record trigger state (atomic) only after successful publish.
                await asyncio.to_thread(
                    self._store.record_alert_trigger, fresh["alert_id"]
                )
