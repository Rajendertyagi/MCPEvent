"""
Event model and publish orchestration.

Separate from MCP transport — this module knows nothing about subscriptions,
resources, or the MCP protocol. It owns event validation, ID generation,
in-memory state, and coordinates between the persistent store and the
live subscription bus (which is injected at call time).
"""

import asyncio
import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from mcp.shared.subscriptions import ResourceUpdated
from server_modules.contract import RESOURCE_EVENT_LATEST

logger = logging.getLogger(__name__)

# ─── Constants ────────────────────────────────────────────────────────────────

# (RESOURCE_EVENT_LATEST imported from server_modules.contract)

# Bounded in-memory + durable recent-history capacity (single canonical owner).
RECENT_HISTORY_CAPACITY = 200

# ─── Alert evaluator hook (process-wide, Context-free) ───────────────────────
# Set once at startup via configure_alert_evaluator(). events.py never imports
# the alert module directly — this avoids a circular import while still letting
# the canonical publish path trigger alert evaluation.
_alert_evaluator = None


def configure_alert_evaluator(fn) -> None:
    """Register the process-wide alert evaluator callable.

    The callable must accept a single event dict and be awaitable (async).
    It must be Context-free: no MCP Context, ClientSession, or request state.
    """
    global _alert_evaluator
    _alert_evaluator = fn


# ─── Metrics collector hook (process-wide, Context-free) ─────────────────────
# Set once at startup via configure_metrics(). events.py never imports
# server_modules.metrics — the single canonical RuntimeMetrics instance is
# injected from server.py, mirroring the alert-evaluator wiring.
_metrics = None

# ─── SSE broadcast hook (process-wide, Context-free) ────────────────────────
# Set once at startup via configure_sse_broker(). events.py never imports
# sse_broker — this avoids a circular import while still letting the
# canonical publish path fan events out to live SSE subscribers.
_event_broker = None


def configure_sse_broker(broker: Any) -> None:
    """Register the process-wide SSE broadcast broker."""
    global _event_broker
    _event_broker = broker


def configure_metrics(metrics) -> None:
    """Register the process-wide metrics collector.

    The object must expose the RuntimeMetrics recording methods. It must be
    Context-free and never raise into the publication path (callers guard).
    """
    global _metrics
    _metrics = metrics


def _metric(method: str, *args) -> None:
    """Call a RuntimeMetrics method, never letting a metric fault break the caller."""
    if _metrics is None:
        return
    try:
        getattr(_metrics, method)(*args)
    except Exception:
        logger.debug("metrics.%s raised; ignored", method, exc_info=True)


def _record_publication_failure() -> None:
    """Increment publication_failures_total without ever masking the caller's error."""
    _metric("record_publication_failure")


async def _maybe_evaluate_alerts(event: dict[str, Any]) -> None:
    """Invoke the alert evaluator after an event is published, if configured.

    Explicit recursion guard: alert.triggered events must never be re-evaluated
    as alert input. Evaluator failures are logged with a stack trace but do NOT
    turn the original (already-successful) publication into a failure.
    """
    if _alert_evaluator is None:
        return
    # Recursion protection — system-generated trigger events are not input.
    if event.get("type") == "alert.triggered":
        return
    try:
        await _alert_evaluator(event)
    except Exception:
        logger.exception(
            "alert evaluation failed for event id=%s type=%s",
            event.get("id"),
            event.get("type"),
        )
        _metric("record_alert_failure")


# ─── In-memory state ──────────────────────────────────────────────────────────

_latest_event: dict[str, Any] = {
    "id": uuid.uuid4().hex,
    "type": "server.started",
    "source": "mcp-server",
    "timestamp": datetime.now(timezone.utc).isoformat(),
    "data": {"message": "MCP server started"},
}

_published_event_count: int = 0
_history_lock = __import__("threading").Lock()
_event_history: list[dict[str, Any]] = []

_server_start_time: datetime = datetime.now(timezone.utc)


# ─── Public accessors (for server.py resources) ──────────────────────────────

def get_latest_event() -> dict[str, Any]:
    """Return a copy of the latest event dict."""
    return dict(_latest_event)


def get_server_start_time() -> datetime:
    """Return the server start timestamp."""
    return _server_start_time


def get_event_count() -> int:
    """Return the total number of events published."""
    return _published_event_count


def get_event_history(limit: int = 50) -> list[dict[str, Any]]:
    """Return a thread-safe snapshot of the event history buffer."""
    with _history_lock:
        snapshot = list(_event_history)
    n = min(max(1, int(limit)), 50)
    return snapshot[-n:] if len(snapshot) >= n else snapshot


# ─── Notification ─────────────────────────────────────────────────────────────

async def _notify_subscribers_async(
    resource_uri: str,
    bus: Any,  # InMemorySubscriptionBus — typed externally
) -> None:
    """
    Broadcast a resource-update notification to all subscribed MCP clients.

    Uses the MCP SubscriptionBus (2026-07-28 spec). If the bus is not yet
    initialized, the notification is silently skipped — clients can always
    poll the resource.
    """
    if bus is None:
        logger.debug("subscription bus not initialized; notification skipped")
        return
    try:
        _metric("record_notification_attempted")
        await bus.publish(ResourceUpdated(uri=resource_uri))
    except Exception as exc:
        logger.error(
            "failed to broadcast resource update for %s: %s", resource_uri, exc
        )
        _metric("record_notification_failed")


# ─── Publish ──────────────────────────────────────────────────────────────────

async def publish_event(
    event_type: str,
    source: str,
    data: dict[str, Any] | None = None,
    *,
    persistent: bool = False,
    routing: dict[str, Any] | None = None,
    store: Any = None,  # EventStore — typed externally
    bus: Any = None,    # InMemorySubscriptionBus — typed externally
) -> dict[str, Any]:
    """
    Publish a new event through the single canonical publication path.

    All events use a UUID v4 identifier for stable, collision-resistant identity.
    Persistent events are additionally written to SQLite (before notification)
    and receive a monotonic sequence number for replay ordering.

    Routing metadata is optional. When absent, the event is a broadcast.
    When present, it must be a dict with optional keys:
      - "targets": list of consumer_id strings
      - "topics": list of topic strings

    Args:
        event_type: A dot-namespaced identifier, e.g. "alert.received".
        source:     Identifies where the event originated.
        data:       Arbitrary JSON-compatible payload. Can be empty or None.
        persistent: If True, store the event durably before notifying.
        routing:    Optional routing metadata (targets/topics).
        store:      EventStore instance (required when persistent=True).
        bus:        SubscriptionBus instance for live notification.

    Returns:
        The event dictionary that was published.

    Raises:
        ValueError: If event_type, source, data, or routing is invalid.
        RuntimeError: If persistent=True but no store was provided.
    """
    global _latest_event, _published_event_count

    # ── Validation ──────────────────────────────────────────────────────────
    if not event_type or not isinstance(event_type, str):
        _record_publication_failure()
        raise ValueError("event_type must be a non-empty string")
    if not source or not isinstance(source, str):
        _record_publication_failure()
        raise ValueError("source must be a non-empty string")
    if data is None:
        data = {}
    elif not isinstance(data, dict):
        _record_publication_failure()
        raise ValueError("data must be a JSON-compatible object (dict)")

    # ── Validate routing if provided ────────────────────────────────────────
    if routing is not None:
        if not isinstance(routing, dict):
            _record_publication_failure()
            raise ValueError("routing must be a dict or None")
        targets = routing.get("targets")
        if targets is not None:
            if not isinstance(targets, list) or not all(isinstance(t, str) and t for t in targets):
                raise ValueError("routing.targets must be a list of non-empty strings")
            routing["targets"] = list(dict.fromkeys(targets))  # dedupe, preserve order
        topics = routing.get("topics")
        if topics is not None:
            if not isinstance(topics, list) or not all(isinstance(t, str) and t for t in topics):
                raise ValueError("routing.topics must be a list of non-empty strings")
            routing["topics"] = list(dict.fromkeys(topics))  # dedupe, preserve order
        # Strip None values for cleaner storage
        routing = {k: v for k, v in routing.items() if v is not None} or None

    # ── ID generation — UUID v4 for ALL events ──────────────────────────────
    event_id = uuid.uuid4().hex
    timestamp = datetime.now(timezone.utc).isoformat()

    event: dict[str, Any] = {
        "id": event_id,
        "type": event_type.strip(),
        "source": source.strip(),
        "timestamp": timestamp,
        "data": data,
        "persistent": persistent,
    }
    if routing is not None:
        event["routing"] = routing

    # ── Persistence (if requested) ──────────────────────────────────────────
    sequence: int | None = None
    if persistent:
        if store is None:
            raise RuntimeError(
                "persistent=True requires an EventStore instance; "
                "pass store= to publish_event()"
            )
        try:
            sequence = await asyncio.to_thread(
                store.save,
                event_id,
                event["type"],
                event["source"],
                timestamp,
                data,
                routing,
            )
        except Exception as exc:
            logger.error("failed to persist event %s: %s", event_id, exc)
            _record_publication_failure()
            raise RuntimeError(
                f"persistent event publication failed: {exc}"
            ) from exc
        event["sequence"] = sequence

    # ── Update in-memory state ──────────────────────────────────────────────
    _latest_event = event
    _published_event_count += 1

    with _history_lock:
        _event_history.append(event)
        while len(_event_history) > RECENT_HISTORY_CAPACITY:
            _event_history.pop(0)

    # ── Metrics: successful authoritative acceptance ─────────────────────────
    _metric("record_event_published", persistent)
    if event["type"] == "alert.triggered":
        _metric("record_alert_triggered")

    logger.info(
        "event  id=%s  type=%s  source=%s  persistent=%s  seq=%s",
        event_id,
        event["type"],
        event["source"],
        persistent,
        sequence,
    )

    # ── Durable recent observational journal (failure-isolated) ──────────────
    try:
        if store is not None:
            await asyncio.to_thread(
                store.append_recent_event,
                event,
                capacity=RECENT_HISTORY_CAPACITY,
            )
    except Exception:
        logger.exception(
            "recent-history journal append failed for event id=%s type=%s",
            event.get("id"),
            event.get("type"),
        )
        _metric("record_recent_history_failure")
        # Observational failure must NOT fail the authoritative publication.

    # ── Live notification ───────────────────────────────────────────────────
    await _notify_subscribers_async(RESOURCE_EVENT_LATEST, bus)

    # ── SSE fan-out (fire-and-forget, failure-isolated) ─────────────────────
    if _event_broker is not None:
        try:
            _event_broker.broadcast(json.dumps(event, ensure_ascii=False))
        except Exception:
            logger.debug(
                "SSE broadcast failed for event id=%s type=%s",
                event.get("id"),
                event.get("type"),
                exc_info=True,
            )

    # ── Alert evaluation (post-publish hook, Context-free) ───────────────────
    await _maybe_evaluate_alerts(event)

    return event


def restore_recent_history(events_list: list[dict[str, Any]]) -> None:
    """Hydrate in-memory latest/history from the durable recent journal.

    HYDRATION ONLY: does not publish, notify, materialize routing, evaluate
    alerts, or increment publication counters. Restart must not replay history
    as new activity.
    """
    global _latest_event, _event_history
    if not events_list:
        return
    with _history_lock:
        _event_history = list(events_list)  # oldest -> newest
        _latest_event = events_list[-1]
