"""
Source connector framework for the MCP Event Server.

A source knows ONLY how to get external data and call publish_event().
It does NOT know about MCP transport, SubscriptionBus, ResourceUpdated,
SQLite internals, consumer checkpoints, replay, acknowledgement, or routing.

Usage:
    A source implements the EventSource protocol (or any object with
    async def run(self, publisher, stop_event)). The SourceManager
    starts enabled sources as named background tasks via the existing
    BackgroundTaskManager.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Protocol, runtime_checkable

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Publisher — the only way a source publishes events (and records dedup)
# ---------------------------------------------------------------------------

class PublishError(Exception):
    """Raised when publish_event() fails (e.g. storage error)."""
    pass


class SourceConfigError(Exception):
    """Raised when source configuration is invalid (unknown type, bad shape)."""
    pass


class Publisher:
    """
    The single output port a source uses to talk to the event server.

    A source calls ``publisher(...)`` to publish an event and uses
    ``publisher.is_seen(...)`` / ``publisher.mark_seen(...)`` for durable,
    restart-safe deduplication.  The source never touches the store, bus, or
    SQLite directly.
    """

    def __init__(self, store: Any, bus: Any) -> None:
        self._store = store
        self._bus = bus

    async def __call__(
        self,
        event_type: str,
        source: str,
        data: dict[str, Any],
        persistent: bool = False,
        routing: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        import events  # deferred to avoid circular import at module load
        try:
            return await events.publish_event(
                event_type=event_type,
                source=source,
                data=data,
                persistent=persistent,
                routing=routing,
                store=self._store,
                bus=self._bus,
            )
        except Exception as exc:
            raise PublishError(
                f"publish_event failed for source={source!r} type={event_type!r}: {exc}"
            ) from exc

    async def is_seen(self, source_name: str, external_id: str) -> bool:
        """Return True if this external ID was already durably published."""
        return await asyncio.to_thread(
            self._store.source_item_seen, source_name, external_id
        )

    async def mark_seen(
        self,
        source_name: str,
        external_id: str,
        max_items: int | None = None,
    ) -> None:
        """
        Record an external ID as durably seen (after successful publication).

        If ``max_items`` is provided, oldest seen IDs beyond the limit are pruned.
        """
        seen_at = datetime.now(timezone.utc).isoformat()
        await asyncio.to_thread(
            self._store.mark_source_item_seen, source_name, external_id, seen_at
        )
        if max_items:
            await asyncio.to_thread(
                self._store.prune_source_seen_items, source_name, max_items
            )

    async def get_cursor(self, source_name: str) -> str | None:
        """
        Read the durable cursor (ingestion high-water mark) for a source.

        The cursor is stored under the ``"cursor"`` key in the generic
        ``source_state`` table.  Returns None if no cursor has been persisted.
        """
        return await asyncio.to_thread(
            self._store.get_source_state, source_name, "cursor"
        )

    async def set_cursor(self, source_name: str, value: str) -> None:
        """
        Persist the durable cursor (ingestion high-water mark) for a source.

        Stored under the ``"cursor"`` key in the generic ``source_state`` table.
        """
        await asyncio.to_thread(
            self._store.set_source_state, source_name, "cursor", value
        )


def create_publisher(store: Any, bus: Any) -> Publisher:
    """Build a Publisher for the given store/bus."""
    return Publisher(store, bus)


# ---------------------------------------------------------------------------
# EventSource protocol
# ---------------------------------------------------------------------------

@runtime_checkable
class EventSource(Protocol):
    """Minimal protocol for an event source connector."""

    @property
    def name(self) -> str:
        """Stable internal name, e.g. 'http_poller'."""
        ...

    async def run(self, publisher: Any, stop_event: asyncio.Event) -> None:
        """
        Main loop.  Must exit cleanly when stop_event is set.

        Args:
            publisher: async callable from create_publisher().
            stop_event: set by SourceManager on shutdown.
        """
        ...

    def status(self) -> dict[str, Any]:
        """Return current source status for health reporting."""
        ...


# ---------------------------------------------------------------------------
# SourceManager — manages enabled sources as background tasks
# ---------------------------------------------------------------------------

@dataclass
class SourceManager:
    """
    Registry of enabled source connectors.

    Integrates with the existing BackgroundTaskManager — each source runs
    as a named background task with prefix 'source:'.
    """

    _sources: dict[str, Any] = field(default_factory=dict)
    _bg_manager: Any = None  # BackgroundTaskManager
    _store: Any = None
    _bus: Any = None
    _publisher: Any = None
    # Per-source stop events so a single source can be stopped independently
    # without affecting the others. shutdown() sets all of them.
    _stop_events: dict[str, asyncio.Event] = field(default_factory=dict)

    async def initialize(
        self,
        bg_manager: Any,
        store: Any,
        bus: Any,
    ) -> None:
        """Set references.  Must be called before start_all()."""
        self._bg_manager = bg_manager
        self._store = store
        self._bus = bus
        self._publisher = create_publisher(store, bus)

    def register(self, source: Any) -> None:
        """Register a source instance.  Must be called before start_all()."""
        name = source.name
        if name in self._sources:
            logger.warning("source '%s' already registered — overwriting", name)
        self._sources[name] = source
        if name not in self._stop_events:
            self._stop_events[name] = asyncio.Event()

    async def start_all(self, configs: dict[str, Any]) -> None:
        """
        Start all enabled sources.

        configs: the "sources" section from config.json, e.g.
            {"http_poller": {"enabled": true, ...}, "test_source": {"enabled": false}}
        """
        for name, source in self._sources.items():
            src_cfg = configs.get(name, {})
            if not src_cfg.get("enabled", False):
                logger.info("source '%s' disabled — skipping", name)
                continue
            await self._start_one(name, source, src_cfg)

    async def _start_one(self, name: str, source: Any, cfg: dict[str, Any]) -> None:
        """Start a single source as a background task."""
        task_name = f"source:{name}"
        stop_event = self._stop_events.setdefault(name, asyncio.Event())

        async def _wrapper():
            logger.info("source '%s' starting", name)
            try:
                await source.run(self._publisher, stop_event)
            except asyncio.CancelledError:
                logger.info("source '%s' cancelled", name)
                raise
            except Exception as exc:
                logger.error("source '%s' failed: %s", name, exc)
            else:
                logger.info("source '%s' exited normally", name)

        try:
            await self._bg_manager.start(task_name, _wrapper())
        except Exception as exc:
            logger.error("failed to start source '%s': %s", name, exc)

    async def shutdown(self) -> None:
        """Signal ALL sources to stop (server teardown)."""
        for ev in self._stop_events.values():
            ev.set()
        logger.info("source manager: stop signal sent to %d source(s)", len(self._sources))

    async def stop_source(self, name: str) -> bool:
        """
        Stop a single named source cleanly and independently of other sources.

        Signals the source's dedicated stop event (so its run loop exits at the
        next check) and cancels its background task via the BackgroundTaskManager.
        Other registered sources are unaffected.

        Returns True if the named source was known (and thus signaled),
        False otherwise.
        """
        if name not in self._sources:
            logger.warning("stop_source: unknown source '%s'", name)
            return False

        stop_event = self._stop_events.get(name)
        if stop_event is not None:
            stop_event.set()

        if self._bg_manager is not None:
            try:
                await self._bg_manager.cancel(f"source:{name}")
            except Exception as exc:
                logger.debug("stop_source: cancel task for '%s' raised: %s", name, exc)

        logger.info("source '%s' stop signaled", name)
        return True

    def get_status(self) -> dict[str, Any]:
        """Return status of all registered sources (for mcp-event://system/info or mcp-event://sources/status)."""
        result: dict[str, Any] = {}
        for name, source in self._sources.items():
            task_name = f"source:{name}"
            task_info = self._bg_manager.status(task_name) if self._bg_manager else {}
            task_status = task_info.get(task_name, {})
            src_status = source.status()
            src_status["task"] = task_status
            result[name] = src_status
        return result

    @property
    def enabled_sources(self) -> dict[str, Any]:
        return dict(self._sources)


# ---------------------------------------------------------------------------
# Static source registry + config-driven builder (Nuitka-safe, no discovery)
# ---------------------------------------------------------------------------

from sources.registry import SOURCE_TYPES  # noqa: E402  (bottom import avoids cycles)


def build_source_manager(sources_cfg: dict[str, Any] | None) -> SourceManager:
    """
    Build a SourceManager from the ``sources`` section of config.json.

    This is the ONLY place that maps a config ``"type"`` string to a concrete
    source class.  server.py calls this and knows nothing about individual
    source implementations.

    Args:
        sources_cfg: the "sources" dict, e.g.
            {"market_feed": {"type": "http_poller", "enabled": true, ...}}

    Returns:
        A SourceManager with all valid sources registered.  An empty config
        yields an empty (but valid) manager.

    Raises:
        SourceConfigError: on an unknown source type or malformed config entry.
    """
    manager = SourceManager()
    if not sources_cfg:
        return manager

    if not isinstance(sources_cfg, dict):
        raise SourceConfigError("'sources' must be a JSON object of named sources")

    for name, cfg in sources_cfg.items():
        if not isinstance(cfg, dict):
            raise SourceConfigError(
                f"source '{name}' config must be a JSON object, got "
                f"{type(cfg).__name__}"
            )

        src_type = cfg.get("type", name)
        cls = SOURCE_TYPES.get(src_type)
        if cls is None:
            raise SourceConfigError(
                f"unknown source type '{src_type}' for source '{name}'; "
                f"known types: {sorted(SOURCE_TYPES)}"
            )

        # Bind the runtime instance name (separate from the implementation type).
        instance_cfg = dict(cfg)
        instance_cfg["source_name"] = name
        manager.register(cls(instance_cfg))

    return manager
