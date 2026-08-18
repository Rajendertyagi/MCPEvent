"""
Deterministic test source — generates predictable events on a timer.

This source is for integration testing and extensibility proof.  It is DISABLED
by default and must be explicitly enabled in config.

It proves that a second (or Nth) source can be added without modifying:
  - MCP server setup
  - event publishing internals
  - routing, replay, SQLite consumer state
  - subscription logic

The runtime instance name is taken from config ("source_name"), so multiple
instances of the same implementation class can coexist (e.g. two test feeds).
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)


class TestSource:
    """
    Deterministic source that publishes numbered events at fixed intervals.

    Configuration (in config.json sources section):
    {
      "test_source": {
        "type": "test_source",
        "enabled": true,
        "interval_seconds": 2,
        "event_type": "test.source.tick",
        "max_events": 5,
        "persistent": false
      }
    }
    """

    def __init__(self, cfg: dict[str, Any]) -> None:
        self._interval: float = cfg.get("interval_seconds", 2.0)
        self._event_type: str = cfg.get("event_type", "test.source.tick")
        self._max_events: int = cfg.get("max_events", 5)
        self._persistent: bool = cfg.get("persistent", False)
        self._routing: dict[str, Any] | None = cfg.get("routing")
        self._tick_count: int = 0
        self._state: str = "initialized"
        self._last_success_at: str | None = None
        self._events_published: int = 0
        self._success_count: int = 0
        self._had_failure: bool = False
        self._last_error_summary: str | None = None

        # Dedup configuration (durable, restart-safe via SQLite)
        dedup_cfg = cfg.get("dedup", {})
        if isinstance(dedup_cfg, dict):
            self._dedup_enabled: bool = bool(dedup_cfg.get("enabled", True))
            self._dedup_max: int = int(dedup_cfg.get("max_items", 10000))
        else:
            self._dedup_enabled = True
            self._dedup_max = 10000

        self._source_name: str = cfg.get("source_name", "test_source")
        # Optional startup delay so tests/clients can subscribe or register a
        # consumer before the first event is published (no functional impact).
        self._initial_delay: float = float(cfg.get("initial_delay_seconds", 0))

    @property
    def name(self) -> str:
        return self._source_name

    def status(self) -> dict[str, Any]:
        """Return minimal, truthful status for health reporting."""
        return {
            "name": self._source_name,
            "type": "test_source",
            "enabled": True,
            "state": self._state,
            "last_success_at": self._last_success_at,
            "last_error_at": None,
            "last_error_summary": self._last_error_summary,
            "events_published": self._events_published,
            "tick_count": self._tick_count,
            "interval_seconds": self._interval,
            "max_events": self._max_events,
            "dedup_enabled": self._dedup_enabled,
        }

    async def run(self, publisher: Any, stop_event: asyncio.Event) -> None:
        """Publish deterministic events until max reached or stop signaled."""
        self._state = "running"

        if self._initial_delay > 0:
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=self._initial_delay)
            except asyncio.TimeoutError:
                pass
            if stop_event.is_set():
                self._state = "stopped"
                return

        try:
            while not stop_event.is_set():
                if self._tick_count >= self._max_events:
                    logger.info("test_source '%s': reached max_events=%d — stopping",
                                self._source_name, self._max_events)
                    break

                self._tick_count += 1
                now = datetime.now(timezone.utc)

                # Stable external ID so restarts do not re-publish the same tick.
                ext_id = f"tick-{self._tick_count}"

                # Durable dedup: skip if already published (e.g. after restart).
                if self._dedup_enabled:
                    try:
                        if await publisher.is_seen(self._source_name, ext_id):
                            logger.debug("test_source '%s': tick %d already seen — skip",
                                         self._source_name, self._tick_count)
                            continue
                    except Exception as exc:
                        logger.error("test_source '%s': dedup check failed: %s",
                                     self._source_name, exc)

                event_data: dict[str, Any] = {
                    "tick": self._tick_count,
                    "max": self._max_events,
                    "timestamp": now.isoformat(),
                    "external_id": ext_id,
                }

                try:
                    await publisher(
                        event_type=self._event_type,
                        source=self._source_name,
                        data=event_data,
                        persistent=self._persistent,
                        routing=self._routing,
                    )
                    self._last_success_at = now.isoformat()
                    self._events_published += 1
                    self._success_count += 1
                    if self._dedup_enabled:
                        await publisher.mark_seen(self._source_name, ext_id, self._dedup_max)
                    logger.info(
                        "test_source '%s': tick %d/%d published", self._source_name,
                        self._tick_count, self._max_events,
                    )
                except asyncio.CancelledError:
                    self._state = "stopped"
                    raise
                except Exception as exc:
                    self._last_error_summary = str(exc)[:200]
                    self._had_failure = True
                    logger.error("test_source '%s': publish failed at tick %d: %s",
                                 self._source_name, self._tick_count, exc)
                    self._state = "degraded"
                    # Continue despite error — test source is resilient
                    continue

                # Wait for next interval or stop signal
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=self._interval)
                    break  # stop_event was set
                except asyncio.TimeoutError:
                    pass
        finally:
            # Terminal state must reflect whether the bounded work actually
            # succeeded. "completed" is reserved for a clean run with no
            # unrecovered publication failures. A run that exhausted its events
            # while suffering failures is "degraded" (partial) or "failed"
            # (no successful publications at all) — never "completed".
            if stop_event.is_set():
                self._state = "stopped"
            elif self._tick_count >= self._max_events:
                if self._had_failure and self._success_count == 0:
                    self._state = "failed"
                elif self._had_failure:
                    self._state = "degraded"
                else:
                    self._state = "completed"
            else:
                self._state = "stopped"
