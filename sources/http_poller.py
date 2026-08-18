"""
HTTP JSON Poller — a generic source connector that polls an HTTP endpoint
and publishes events for each new item found.

Uses only Python standard library (urllib.request, json, asyncio.to_thread).
No external dependencies.

Deduplication is durable and restart-safe: seen external IDs are recorded in
SQLite (source_seen_items) via the publisher.  An item is published only if it
has not been seen; on successful publication it is marked seen.  If publication
fails, the item is NOT marked seen and will be retried on the next cycle
(at-least-once ingestion; never silent loss).

An optional durable cursor (ingestion high-water mark) is persisted in the
generic ``source_state`` table under the key ``"cursor"``.  It is restored at
start and advanced only after a successful publish + dedup.  The cursor is a
progress marker / pagination hint only; it never replaces dedup, so it cannot
cause event loss even with overlapping pages or out-of-order data.

URLs and error text are sanitized before being exposed through status/output so
that query strings, fragments, userinfo, and tokens never leak.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import time
import urllib.request
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote, urlsplit, urlunsplit

logger = logging.getLogger(__name__)


def _resolve_env_headers(headers: dict[str, str]) -> dict[str, str]:
    """
    Resolve header values that reference environment variables.

    Convention: if a header value starts with '$', treat the rest as
    an environment variable name.  e.g. "$MY_TOKEN" -> os.environ["MY_TOKEN"].
    Missing env vars are skipped (the header is dropped) with a clear warning.
    """
    resolved: dict[str, str] = {}
    for k, v in headers.items():
        if isinstance(v, str) and v.startswith("$"):
            env_name = v[1:]
            env_val = os.environ.get(env_name)
            if env_val is None:
                logger.warning(
                    "header '%s' references env var '%s' which is not set — skipping",
                    k, env_name,
                )
                continue
            resolved[k] = env_val
        else:
            resolved[k] = v
    return resolved


def _navigate_json(data: Any, path: str) -> Any:
    """Navigate a nested dict/list using a dot-separated path. Empty path returns data as-is."""
    if not path:
        return data
    parts = [p for p in path.split(".") if p]
    current = data
    for part in parts:
        if isinstance(current, dict):
            current = current.get(part)
        else:
            return None
    return current


def _extract_item_id(item: Any, id_path: str) -> str | None:
    """Extract a stable external ID from an item dict."""
    if not id_path:
        return None
    val = _navigate_json(item, id_path)
    if val is None:
        return None
    return str(val)


def sanitize_url(url: str) -> str:
    """
    Return a safe display form of a URL: scheme + host + path only.

    Strips userinfo (user:pass@), query string (?token=...), and fragment (#...).
    Used so source status never leaks secrets through MCP resources/logs.
    """
    if not url:
        return ""
    try:
        parts = urlsplit(url)
    except Exception:
        return ""
    netloc = parts.netloc
    if "@" in netloc:
        netloc = netloc.split("@", 1)[1]
    return urlunsplit((parts.scheme, netloc, parts.path, "", ""))


def _safe_error(message: str, url: str | None = None) -> str:
    """Redact any secret-bearing URL from an error message before storing/displaying it."""
    msg = str(message)
    if url:
        msg = msg.replace(url, sanitize_url(url))
    return msg[:200]


class HttpJsonPoller:
    """
    Generic HTTP JSON polling source.

    Polls a URL, parses JSON, extracts items, deduplicates using external IDs
    (durable + restart-safe), and publishes events through the publisher callable.
    """

    def __init__(self, cfg: dict[str, Any]) -> None:
        self._url: str = cfg.get("url", "")
        self._interval: float = cfg.get("interval_seconds", 60)
        self._timeout: float = cfg.get("timeout_seconds", 10)
        self._max_response_bytes: int = cfg.get("max_response_bytes", 1_048_576)
        self._headers: dict[str, str] = _resolve_env_headers(cfg.get("headers", {}))
        self._item_path: str = cfg.get("item_path", "")
        self._id_path: str = cfg.get("id_path", "id")
        self._timestamp_path: str = cfg.get("timestamp_path", "")
        self._event_type_prefix: str = cfg.get("event_type_prefix", "http_poller")
        self._persistent: bool = cfg.get("persistent", True)
        self._routing: dict[str, Any] | None = cfg.get("routing")
        self._max_retries: int = cfg.get("max_retries", 5)
        self._backoff_base: float = cfg.get("backoff_base", 1.0)
        self._backoff_max: float = cfg.get("backoff_max", 30.0)

        # Dedup configuration (durable, restart-safe via SQLite)
        dedup_cfg = cfg.get("dedup", {})
        if isinstance(dedup_cfg, dict):
            self._dedup_enabled: bool = bool(dedup_cfg.get("enabled", True))
            self._dedup_max: int = int(dedup_cfg.get("max_items", 10000))
        else:
            self._dedup_enabled = True
            self._dedup_max = 10000

        # Cursor persistence (durable ingestion high-water mark in source_state).
        # Reuses existing timestamp_path / id_path fields. Optional; disabled if
        # neither field is configured or it is explicitly turned off. The cursor
        # is a progress marker only — durable dedup (source_seen_items) remains
        # the authoritative duplicate filter, so the cursor is never used as a
        # hard "ignore <= cursor" rule (which would risk dropping late/out-of-order
        # items). The cursor value is stored under the "cursor" key in source_state.
        cursor_cfg = cfg.get("cursor", True)
        if isinstance(cursor_cfg, dict):
            self._cursor_enabled = bool(cursor_cfg.get("enabled", True))
            cursor_path = cursor_cfg.get("path", "auto")  # "auto" | "timestamp" | "id"
            self._cursor_param = cursor_cfg.get("param")   # optional query-param name
            self._cursor_header = cursor_cfg.get("header")  # optional header name
        else:
            self._cursor_enabled = bool(cursor_cfg)
            cursor_path = "auto"
            self._cursor_param = None
            self._cursor_header = None

        if self._cursor_enabled:
            if cursor_path == "timestamp":
                self._cursor_field = "timestamp" if self._timestamp_path else None
            elif cursor_path == "id":
                self._cursor_field = "id"
            else:  # "auto"
                if self._timestamp_path:
                    self._cursor_field = "timestamp"
                elif self._id_path:
                    self._cursor_field = "id"
                else:
                    self._cursor_field = None
            if self._cursor_field is None:
                # No usable field — disable rather than persist meaningless data.
                self._cursor_enabled = False
                self._cursor_param = None
                self._cursor_header = None
        else:
            self._cursor_field = None
            self._cursor_param = None
            self._cursor_header = None

        # In-memory cursor (restored from source_state at run start).
        self._cursor_value: str | None = None

        # Status tracking
        self._state: str = "initialized"
        self._last_success_at: str | None = None
        self._last_error_at: str | None = None
        self._last_error_summary: str | None = None
        self._events_published: int = 0

        # In-memory L1 dedup cache (bounded); durable SQLite is the source of truth.
        self._seen_ids: list[str] = []
        self._seen_max: int = 500

        self._source_name: str = cfg.get("source_name", "http_poller")
        # Optional startup delay so tests/clients can subscribe or register a
        # consumer before the first poll publishes events (no functional impact).
        self._initial_delay: float = float(cfg.get("initial_delay_seconds", 0))

    @property
    def name(self) -> str:
        return self._source_name

    def status(self) -> dict[str, Any]:
        """Return minimal, truthful, sanitized status for health reporting."""
        return {
            "name": self._source_name,
            "type": "http_poller",
            "enabled": True,
            "state": self._state,
            "last_success_at": self._last_success_at,
            "last_error_at": self._last_error_at,
            "last_error_summary": self._last_error_summary,
            "events_published": self._events_published,
            "endpoint": sanitize_url(self._url),
            "interval_seconds": self._interval,
            "dedup_enabled": self._dedup_enabled,
            "cursor_persisted": self._cursor_enabled,
            "cursor": self._cursor_value,
        }

    def _validate_url(self) -> bool:
        """Basic URL safety check (does not log the raw URL)."""
        if not self._url:
            return False
        if not self._url.startswith(("http://", "https://")):
            logger.error("source '%s': URL must start with http:// or https://", self._source_name)
            return False
        return True

    def _fetch_json(self) -> Any:
        """Fetch JSON from the configured URL.  Runs in a thread (blocking I/O)."""
        url = self._url
        headers = dict(self._headers)
        # Optional cursor injection (pagination hint sent to the upstream API).
        # This is a soft hint only — durable dedup remains the authoritative
        # duplicate filter, so a missing/stale cursor never causes event loss.
        if self._cursor_enabled and self._cursor_value:
            if self._cursor_param:
                sep = "&" if "?" in url else "?"
                url = f"{url}{sep}{self._cursor_param}={quote(str(self._cursor_value), safe='')}"
            if self._cursor_header:
                headers[self._cursor_header] = str(self._cursor_value)
        req = urllib.request.Request(url, method="GET")
        for k, v in headers.items():
            req.add_header(k, v)
        if "User-Agent" not in headers:
            req.add_header("User-Agent", "MCP-Event-Server/0.2")

        with urllib.request.urlopen(req, timeout=self._timeout) as resp:
            content_length = resp.headers.get("Content-Length")
            if content_length and int(content_length) > self._max_response_bytes:
                raise ValueError(
                    f"response too large: {content_length} > {self._max_response_bytes}"
                )

            raw = resp.read(self._max_response_bytes + 1)
            if len(raw) > self._max_response_bytes:
                raise ValueError(
                    f"response exceeded size limit: {len(raw)} > {self._max_response_bytes}"
                )

            return json.loads(raw)

    def _extract_items(self, data: Any) -> list[dict[str, Any]]:
        """Navigate to the items list using item_path."""
        items = _navigate_json(data, self._item_path)
        if items is None:
            logger.warning("source '%s': item_path '%s' returned None", self._source_name, self._item_path)
            return []
        if isinstance(items, dict):
            return [items]
        if isinstance(items, list):
            return items
        logger.warning("source '%s': item_path '%s' returned %s (expected list or dict)",
                       self._source_name, self._item_path, type(items).__name__)
        return []

    # --- In-memory L1 dedup helpers (kept as a fast pre-filter; durable is L2) ---

    def _is_duplicate(self, ext_id: str) -> bool:
        return ext_id in self._seen_ids

    def _mark_seen(self, ext_id: str) -> None:
        self._seen_ids.append(ext_id)
        if len(self._seen_ids) > self._seen_max:
            self._seen_ids = self._seen_ids[-self._seen_max:]

    def _extract_cursor_value(self, item: Any) -> str | None:
        """Extract the cursor (high-water mark) value from an item, or None."""
        if self._cursor_field is None:
            return None
        if self._cursor_field == "timestamp":
            val = _navigate_json(item, self._timestamp_path)
        else:  # "id"
            val = _navigate_json(item, self._id_path)
        if val is None:
            return None
        return str(val)

    async def run(self, publisher: Any, stop_event: asyncio.Event) -> None:
        """Main polling loop with retry/backoff and durable deduplication."""
        if not self._validate_url():
            self._state = "failed"
            self._last_error_summary = "invalid URL configuration"
            return

        self._state = "running"

        if self._initial_delay > 0:
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=self._initial_delay)
            except asyncio.TimeoutError:
                pass
            if stop_event.is_set():
                self._state = "stopped"
                return

        # Restore durable cursor (high-water mark) from source_state. Failure to
        # load is non-fatal: the cursor is an optimization, not required for
        # correctness (dedup is authoritative). Never block startup on this.
        if self._cursor_enabled:
            try:
                loaded = await publisher.get_cursor(self._source_name)
                if loaded is not None:
                    self._cursor_value = loaded
                    logger.info("source '%s': restored cursor from source_state: %s",
                                self._source_name, loaded)
            except Exception as exc:
                logger.error("source '%s': failed to load cursor (continuing without it): %s",
                             self._source_name, exc)

        backoff = self._backoff_base

        while not stop_event.is_set():
            try:
                data = await asyncio.to_thread(self._fetch_json)
                items = self._extract_items(data)

                new_count = 0
                for item in items:
                    if not isinstance(item, dict):
                        continue

                    ext_id = _extract_item_id(item, self._id_path)
                    if ext_id is None:
                        # Stable content hash when no explicit external ID is present.
                        ext_id = hashlib.sha256(
                            json.dumps(item, sort_keys=True, ensure_ascii=False).encode()
                        ).hexdigest()[:16]

                    # --- Deduplication (L1 in-memory, then L2 durable) ---
                    if self._dedup_enabled:
                        if self._is_duplicate(ext_id):
                            continue
                        try:
                            already_seen = await publisher.is_seen(self._source_name, ext_id)
                        except Exception as exc:
                            logger.error("source '%s': dedup check failed: %s",
                                         self._source_name, exc)
                            already_seen = False
                        if already_seen:
                            self._mark_seen(ext_id)  # keep L1 in sync with truth
                            continue

                    event_type = f"{self._event_type_prefix}.item.received"
                    event_data: dict[str, Any] = {
                        "external_id": ext_id,
                        "item": item,
                    }
                    if self._timestamp_path:
                        ts_val = _navigate_json(item, self._timestamp_path)
                        if ts_val is not None:
                            event_data["external_timestamp"] = str(ts_val)

                    try:
                        await publisher(
                            event_type=event_type,
                            source=self._source_name,
                            data=event_data,
                            persistent=self._persistent,
                            routing=self._routing,
                        )
                        # Mark seen ONLY after successful publication.
                        if self._dedup_enabled:
                            self._mark_seen(ext_id)  # L1
                            await publisher.mark_seen(self._source_name, ext_id, self._dedup_max)  # L2
                        new_count += 1
                        # Advance durable cursor ONLY after successful publish + dedup.
                        if self._cursor_enabled:
                            cval = self._extract_cursor_value(item)
                            if cval is not None:
                                self._cursor_value = cval
                                try:
                                    await publisher.set_cursor(self._source_name, cval)
                                except Exception as exc:
                                    logger.error(
                                        "source '%s': failed to persist cursor: %s",
                                        self._source_name, exc,
                                    )
                    except Exception as exc:
                        # Publication failed — do NOT mark as seen; retry next cycle.
                        logger.error(
                            "source '%s': publish failed for item %s: %s",
                            self._source_name, ext_id, _safe_error(str(exc), self._url),
                        )
                        break  # stop processing this batch, retry on next cycle

                self._last_success_at = datetime.now(timezone.utc).isoformat()
                self._last_error_summary = None
                self._events_published += new_count

                if new_count > 0:
                    logger.info("source '%s': published %d new event(s)", self._source_name, new_count)

                backoff = self._backoff_base
                self._state = "running"

            except asyncio.CancelledError:
                self._state = "stopped"
                raise
            except Exception as exc:
                self._last_error_at = datetime.now(timezone.utc).isoformat()
                self._last_error_summary = _safe_error(str(exc), self._url)
                self._state = "degraded"  # task still alive, retrying
                logger.error("source '%s': poll failed: %s",
                             self._source_name, _safe_error(str(exc), self._url))

                await asyncio.sleep(min(backoff, self._backoff_max))
                backoff = min(backoff * 2, self._backoff_max)
                continue

            # Sleep until next poll interval (cancellation-aware)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=self._interval)
                break  # stop_event was set
            except asyncio.TimeoutError:
                pass  # normal poll interval elapsed

        self._state = "stopped"
