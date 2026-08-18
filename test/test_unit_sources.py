#!/usr/bin/env python3
"""Unit tests for source connectors — no server required.

Covers:
  * P8-U1: source_state CRUD in store.py
  * P8-U2: HttpJsonPoller unit tests (URL validation, JSON nav, dedup, env headers, sanitized status)
  * P8-U3: SourceManager registration and status
  * P8-U4: create_publisher returns a Publisher with publish + durable dedup helpers
  * R1: known source type creates the correct class
  * R2: two instances work independently
  * R3: unknown source type fails with SourceConfigError
  * R5: server.py has no source-specific imports
  * SEC1: URL sanitization in status
  * SEC2: error message sanitization
  * SEC3: env-secret header not in status
  * D6: dedup isolation across sources
  * D7: dedup pruning

Each test is independently runnable via ``python -m test.test_unit_sources``.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time

# Make the test dir importable so the shared normalizer (mcp_result) resolves,
# regardless of the working directory the suite is launched from.
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_DIR = os.path.dirname(_SCRIPT_DIR)
if _PROJECT_DIR not in sys.path:
    sys.path.insert(0, _PROJECT_DIR)
if _SCRIPT_DIR not in sys.path:
    sys.path.insert(0, _SCRIPT_DIR)

from mcp_result import reserve_free_port  # noqa: E402
from helpers.runner import R  # noqa: E402


# ---------------------------------------------------------------------------
# Unit Tests (P8-U1 through P8-U4)
# ---------------------------------------------------------------------------


# Legacy ID: P8-U1
def test_source_state_crud(runner: R) -> None:
    """P8-U1: source_state CRUD in store.py — get/set/get_all on a fresh DB."""
    name = "P8-U1-source-state-crud"
    import store as store_mod

    test_db = os.path.join(_PROJECT_DIR, "_p8_test_u1.db")
    try:
        es = store_mod.EventStore(test_db)

        result = es.get_source_state("src_a", "cursor")
        runner.assert_eq(name + "-initial", result, None)

        es.set_source_state("src_a", "cursor", "abc123")
        result = es.get_source_state("src_a", "cursor")
        runner.assert_eq(name + "-set", result, "abc123")

        es.set_source_state("src_a", "cursor", "def456")
        result = es.get_source_state("src_a", "cursor")
        runner.assert_eq(name + "-update", result, "def456")

        es.set_source_state("src_a", "last_url", "http://example.com")
        all_state = es.get_all_source_state("src_a")
        runner.assert_eq(name + "-get-all", all_state, {
            "cursor": "def456",
            "last_url": "http://example.com",
        })

        es.set_source_state("src_b", "cursor", "xyz")
        result_a = es.get_source_state("src_a", "cursor")
        result_b = es.get_source_state("src_b", "cursor")
        runner.assert_eq(name + "-isolation-a", result_a, "def456")
        runner.assert_eq(name + "-isolation-b", result_b, "xyz")

        result = es.get_source_state("src_a", "nonexistent")
        runner.assert_eq(name + "-missing-key", result, None)

        result = es.get_source_state("no_such_source", "cursor")
        runner.assert_eq(name + "-missing-source", result, None)
    finally:
        for suffix in ("", "-wal", "-shm", "-journal"):
            p = test_db + suffix
            if os.path.exists(p):
                try:
                    os.remove(p)
                except OSError:
                    pass


# Legacy ID: P8-U2
def test_http_json_poller_unit(runner: R) -> None:
    """P8-U2: HttpJsonPoller unit tests — URL validation, item extraction, dedup, env header resolution, sanitized status."""
    name = "P8-U2-http-poller"
    from sources.http_poller import (
        HttpJsonPoller,
        _navigate_json,
        _resolve_env_headers,
        _extract_item_id,
        sanitize_url,
        _safe_error,
    )

    p = HttpJsonPoller({"url": ""})
    runner.assert_true(name + "-empty-url", not p._validate_url(), "empty URL should fail")

    p = HttpJsonPoller({"url": "ftp://example.com"})
    runner.assert_true(name + "-bad-scheme", not p._validate_url(), "ftp URL should fail")

    p = HttpJsonPoller({"url": "https://example.com/api"})
    runner.assert_true(name + "-good-https", p._validate_url(), "https URL should pass")

    p = HttpJsonPoller({"url": "http://localhost:9999/items"})
    runner.assert_true(name + "-good-http", p._validate_url(), "http URL should pass")

    data = {"a": {"b": {"c": 42}}, "list": [10, 20, 30]}
    runner.assert_eq(name + "-nav-root", _navigate_json(data, ""), data)
    runner.assert_eq(name + "-nav-nested", _navigate_json(data, "a.b.c"), 42)
    runner.assert_eq(name + "-nav-miss", _navigate_json(data, "a.b.d"), None)
    runner.assert_eq(name + "-nav-list", _navigate_json(data, "list"), [10, 20, 30])
    runner.assert_eq(name + "-nav-deep-miss", _navigate_json(data, "x.y.z"), None)

    item = {"id": "abc", "nested": {"id": "xyz"}}
    runner.assert_eq(name + "-eid-direct", _extract_item_id(item, "id"), "abc")
    runner.assert_eq(name + "-eid-nested", _extract_item_id(item, "nested.id"), "xyz")
    runner.assert_eq(name + "-eid-missing", _extract_item_id(item, "missing"), None)
    runner.assert_eq(name + "-eid-empty", _extract_item_id(item, ""), None)

    p = HttpJsonPoller({})
    runner.assert_true(name + "-dedup-fresh", not p._is_duplicate("id1"), "fresh ID not duplicate")
    p._mark_seen("id1")
    runner.assert_true(name + "-dedup-seen", p._is_duplicate("id1"), "seen ID is duplicate")
    runner.assert_true(name + "-dedup-other", not p._is_duplicate("id2"), "other ID not duplicate")

    for i in range(501):
        p._mark_seen(f"bulk-{i}")
    runner.assert_true(name + "-dedup-evict", not p._is_duplicate("bulk-0"),
                       "oldest ID should be evicted after 500+ marks")
    runner.assert_true(name + "-dedup-recent", p._is_duplicate("bulk-500"),
                       "most recent ID still in dedup list")

    os.environ["_P8_TEST_HDR"] = "resolved-value"
    headers = _resolve_env_headers({"Authorization": "$_P8_TEST_HDR", "Static": "plain"})
    runner.assert_eq(name + "-env-resolved", headers.get("Authorization"), "resolved-value")
    runner.assert_eq(name + "-env-static", headers.get("Static"), "plain")

    os.environ.pop("_P8_TEST_HDR", None)
    headers = _resolve_env_headers({"Authorization": "$_P8_NONEXISTENT_VAR"})
    runner.assert_true(name + "-env-missing", "Authorization" not in headers,
                       "unset var header should be skipped")

    # --- sanitized status (no raw URL / secrets) ---
    p = HttpJsonPoller({"url": "https://user:pass@example.com/api?token=secret#frag",
                        "source_name": "my_poller"})
    st = p.status()
    runner.assert_eq(name + "-status-name", st["name"], "my_poller")
    runner.assert_eq(name + "-status-endpoint", st["endpoint"], "https://example.com/api")
    runner.assert_eq(name + "-status-state", st["state"], "initialized")
    runner.assert_eq(name + "-status-events", st["events_published"], 0)
    runner.assert_true(name + "-status-no-url-key", "url" not in st,
                       "raw 'url' key must not be exposed")
    runner.assert_true(name + "-status-no-secret",
                       "secret" not in json.dumps(st) and "pass" not in json.dumps(st),
                       "status must not contain secret/userinfo")
    runner.assert_true(name + "-status-keys",
                       {"name", "type", "enabled", "state", "last_success_at",
                        "last_error_at", "last_error_summary", "events_published",
                        "endpoint", "interval_seconds", "dedup_enabled"}
                       <= set(st.keys()),
                       f"missing status keys: {set(st.keys())}")

    # --- sanitize_url / _safe_error unit checks ---
    runner.assert_eq(name + "-sanitize-url",
                     sanitize_url("https://u:p@host/x/y?t=1#f"),
                     "https://host/x/y")
    runner.assert_eq(name + "-safe-error",
                     _safe_error("boom https://u:p@host/x?t=1#f", "https://u:p@host/x?t=1#f"),
                     "boom https://host/x")


# Legacy ID: P8-U3
def test_source_manager_registration(runner: R) -> None:
    """P8-U3: SourceManager registration and status."""
    name = "P8-U3-source-manager"
    from sources import SourceManager
    from sources.test_source import TestSource

    sm = SourceManager()
    runner.assert_eq(name + "-init-empty", len(sm._sources), 0)

    ts = TestSource({"interval_seconds": 1, "max_events": 2})
    sm.register(ts)
    runner.assert_eq(name + "-registered", len(sm._sources), 1)
    runner.assert_in(name + "-has-test", "test_source", sm._sources)

    ts2 = TestSource({"interval_seconds": 5, "max_events": 10})
    sm.register(ts2)
    runner.assert_eq(name + "-overwrite-count", len(sm._sources), 1)
    runner.assert_eq(name + "-overwrite-val", sm._sources["test_source"]._interval, 5)

    status = sm.get_status()
    runner.assert_true(name + "-status-dict", isinstance(status, dict), "status should be dict")
    runner.assert_in(name + "-status-has", "test_source", status)
    runner.assert_eq(name + "-status-state", status["test_source"]["state"], "initialized")

    sources = sm.enabled_sources
    runner.assert_eq(name + "-enabled-count", len(sources), 1)
    runner.assert_in(name + "-enabled-has", "test_source", sources)


# Legacy ID: P8-U4
async def test_create_publisher(runner: R) -> None:
    """P8-U4: create_publisher returns a Publisher with publish + durable dedup helpers."""
    name = "P8-U4-create-publisher"
    from sources import create_publisher
    import store as store_mod
    from mcp.server.subscriptions import InMemorySubscriptionBus

    test_db = os.path.join(_PROJECT_DIR, "_p8_test_u4.db")
    try:
        es = store_mod.EventStore(test_db)
        bus = InMemorySubscriptionBus()
        publisher = create_publisher(es, bus)
        runner.assert_true(name + "-callable", callable(publisher), "publisher should be callable")

        event = await publisher(
            event_type="test.p8.u4",
            source="unit_test",
            data={"hello": "world"},
            persistent=False,
        )
        runner.assert_true(name + "-returns-dict", isinstance(event, dict), "publisher returns dict")
        runner.assert_eq(name + "-type", event.get("type"), "test.p8.u4")
        runner.assert_eq(name + "-source", event.get("source"), "unit_test")
        runner.assert_true(name + "-has-id", bool(event.get("id")), "event has id")
        runner.assert_true(name + "-no-seq", event.get("sequence") is None,
                           "transient event has no sequence")

        event_p = await publisher(
            event_type="test.p8.u4.persist",
            source="unit_test",
            data={"persist": True},
            persistent=True,
        )
        runner.assert_true(name + "-persistent", event_p.get("persistent") is True,
                           "persistent event flag")
        runner.assert_true(name + "-has-seq", event_p.get("sequence") is not None,
                           "persistent event has sequence")

        # dedup helpers
        runner.assert_false(name + "-seen-before", await publisher.is_seen("u4", "X"))
        await publisher.mark_seen("u4", "X")
        runner.assert_true(name + "-seen-after", await publisher.is_seen("u4", "X"))
        runner.assert_false(name + "-seen-other", await publisher.is_seen("u4", "Y"))
    finally:
        for suffix in ("", "-wal", "-shm", "-journal"):
            p = test_db + suffix
            if os.path.exists(p):
                try:
                    os.remove(p)
                except OSError:
                    pass


# ===================================================================
# Source Registry Tests (R1..R5)
# ===================================================================


# Legacy ID: R1
def test_registry_known_type(runner: R) -> None:
    """R1: known source type creates the correct class."""
    name = "R1-registry-known-type"
    from sources import build_source_manager
    from sources.http_poller import HttpJsonPoller
    from sources.test_source import TestSource

    m = build_source_manager({"a": {"type": "http_poller", "url": "https://x.com"},
                               "b": {"type": "test_source"}})
    runner.assert_eq(name + "-http-class", m._sources["a"].__class__.__name__, "HttpJsonPoller")
    runner.assert_eq(name + "-test-class", m._sources["b"].__class__.__name__, "TestSource")


# Legacy ID: R2
def test_registry_two_instances(runner: R) -> None:
    """R2: two instances of the same type work independently with distinct runtime names."""
    name = "R2-registry-two-instances"
    from sources import build_source_manager

    m = build_source_manager({
        "feed_one": {"type": "http_poller", "url": "https://a.com"},
        "feed_two": {"type": "http_poller", "url": "https://b.com"},
    })
    runner.assert_eq(name + "-count", len(m._sources), 2)
    runner.assert_eq(name + "-name-one", m._sources["feed_one"].name, "feed_one")
    runner.assert_eq(name + "-name-two", m._sources["feed_two"].name, "feed_two")
    runner.assert_eq(name + "-url-one", m._sources["feed_one"]._url, "https://a.com")
    runner.assert_eq(name + "-url-two", m._sources["feed_two"]._url, "https://b.com")


# Legacy ID: R3
def test_registry_unknown_type(runner: R) -> None:
    """R3: unknown source type fails clearly with SourceConfigError."""
    name = "R3-registry-unknown-type"
    from sources import build_source_manager, SourceConfigError

    try:
        build_source_manager({"bad": {"type": "does_not_exist"}})
        runner.fail(name, "expected SourceConfigError")
    except SourceConfigError as exc:
        runner.assert_true(name + "-raises", "does_not_exist" in str(exc),
                           f"error should name the bad type: {exc}")


# Legacy ID: R5
def test_server_no_source_imports(runner: R) -> None:
    """R5: adding a built-in source requires NO source-specific import in server.py."""
    name = "R5-registry-no-server-import"
    import server as server_mod
    runner.assert_false(name + "-no-http-class",
                        hasattr(server_mod, "HttpJsonPoller"),
                        "server.py must not import HttpJsonPoller")
    runner.assert_false(name + "-no-test-class",
                        hasattr(server_mod, "TestSource"),
                        "server.py must not import TestSource")
    # server.py must still build a source manager via the subsystem
    runner.assert_true(name + "-has-manager",
                       hasattr(server_mod, "_source_manager"),
                       "server.py should have a _source_manager")


# ===================================================================
# URL / Secret Sanitization Tests (SEC1..SEC3)
# ===================================================================


# Legacy ID: SEC1
def test_url_sanitized(runner: R) -> None:
    """SEC1: secret-bearing URL is sanitized in public status output."""
    name = "SEC1-url-sanitized"
    from sources.http_poller import HttpJsonPoller, sanitize_url

    secret_url = "https://user:pass@example.com/api/path?token=SECRET#frag"
    p = HttpJsonPoller({"url": secret_url, "source_name": "sec1"})
    st = p.status()
    runner.assert_eq(name + "-endpoint", st["endpoint"], "https://example.com/api/path")
    dumped = json.dumps(st)
    for leak in ("user", "pass", "SECRET", "token", "frag", "example.com/api/path?token"):
        runner.assert_not_in(name + f"-no-leak-{leak}", leak, dumped)
    # direct sanitize_url check
    runner.assert_eq(name + "-sanitize", sanitize_url(secret_url), "https://example.com/api/path")


# Legacy ID: SEC2
def test_error_message_sanitized(runner: R) -> None:
    """SEC2: an error message containing a raw URL is sanitized before storage."""
    name = "SEC2-error-sanitized"
    from sources.http_poller import _safe_error

    raw = "URLError: <urlopen error [Errno 8] nodename nor servname provided, or not known> https://user:pass@example.com/api?token=SECRET#frag"
    safe = _safe_error(raw, "https://user:pass@example.com/api?token=SECRET#frag")
    runner.assert_not_in(name + "-no-secret", "SECRET", safe)
    runner.assert_not_in(name + "-no-userinfo", "user:pass", safe)
    runner.assert_in(name + "-sanitized-present", "https://example.com/api", safe)


# Legacy ID: SEC3
def test_env_secret_not_in_status(runner: R) -> None:
    """SEC3: an environment-secret header value never appears in public status."""
    name = "SEC3-env-secret"
    from sources.http_poller import HttpJsonPoller

    os.environ["_P8_SECRET_TOKEN"] = "SUPER_SECRET_VALUE_123"
    try:
        p = HttpJsonPoller({
            "url": "https://api.example.com",
            "source_name": "sec3",
            "headers": {"Authorization": "$_P8_SECRET_TOKEN"},
        })
        st = p.status()
        dumped = json.dumps(st)
        runner.assert_not_in(name + "-no-secret-in-status", "SUPER_SECRET_VALUE_123", dumped)
        # The resolved secret is allowed to live in the internal _headers used for
        # the actual HTTP request; the security boundary is the public status().
        runner.assert_true(name + "-secret-in-headers-internal",
                           "SUPER_SECRET_VALUE_123" in json.dumps(getattr(p, "_headers", {})),
                           "resolved secret should be present in internal request headers")
    finally:
        os.environ.pop("_P8_SECRET_TOKEN", None)


# ===================================================================
# Dedup unit tests (D6, D7)
# ===================================================================


# Legacy ID: D6
def test_dedup_isolation_across_sources(runner: R) -> None:
    """D6: same external ID under two different sources is tracked independently."""
    name = "D6-dedup-isolation"
    import store as store_mod
    test_db = os.path.join(_PROJECT_DIR, "_p8_test_d6.db")
    try:
        es = store_mod.EventStore(test_db)
        es.mark_source_item_seen("src_a", "same_id", "2026-01-01")
        es.mark_source_item_seen("src_b", "same_id", "2026-01-01")
        runner.assert_true(name + "-a-seen", es.source_item_seen("src_a", "same_id"))
        runner.assert_true(name + "-b-seen", es.source_item_seen("src_b", "same_id"))
        runner.assert_false(name + "-a-other", es.source_item_seen("src_a", "other_id"))
        runner.assert_false(name + "-b-other", es.source_item_seen("src_b", "other_id"))
    finally:
        for suffix in ("", "-wal", "-shm", "-journal"):
            p = test_db + suffix
            if os.path.exists(p):
                try:
                    os.remove(p)
                except OSError:
                    pass


# Legacy ID: D7
def test_dedup_pruning(runner: R) -> None:
    """D7: dedup pruning keeps recent IDs, removes oldest when over limit."""
    name = "D7-dedup-pruning"
    import store as store_mod
    test_db = os.path.join(_PROJECT_DIR, "_p8_test_d7.db")
    try:
        es = store_mod.EventStore(test_db)
        for i in range(20):
            es.mark_source_item_seen("src", f"id{i:02d}", f"2026-01-01T00:00:{i:02d}Z")
        deleted = es.prune_source_seen_items("src", 10)
        conn = __import__("sqlite3").connect(test_db)
        cnt = conn.execute(
            "SELECT COUNT(*) FROM source_seen_items WHERE source_name=?", ("src",)
        ).fetchone()[0]
        conn.close()
        runner.assert_eq(name + "-deleted", deleted, 10)
        runner.assert_eq(name + "-remaining", cnt, 10)
        # oldest (id00) removed, newest (id19) preserved
        runner.assert_false(name + "-oldest-removed", es.source_item_seen("src", "id00"))
        runner.assert_true(name + "-newest-kept", es.source_item_seen("src", "id19"))
    finally:
        for suffix in ("", "-wal", "-shm", "-journal"):
            p = test_db + suffix
            if os.path.exists(p):
                try:
                    os.remove(p)
                except OSError:
                    pass


# ===================================================================
# Main
# ===================================================================


async def main() -> bool:
    runner = R()

    # Unit tests (no server)
    test_source_state_crud(runner)
    test_http_json_poller_unit(runner)
    test_source_manager_registration(runner)
    await test_create_publisher(runner)

    # Source registry
    test_registry_known_type(runner)
    test_registry_two_instances(runner)
    test_registry_unknown_type(runner)
    test_server_no_source_imports(runner)

    # URL / secret sanitization
    test_url_sanitized(runner)
    test_error_message_sanitized(runner)
    test_env_secret_not_in_status(runner)

    # Dedup unit tests
    test_dedup_isolation_across_sources(runner)
    test_dedup_pruning(runner)

    return runner.summary()


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
