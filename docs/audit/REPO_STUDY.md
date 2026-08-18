# Repo Study — `mcp-event` (MCP Event Server)

A self-hosted **MCP (Model Context Protocol) event server** written in Python (asyncio + SQLite, stdlib-only beyond the `mcp` SDK). It is a generic, production-shaped foundation for event-driven MCP servers: external "sources" publish events; consumers subscribe, get live notifications, and replay durable events with acknowledgement/checkpoint semantics.

---

## 1. What it is / purpose

- Exposes an MCP server over **Streamable HTTP** (`http://127.0.0.1:8000/mcp`) built on `mcp>=2.0.0`.
- Single canonical event path (`events.publish_event`) reused by every producer (manual tool + future external sources).
- Durable persistence (SQLite/WAL), topic/target routing, at-least-once delivery, replay/reconnect, ack + monotonic checkpoints, background task supervision, and a pluggable source framework.

## 2. Module map (separation of concerns is the dominant design theme)

| File | Responsibility |
|------|----------------|
| `server.py` | MCP wiring: config load/validate, resources, tools, lifespan, run loop. Knows **nothing** about concrete sources. |
| `events.py` | Event model + `publish_event()` orchestration. Validation, UUIDv4 ids, in-memory history, live subscription notify. No DB/transport knowledge. |
| `store.py` | SQLite layer (`EventStore`, `SCHEMA_VERSION=7`). Persistence, consumers, routing materialization, ack, checkpoints, source_state, durable dedup. |
| `runtime.py` | `AppContext`, `BackgroundTaskManager` (process-owned coroutine supervisor, not MCP Tasks), `make_lifespan`. |
| `errors.py` | Structured error hierarchy (`MCPEventServerError` base). |
| `sources/__init__.py` | `Publisher` (source output port), `EventSource` protocol, `SourceManager`, `build_source_manager`. |
| `sources/registry.py` | **Static** `SOURCE_TYPES` dict (Nuitka-safe; no dynamic import / entry points). |
| `sources/http_poller.py` | Built-in source: polls an HTTP JSON endpoint, dedups, durable + restart-safe, cursor persistence, URL/secret sanitization. |
| `sources/test_source.py` | Built-in deterministic source (timed ticks) for testing/extensibility proof. Disabled by default. |
| `client.py` | Example MCP client: lists tools/resources, subscribes to `event://latest` via `subscriptions/listen`. |
| `config.json` | Runtime config (host/port/timeouts/replay/`sources`). `config.json.bak` is the P8 test config (port 8001, `data_p8`). |
| `test/integrate_test.py` | Regression suite (74 tests, port 8000). |
| `test/test_phase8.py` | Phase 8.1 hardening suite (178 tests, port 8001): dedup, registry, sanitization, live notify, source behavior. |
| `verify_cursor.py`, `verify_phase82.py` | Standalone verification scripts for schema v7 / cursor / dedup / `stop_source`. |
| `Doc/audit/*.md` | FINAL_REPORT, PHASE8_REPORT, VERIFICATION_REPORT — testing/QA write-ups. |

## 3. API surface (`server.py`)

**Resources (4):** `event://latest`, `alerts://pending`, `server://info`, `sources://status`.
**Tools (16):** `ping`, `generate_event` (manual/test), `list_events`, `register_consumer`, `add_consumer_topic`, `list_relevant_events`, `get_pending_events` (replay), `acknowledge_event`, `get_consumer_checkpoint`, `progress_report_test`, `long_running_test`, `background_publish_test`, `list_background_tasks`, `start_test_source`, `start_failing_source`, `stop_test_source`.

## 4. Core data flow

```
source / tool
   │  publish_event(event_type, source, data, persistent, routing)
   ▼
events.py ── validate ──► (persistent? store.save() → SQLite + monotonic sequence)
          ──► update in-memory _event_history/_latest_event
          ──► bus.publish(ResourceUpdated("event://latest"))   ← live notification
```

- **Routing** (frozen at publish time): `None` = broadcast to all consumers; `targets` list; `topics` list (intersect consumer topics). Relevance is **materialized** into `consumer_event_state` on `save()`, so later topic changes don't alter history.
- **Replay/ack**: `get_pending_events` returns unacked, relevant events after the consumer's durable checkpoint. `acknowledge_event` advances the checkpoint to the highest safe sequence (monotonic; gaps block advancement until filled — see tests T5/CP3/CP4).

## 5. Source framework (extensibility seam)

- `build_source_manager(config["sources"])` maps config `"type"` → class from `SOURCE_TYPES`. A bad type raises `SourceConfigError`; sources disabled → MCP still starts.
- `SourceManager` runs each enabled source as a background task (`source:<name>`); `Publisher` is the only way a source emits events and records durable dedup (`is_seen`/`mark_seen`) + cursor (`get_cursor`/`set_cursor`).
- **Durable dedup**: check-before-publish, mark-after-success, never mark on failure → at-least-once, restart-safe. `source_seen_items` table; `prune_source_seen_items` bounds growth.
- **Security**: `sanitize_url` strips userinfo/query/fragment; `status()` only exposes a sanitized `endpoint`; secrets may live in internal `_headers` but never in public output.

## 6. Schema (`store.py`, v7)

`persistent_events` (with `sequence` AUTOINCREMENT), `consumers`, `consumer_topics`, `consumer_event_state`, `consumer_checkpoints`, `source_state` (cursor/state), `source_seen_items` (dedup). Auto-migrates v1→v7 with rollback on error. **Current `data/events.db` is healthy: version 7, all 8 tables present.**

## 7. Testing / quality

- 252 deterministic tests documented (178 Phase 8.1 + 74 regression), all green per `Doc/audit/FINAL_REPORT.md`.
- Strong error handling: failing source doesn't crash server; timeouts/cancellation respected; structured errors surfaced to clients.
- Nuitka-compatible (static registry, no `importlib`/`entry_points`).

## 8. Notable operational observation

`server_err.txt` logged `no such table: persistent_events` / `no such table: consumers` at 14:30. **Root cause (transient, not a code defect):** the running server on port 8000 stayed alive while a test harness (`integrate_test.py`) deleted and recreated `data/events.db` underneath it — the old process's WAL/main-db became inconsistent. The current `data/events.db` is fully healthy (v7, all tables). No production bug; just keep one server process per DB (tests already bind a separate port 8001).

## 9. Hygiene

Stray test DBs at repo root (`_ro_test.db`(+wal/shm), `_t_v7.db`, `_t_v7_12088.db`, `_t_v7_1812_a.db`, `_t_v7_1812_b.db`) are leftover test artifacts, unrelated to runtime. `config.json` is the live config; `config.json.bak` is the P8 test config.

## 10. How to run

```powershell
cd D:\Temp\mcp-event
python server.py                 # serves http://127.0.0.1:8000/mcp
python test/integrate_test.py    # regression (74 tests, deletes data/events.db first)
python test/test_phase8.py       # phase 8.1 suite (178 tests, port 8001)
```
Only dependency: `mcp>=2.0.0,<3.0.0`.
