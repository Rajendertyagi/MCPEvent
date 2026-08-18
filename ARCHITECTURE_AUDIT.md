# Architecture Audit — MCP Event Server

**Mode:** Architectural audit (read of current disk + installed MCP SDK 2.0.0).
**Date:** 2026-08-18
**Scope:** Whole repository `D:\Temp\mcp-event` — server, store, runtime, sources, tools, tests, harness.

> Plain-language note: this document is written for a non-technical reader. "Module" = a
> self-contained part of the program. "Trade-off" = what you gain vs what you give up.
> "ADR" = a recorded design decision. Ratings use Strong / Adequate / Limited / Unverified.

---

## 1. Verdict (read this first)

**The architecture is sound and production-quality for its stated purpose: a single-host,
self-hosted MCP event server.** The module boundaries are clean, the live-notification path
is correctly wired to the SDK, timeouts and cleanup are bounded, and the test harness prevents
orphaned processes.

There are **no architectural blockers**. The only must-fix is a **stale sentence in an existing
report** (`HARNESS_FOLLOWUP_REPORT.md` §39) that mislabels the current store as a violation — it
is not. The remaining items are evolution work (broker integration, scaling, packaging), not defects.

---

## 2. What the system actually is

One Python program that:
1. Exposes an MCP server over Streamable HTTP (the "door" clients talk to).
2. Publishes events through a single code path (`publish_event`).
3. Stores persistent events in SQLite (durable) and keeps a small recent-history buffer in memory.
4. Notifies subscribed clients live when something changes (`ResourceUpdated` → `Client.listen`).
5. Supports consumer identities, topic routing, acknowledgement, and replay/checkpoint.
6. Can pull events from external systems via pluggable "sources" (HTTP poller, test source).

It is a **modular monolith**: one process, internally split into clear sections. It is also
**event-driven** inside that process (publish → notify → listen).

---

## 3. How it is put together (containers)

| Section | File(s) | Job |
|---------|---------|-----|
| Door / MCP wiring | `server.py`, `mcp` SDK | Transport, tool/resource registration, run loop |
| Event core | `events.py` | Validation, ID, persistence trigger, live notify |
| Store | `store.py` (facade) + `store_modules/` | Durable SQLite: events, consumers, delivery, replay, source state |
| Runtime | `runtime.py` | Process-wide state (`AppContext`), background-task supervisor, lifespan |
| Sources | `sources/` | External ingest (HTTP poller, test source) via a single `Publisher` port |
| Config | `server_modules/config.py`, `config.json` | Load + validate settings, refuse unsafe binds |

The dependency direction is correct: `server.py` depends on the sections; the sections do **not**
reach back into MCP transport. Sources and tools both call the *same* `publish_event` — one path,
no duplication.

---

## 4. Quality-attribute assessment (with trade-offs)

### Reliability — **Strong**
- Timeouts are bounded everywhere (tool calls, DB ops, background shutdown, per-test file).
- Live notification path awaits the SDK bus correctly (`events.py:83`), so clients actually get told.
- Source ingest is at-least-once: a failed publish is **not** marked "seen", so it retries next cycle.
- **Trade-off given up:** in-memory recent-events buffer is lost on restart. Only *persistent* events
  survive (by design). If a client needs the last N non-persistent events after a crash, it won't get them.

### Maintainability — **Strong**
- Facade + module split is clean; each store submodule imports its own `datetime` (the earlier
  regression was isolated to one file and is fixed).
- Errors are plain, structured exceptions; the SDK wraps them. `MCPError` is reserved, never misused.

### Scalability — **Limited**
- Single process, single in-process store and in-process notification bus. Fine for one host.
- **Trade-off given up:** you cannot run two copies sharing load — the bus and store live inside the
  process. Scaling out needs a shared store (e.g. Postgres) and a shared bus (e.g. Redis). Not needed yet.

### Observability — **Adequate**
- Structured errors, logs, server `info` resource, source `status` resource.
- **Trade-off given up:** no metrics (counters/gauges) or distributed tracing. For one host, acceptable.

### Security — **Adequate (loopback-only today)**
- Binds to `127.0.0.1` only; config refuses `0.0.0.0`/`::`. For loopback the SDK auto-applies transport security.
- **Trade-off given up:** no authn/authz. Before exposing to a broker/network, add `allowed_hosts`/
  `allowed_origins`/`transport_security` and an auth layer.

### Testability — **Strong**
- Harness uses real MCP readiness (initialize + `ping`), bounded per-file timeout, parent-owned
  process-group cleanup, distinct `TIMEOUT` vs `FAIL`. Orphan risk is low.

### Nuitka / packaging portability — **Unverified**
- Code is Nuitka-friendly (static source registry, stdlib-only poller, no dynamic import scanning).
- **Trade-off given up:** the `mcp` SDK itself must be bundled into the frozen binary; this has not
  been proven with a real Nuitka build yet.

---

## 5. Failure-mode challenges ("what happens when X fails?")

- **External source publish fails** → item not marked seen → retried next poll. No silent loss. ✅
- **SQLite write fails** → `publish_event` raises; caller gets a clear error; event not stored. Acceptable.
- **Client drops mid-listen** → SDK raises `SubscriptionLost`; client re-listens and re-reads. No replay
  by design (level-trigger model). ✅
- **Process hard-killed** → background tasks are shielded (not awaited); sources restart next run.
  Acceptable for a single instance; would need orchestration for multi-instance.
- **Port already in use (WinError 10048)** → `reserve_free_port()` + log parsing picks/confirms the
  real port; low risk. Mitigated.
- **A bad background task raises** → isolated; does not kill the server (`sources/__init__.py` wrapper). ✅

---

## 6. Key design decisions (ADRs, plain language)

**ADR-001 — Transport = Streamable HTTP, stateless + JSON responses.**
- Gain: simple, firewall-friendly, no server session to manage.
- Give up: no server-side session; a "consumer" is identified by the client, not by a server session.
  Reconnect/replay is rebuilt from the durable store, not from memory.

**ADR-002 — Store is a thin facade over `store_modules/`.**
- Gain: clear boundary; schema migrations v1→v7 live in one place; business logic not duplicated.
- Give up: one extra layer of indirection (minor).

**ADR-003 — `publish_event` is Context-free (store/bus injected).**
- Gain: every writer (tool, source, background) uses the exact same path; no per-request state leaks
  into the event core; easy to test.
- Give up: callers must pass `store`/`bus` explicitly (intentional, makes dependencies visible).

**ADR-004 — In-process `InMemorySubscriptionBus`.**
- Gain: zero infrastructure, fast, no network hop for notifications.
- Give up: notifications cannot cross process/host boundaries. Single-instance only (see Scalability).

**ADR-005 — Static source registry (no plugin discovery).**
- Gain: Nuitka onefile/standalone safe; behavior is predictable and reviewable.
- Give up: adding a new source type is a code change, not a drop-in plugin file.

---

## 7. Risk register (prioritized)

| # | Risk | Severity | Action |
|---|------|----------|--------|
| P1 | `HARNESS_FOLLOWUP_REPORT.md` §39 calls the current store a "violation" — it is the expected baseline | Doc only | Fix wording (Craft) |
| P2 | In-memory recent-events buffer is non-durable | Low (by design) | Confirm intent before broker; document |
| P3 | Single-instance only (bus + store in-process) | Medium (future) | Plan shared store/bus if scaling out |
| P4 | Nuitka bundling of `mcp` SDK unverified | Medium (future) | Do a Nuitka smoke build |
| P5 | `alerts://pending` == "all persistent events" (semantic overlap) | Low | Split or document before broker |

---

## 8. Recommended evolution (next steps)

1. **Fix P1** — correct the stale §39 wording in the harness report.
2. **Prove a real source** — enable `http_poller` against a real endpoint to exercise the `Publisher` seam end-to-end.
3. **Define the broker contract** — event schema, auth, and what "alert" means distinctly from "persistent event".
4. **Harden for network** — add `allowed_hosts`/`allowed_origins`/`transport_security` + auth before any non-loopback exposure.
5. **Nuitka smoke build** — confirm the frozen binary bundles `mcp` and starts.

## 9. What NOT to do

- **Do not** start S2/S3 store refactoring or any broad refactor now — the baseline is frozen and verified.
- **Do not** re-run the full `--group all` regression unless explicitly asked (use `--group fast mcp`).
- **Do not** jump to microservices — a modular monolith is the right call for this team/size today.

---

*Audit complete. No production code was modified; this report is advisory. One documentation fix
(P1) is recommended and can be applied in Craft mode.*
