# Test Runtime Map — MCP Event Server Suite

> **TL;DR** — While fixing ONE feature, run its file (or `--group <name>`) directly.
> Do **not** run the full regression. Every test file is isolated and hard-capped at
> 300 s, so a single file can never stall the suite for hours. Run the whole suite only
> on CI or before a release.

This document maps every feature-focused test file to (a) how many MCP servers it
starts, (b) *why*, and (c) the bounded-wait strategy that keeps it fast. It exists
because the original monoliths (`test_phase8.py`, `integrate_test.py`) spawned up to
~14 server subprocesses per file and used long fixed `sleep()` calls — the root cause
of multi-hour, sometimes-hung test runs.

---

## 1. Harness: `test/run_all.py`

`run_all.py` is the regression driver. It does **not** import the test modules; it runs
each as an **isolated subprocess** with a hard per-file timeout.

| Flag | Meaning |
|------|---------|
| `--group fast\|source\|consumer\|mcp\|lifecycle\|unit\|performance` | run a curated subset (repeatable) |
| `--timeout N` | hard kill time (seconds) per file; default **300.0** |
| `--list-groups` | print groups and exit |

Behavior:
- Each file runs as its own process. A hung file is terminated by the RUNNER, which
  OWNS cleanup via **process-group signaling** (Issue A): Windows `CTRL_BREAK_EVENT` to
  the child's group (graceful) → if it does not exit in a bounded grace window, a
  `taskkill /F /T /PID <child>` tree kill of ONLY the owned hierarchy (the child AND any
  `server.py` it spawned); POSIX `SIGTERM`/`SIGKILL` to the group via `os.killpg`. The
  runner does **NOT** rely on the child's `atexit` (a force-killed process cannot run it).
  A timed-out file is reported as **TIMEOUT** (distinct from FAIL) with PID + cleanup
  diagnostics, and a cleanup failure is surfaced rather than masked as green (§10, §11).
- Per-file and **slowest-3** wall-clock times are printed, so optimization effort is
  data-driven.

`DELETED` files (do **not** recreate, wrap, or re-add to `run_all.py`):
`test/test_phase8.py`, `test/integrate_test.py`.

---

## 2. Server-usage matrix

Legend: **0-SRV** = no MCP server (direct app-object tests); **1-SRV** = exactly one server;
**N-SRV** = multiple servers (distinct configs or intentional restarts).

| File | Servers | Why | Notes |
|------|---------|-----|-------|
| `test_unit_sources.py` | **0-SRV** | pure unit of source classes | fastest |
| `test_acknowledgement.py` | **0-SRV** | `EventStore` + stub bus | ack+advance both called (mirrors the MCP tool) |
| `test_consumers.py` | **0-SRV** | `EventStore` + stub bus | consumer/topic routing |
| `test_background_tasks.py` | **0-SRV** | `BackgroundTaskManager` + `SourceManager` | lifecycle only |
| `test_source_dedup.py` | **0-SRV** | `HttpJsonPoller` + `EventStore` + mock | restart-dedup simulated on one SQLite file |
| `test_source_lifecycle.py` | **0-SRV** + **1-SRV** (S15) | direct `SourceManager`; S15 is the single end-to-end HTTP check | 13 of 14 tests are server-free |
| `test_events.py` | **1-SRV** | one server, `test_source` live | tool-boundary coverage of generate/list/replay |
| `test_timeouts.py` | **1-SRV** (file scope) + s10 own | shared server for P7T5/P7T16; s10 needs a slow mock poller | file-scope server is stopped before s10 starts its own (no orphan) |
| `test_subscriptions.py` | **5-SRV** | each test needs a *different* source config (N1 needs `max_events=1` "exactly one"; N3/N4/S5 need many) | file-scope server removed — it was an orphaned leak |
| `test_errors.py` | **multi** (restarts) | P7T14/P7T15 intentionally stop+restart the server to verify shutdown-during-active-tool/bg recovery | "restart-only for E" case — must NOT be collapsed to one server |
| `test_reconnect.py` | **multi** (restarts) | T10/P7T13/P8T10 verify persistence/state survive restart | restart tests, by design |
| `test_sdk_alignment.py` | **1-SRV** | SDK tool/resource contract | |
| `test_multi_client.py` | **1-SRV** | three concurrent MCP sessions | |
| `test_performance.py` | **1-SRV** | throughput under load | |
| `test_lifespan.py` | **1-SRV** | server lifespan startup/teardown | |

**Design rule (priority #23):** every D-level file that needs a server now uses **one**
server per file *except* where a test must restart the server (errors, reconnect) or
needs a genuinely different source configuration (subscriptions). The old pattern of
"one server per *test* inside a file" was removed where it leaked (subscriptions' file-scope
orphan) or was redundant (timeouts).

---

## 3. Bounded-wait strategy (priority #6)

No test relies on a long fixed `sleep()` to "probably be ready". Readiness and counts are
polled with a timeout:

- `helpers/wait.wait_for_value(getter, expected, timeout, interval, description, eq)` — polls until
  `getter() == expected` or `timeout` elts (raises a clear `timeout` error, test fails **fast**).
- `helpers/wait.wait_until(predicate, timeout, description)` — polls until `predicate()` is truthy.
- `helpers/mcp.call(...)` — every MCP tool call is wrapped in `asyncio.wait_for(..., MCP_CALL_TIMEOUT=30s)`
  so a stalled transport raises `TimeoutError` instead of hanging.
- `helpers/lifecycle.start_server(...)` — fails **fast** if the server process exits during
  startup (no 20 s TCP wait) and is bounded (3 s terminate → hard kill) on shutdown.
- `helpers/lifecycle.wait_mcp_ready(...)` — MANDATORY readiness: process-alive + TCP-open
  (pre-check only) + a **real** MCP op (`initialize` + `ping`) succeeds. TCP-open alone is
  NOT accepted; the call fails at the deadline if the MCP op never succeeds (Issue C).

Server startup/shutdown fast-fail; a server that never becomes ready is killed and reported,
never awaited indefinitely.

---

## 4. How to run a focused check

```bash
# One file, directly (fastest while debugging):
python test/test_acknowledgement.py
python test/test_source_lifecycle.py

# A curated group:
python test/run_all.py --group fast        # 0-server + single-server quick files
python test/run_all.py --group mcp         # MCP-protocol / integration files

# One file with a tighter cap (e.g. 60 s):
python test/run_all.py --group source --timeout 60
```

**Do not run `python test/run_all.py` (full regression) while iterating on one feature.**
Use a group or the single file. The full run is the slow path and is meant for CI/release.

---

## 5. Latent bugs fixed during the optimization pass

These were pre-existing failures that only surfaced once the suite was actually exercised;
recorded here so they are not reintroduced:

1. `helpers/mcp.py` was missing `import time` (used by `wait_source_ready` /
   `wait_for_event_count`) — broke every caller (`test_subscriptions.py`, `test_timeouts.py` s10).
2. `test_acknowledgement.py` / `test_consumers.py` (T5) called `store.acknowledge_event` but
   not `store.advance_checkpoint`. The MCP `acknowledge_event` tool chains both; the direct
   tests now use an `_ack()` helper that does the same, so checkpoints actually advance.
3. `test_consumers.py` used `store.add_consumer_topic` — the real method is `store.add_topic`.
4. `test_consumers.py` / `test_acknowledgement.py` were missing `import asyncio` (NameError at
   `asyncio.run(main())`).
5. `test_subscriptions.py` started an orphaned file-scope server that every test overwrote and
   leaked (port held after the process exited). Removed; each test starts its own config-specific server.
6. `test_timeouts.py` s10 overwrote the file-scope server, orphaning it; now `stop_server()` is
   called before s10 starts its own.
7. `test_source_lifecycle.py` S5/S14 registered the consumer **after** the source published, so
   `consumer_event_state` was never materialized (replay returned nothing). Consumers are now
   registered before `start_all`. S11 raced on `active_count==0` (instant dict-delete in
   `BackgroundTaskManager.cancel`) vs the source loop's terminal state; it now waits for the
   actual `stopped`/`completed` transition.
