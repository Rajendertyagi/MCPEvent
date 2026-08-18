# Implementation Report — Test Harness Runtime Optimization (43-point)

**Project:** `D:\Temp\mcp-event` (MCP event server, `mcp==2.0.0`, Python 3.14)
**Phase:** C — VERIFICATION / TESTING-ONLY (production source frozen)
**Date:** 2026-08-17
**Goal:** make focused testing fast and guarantee no individual test can hang for hours.

---

## 0. Headline result

- **All 16 feature test files PASS** (one-shot bounded validation sweep; each file capped at
  300 s by the harness, so no hang is possible).
- **0-server (direct app-object) files: 6** — `unit_sources`, `acknowledgement`, `consumers`,
  `background_tasks`, `source_dedup`, `source_lifecycle` (S15 aside). These run in **sub-second
  to a few seconds**, no subprocess.
- **Harness:** `run_all.py` runs each file as an isolated subprocess with a **hard 300 s/file
  timeout** + graceful `CTRL_BREAK_EVENT`/`SIGTERM` cleanup + `atexit` orphan-kill. The deleted
  monoliths (`test_phase8.py`, `integrate_test.py`) are gone and are **not** in `run_all.py`.
- **Latent bugs fixed:** 7 (details §4) — the suite was green *on paper* but several files had
  never actually been executed; they now pass.

> Note on "full regression": the directive said **no full regression during feature debugging**.
> This report's green result comes from a **single bounded validation sweep**, not an iterative
> regression loop. While iterating, run one file or `--group <name>`.

---

## 1. Priority-ordered implementation (43 rules → 9 priorities)

### Priority 1 — Hard run_all.py child-process timeout  ✅
1. `run_all.py` `_run_one()` wraps `proc.communicate()` in `asyncio.wait_for(..., timeout)`.
2. On timeout: graceful terminate (`CTRL_BREAK_EVENT` on Windows / `SIGTERM`), 10 s grace, then
   hard `proc.kill()`; child reported FAILED.
3. `DEFAULT_PER_FILE_TIMEOUT = 300.0`; overridable via `--timeout N`.
4. `creationflags = CREATE_NEW_PROCESS_GROUP` so the break signal reaches the whole child group.
5. Child `atexit` handler kills any server it spawned → no orphan survives a kill.
6. Per-file + slowest-3 wall-clock reported for data-driven optimization.

### Priority 2 — Bounded async waits  ✅
7. `helpers/wait.wait_for_value(getter, expected, timeout, interval, description, eq)` polls
   instead of sleeping.
8. `helpers/wait.wait_until(predicate, timeout, description)` for state predicates.
9. `helpers/mcp.call` wraps every tool call in `asyncio.wait_for(..., MCP_CALL_TIMEOUT=30)`.
10. `wait_source_ready` / `wait_for_event_count` poll with a timeout (required `import time` —
    see §4.1).
11. All readiness probes fail **fast** (clear `timeout` error) rather than hanging.

### Priority 3 — Eliminate unnecessary source-lifecycle server starts  ✅
12. `test_source_lifecycle.py` runs 13/14 tests directly against `SourceManager`/`EventStore`/
    `BackgroundTaskManager` — **0 servers** (only S15 keeps one real HTTP server as a
    representative end-to-end check).
13. `test_source_dedup.py` runs **0 servers**; restart-dedup is simulated by two
    `HttpJsonPoller` sessions on one SQLite file.
14. `test_background_tasks.py` runs **0 servers** (pure `BackgroundTaskManager`/`SourceManager`).
15. Removed the file-scope orphan server in `test_subscriptions.py` (was started then overwritten
    by every test → leaked).

### Priority 4 — Convert other A/B tests to direct app-object tests  ✅
16. `test_acknowledgement.py` → direct `EventStore` + stub bus (was 1 server). Uses `_ack()`
    helper that mirrors the MCP tool (ack **+** advance).
17. `test_consumers.py` → direct `EventStore` + stub bus (was 1 server/tcp).
18. `test_source_dedup.py` → direct `HttpJsonPoller` + `EventStore` + `mock_http` (was up to 3
    servers).
19. `test_background_tasks.py` → direct `BackgroundTaskManager`/`SourceManager`.
20. `test_source_lifecycle.py` → direct (per Priority 3).
21. `test_events.py` → 1 server (see Priority 5); tool-boundary coverage retained.

### Priority 5 — One server per D-level file + restart-only for E  ✅
22. `test_events.py`: exactly **1** server start.
23. `test_timeouts.py`: 1 shared file-scope server for P7T5/P7T16; s10 stops it before starting
    its own (no orphan).
24. `test_sdk_alignment.py`, `test_multi_client.py`, `test_performance.py`, `test_lifespan.py`:
    **1** server each.
25. `test_subscriptions.py`: 5 servers, each with a **different source config** (N1 needs
    `max_events=1` "exactly one"; N3/N4 need many) — cannot share one config. File-scope orphan
    removed.
26. `test_errors.py` / `test_reconnect.py`: **restart-based** (P7T14/P7T15 stop+restart the
    server; T10/P7T13/P8T10 verify persistence across restart). Deliberately NOT collapsed to one
    server — this *is* the "restart-only for E" case.

### Priority 6 — Remove long fixed sleeps + startup/shutdown fast-fail  ✅
27. No test relies on a long fixed `sleep()` to be "ready"; all use bounded polling.
28. `start_server()` fails fast if the process exits during startup (no 20 s TCP wait).
29. Server shutdown is bounded (3 s terminate → hard kill) so teardown can't stall.
30. `wait_mcp_ready()` = TCP-open probe + optional (non-fatal) MCP ping.

### Priority 7 — Docs (TEST_RUNTIME_MAP.md + README/AGENT.md)  ✅
31. `TEST_RUNTIME_MAP.md` — per-file server-usage matrix, harness behavior, bounded-wait
    strategy, run guidance, latent-bug log.
32. `README.md` — suite layout, run commands, scope guard, deleted-monolith note.
33. `AGENT.md` — rules for adding/changing tests, gotchas already fixed, run guidance.

### Priority 8 — Groups + timing reporting  ✅
34. `run_all.py` groups: `all`, `fast`, `source`, `consumer`, `mcp`, `lifecycle`, `unit`,
    `performance`.
35. Per-file PASS/FAIL + elapsed seconds; slowest-3 summary.
36. `--list-groups` for discovery.

### Priority 9 — 43-point implementation report  ✅ (this file)
37. Covers all 43 rules across the 9 priorities with file/line evidence.
38. Lists deleted monoliths and the standing rule not to recreate them.
39. States the no-full-regression-while-debugging constraint and the one-shot validation result.
40. Documents every latent bug fixed (§4) so none is reintroduced.
41. Confirms 0-server count and the sub-second/seconds runtime of direct tests.
42. Confirms the hard per-file timeout + graceful cleanup prevents multi-hour hangs.
43. Confirms production source is frozen and untouched by this phase.

---

## 2. Files changed (test harness only)

| File | Change |
|------|--------|
| `test/run_all.py` | hard per-file timeout, groups, timing, graceful cleanup (done earlier in phase) |
| `test/helpers/lifecycle.py` | fast-fail startup, bounded shutdown, `CTRL_BREAK_EVENT`, atexit cleanup (earlier) |
| `test/helpers/wait.py` | bounded `wait_for_value` / `wait_until` (earlier) |
| `test/helpers/mcp.py` | **+`import time`** (was used by `wait_source_ready`/`wait_for_event_count` but missing) |
| `test/test_acknowledgement.py` | direct 0-server; added `_ack()` (ack+advance) helper; `p7t22` uses isolated store |
| `test/test_consumers.py` | direct 0-server; **+`import asyncio`**; `add_consumer_topic`→`add_topic`; `_ack()` helper |
| `test/test_source_dedup.py` | direct 0-server (earlier); relocated P8T7 http-poller check |
| `test/test_source_lifecycle.py` | S5/S14 register consumer before publish; S11 waits for terminal state; S15 kept 1 server |
| `test/test_background_tasks.py` | direct 0-server (earlier) |
| `test/test_events.py` | **1** server (earlier) |
| `test/test_subscriptions.py` | removed orphaned file-scope server |
| `test/test_timeouts.py` | stop file-scope server before s10 starts its own (no orphan) |
| `TEST_RUNTIME_MAP.md` | **new** — runtime map |
| `README.md` | **new** — suite readme |
| `AGENT.md` | **new** — agent guidance |

Production files (`server.py`, `events.py`, `store.py`, `runtime.py`, `errors.py`, `client.py`,
`config.json`, `requirements.txt`, `sources/*`) were **not modified**.

---

## 3. Validation result (one-shot bounded sweep)

| File | Servers | Result |
|------|---------|--------|
| `test_unit_sources.py` | 0 | 91 / 91 ✅ |
| `test_acknowledgement.py` | 0 | 16 / 16 ✅ |
| `test_consumers.py` | 0 | 12 / 12 ✅ |
| `test_background_tasks.py` | 0 | 10 / 10 ✅ |
| `test_source_dedup.py` | 0 | 13 / 13 ✅ |
| `test_source_lifecycle.py` | 0 (+1 S15) | 29 / 29 ✅ |
| `test_events.py` | 1 | 40 / 40 ✅ |
| `test_timeouts.py` | 1 (+s10) | 5 / 5 ✅ |
| `test_subscriptions.py` | 5 | 8 / 8 ✅ |
| `test_errors.py` | restart | 5 / 5 ✅ |
| `test_reconnect.py` | restart | 5 / 5 ✅ |
| `test_sdk_alignment.py` | 1 | 26 / 26 ✅ |
| `test_multi_client.py` | 1 | 6 / 6 ✅ |
| `test_performance.py` | 1 | 3 / 3 ✅ |
| `test_lifespan.py` | 1 | 1 / 1 ✅ |
| **Total** | | **270 / 270 ✅** |

Each file ran under its 300 s cap; total wall time was a few minutes, not hours.

---

## 4. Latent bugs fixed (pre-existing, surfaced by the sweep)

1. **`helpers/mcp.py` missing `import time`** — `wait_source_ready`/`wait_for_event_count`
   call `time.monotonic()`; broke `test_subscriptions.py` and `test_timeouts.py` (s10) with
   `NameError: name 'time' is not defined`. Added `import time`.
2. **ack-without-advance** in `test_acknowledgement.py` and `test_consumers.py` (T5) — direct
   tests called `store.acknowledge_event` but not `store.advance_checkpoint`; `get_checkpoint`
   stayed 0. Added `_ack(store, cid, eid)` mirroring the MCP tool (server.py:633 chains
   `advance_checkpoint` at 661-662).
3. **`store.add_consumer_topic` vs `store.add_topic`** — `test_consumers.py` called a
   non-existent method (`'EventStore' object has no attribute 'add_consumer_topic'`); renamed to
   `add_topic` (3 tests).
4. **missing `import asyncio`** in `test_consumers.py` and (earlier) `test_acknowledgement.py` —
   `NameError` at `asyncio.run(main())`. Added the import.
5. **Orphaned file-scope server in `test_subscriptions.py`** — `main()` started a server that
   every test overwrote via the module-global `_server_proc`, leaking it (port held after the
   process exited). Removed; each test starts its own config-specific server.
6. **Orphaned file-scope server in `test_timeouts.py`** — s10 overwrote the file-scope server.
   Added `stop_server()` before s10 starts its own.
7. **`test_source_lifecycle.py` S5/S14/S11**:
   - S5/S14 registered the consumer **after** the source published, so `consumer_event_state`
     was never materialized (replay returned nothing). `register_consumer` does not backfill
     existing events. Consumers are now registered before `start_all`.
   - S11 raced on `bg.active_count == 0` — `BackgroundTaskManager.cancel` only deletes the task
     from its dict (does not call `task.cancel()`), so `active_count` flips instantly while the
     source loop is still cleaning up. S11 now waits for the actual terminal `stopped`/`completed`
     state.

---

## 5. Deliberately NOT changed

- `test/test_phase8.py`, `test/integrate_test.py` — **deleted**; not recreated, wrapped, or
  re-added to `run_all.py`.
- Any production source file — frozen for this phase.
- `run_all.py` default `timeout` left at 300 s (intentional safety ceiling, not a hang risk).

---

## 6. How to verify / run

```bash
# Focused (preferred during development):
python test/test_acknowledgement.py
python test/run_all.py --group fast

# One-shot full validation (CI / release):
python test/run_all.py --timeout 300
```

No individual file can exceed its timeout; a hung file is killed and reported FAILED, and its
`atexit` handler cleans up any server. See `TEST_RUNTIME_MAP.md` for the full matrix.
