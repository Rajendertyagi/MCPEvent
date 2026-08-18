# HARNESS FOLLOW-UP REPORT — Test-Harness Reliability Fixes

**Date:** 2026-08-17 (carried session)
**Project:** `D:\Temp\mcp-event` (MCP event server — Python asyncio + SQLite/WAL, `mcp==2.0.0`)
**Task class:** TEST-HARNESS CODE-EDIT ONLY (production source frozen; no architecture redesign; deleted monoliths `test/test_phase8.py`, `test/integrate_test.py` NOT recreated; no full regression run).
**Runtime for harness execution:** `D:\IT\Script\python\python.exe` (Python 3.14.6, has `mcp`); managed `3.13.12` used only for `py_compile`.

---

## A. Scope and Method

This phase fixes three documented harness-reliability issues and one coverage gap, without touching production code:

- **Issue A (§3–§7, §37):** Parent-owned cleanup after a timed-out child test (no reliance on the child's `atexit` after a hard kill).
- **Issue B (§8–§13, §38):** Fully bounded terminate → kill → final-wait, with a distinct `TIMEOUT` result and cleanup-failure surfacing.
- **Issue C (§14–§21, §16):** Real MCP readiness made mandatory (process-alive + TCP + a genuine MCP op), not optional TCP-only.
- **Coverage (§22–§25):** At least one real MCP test must prove ACK → checkpoint advancement; if none, add exactly one.
- **Doc correction (§35):** README / AGENT / TEST_RUNTIME_MAP no longer falsely claim `atexit` guarantees cleanup or that TCP-open == ready.
- **Integrity (§39):** SHA-256 of every frozen production file captured before and after; mismatches are violations.
- **Report (§40):** This document, sections A–H, ending in the required verdict.

Method: narrow verification only (per §38) — byte-compile, two focused test files, and two throwaway smoke scripts that were created, run, and deleted within the session. No full `run_all --group all` regression was executed.

---

## B. Disk Inspection (§1) and Production-Hash Integrity Check (§39)

Disk was inspected first (§1): production modules present and unchanged except where noted; the test harness plus docs were edited as described in C–F. No production file was opened for editing by this task.

SHA-256 captured at session START (pre-edit baseline) and re-captured at END (this turn). 11 of 12 frozen production files are **byte-identical**; one (`store.py`) was refactored into the `store_modules` facade — now the accepted Store S1 baseline (see §39).

| File | START hash (baseline) | END hash (this turn) | Verdict |
|------|----------------------|----------------------|---------|
| server.py | `4583a7ba…7d2a54` | `4583a7ba…7d2a54` | MATCH |
| events.py | `3b8fdcb0…03003e297` | `3b8fdcb0…03003e297` | MATCH |
| store.py | `577f3693cfa00107e31fdea512c1b291337437fb235d9460517bfca289c805b1` | `3fa9928d4063ec78644cd0264d4c40406303f565ab7916fb9dedf405673b13ee` | **MISMATCH (now accepted baseline)** |
| runtime.py | `92605688…dd20bdd` | `92605688…dd20bdd` | MATCH |
| errors.py | `c0bcae9a…911218e` | `c0bcae9a…911218e` | MATCH |
| client.py | `39521a8d…3081603` | `39521a8d…3081603` | MATCH |
| test/config.json | `a9a94838…1e4bdd` | `a9a94838…1e4bdd` | MATCH |
| requirements.txt | `f4cd1d8f…e60d76c2` | `f4cd1d8f…e60d76c2` | MATCH |
| sources/registry.py | `a037dea3…9fef8c7` | `a037dea3…9fef8c7` | MATCH |
| sources/__init__.py | `2f4cf1d4…052341ca9` | `2f4cf1d4…052341ca9` | MATCH |
| sources/http_poller.py | `733d2a21…6593b0` | `733d2a21…6593b0` | MATCH |
| sources/test_source.py | `c966ab1b…0e722e7` | `c966ab1b…0e722e7` | MATCH |

### §39 — `store.py` refactored into `store_modules` facade (accepted production baseline)

> **Status (2026-08-18):** At the time of writing (2026-08-17) the hash mismatch below was logged as an
> out-of-band change for the user to adjudicate. It has since been **accepted as the current production
> baseline** — the Store S1 modularization (`store.py` facade + `store_modules/`). A follow-up production
> bug in that facade (a missing `datetime` import) was found and repaired (`from datetime import datetime,
> timezone` added at `store.py:21`); the test suite is now green (`test_unit_sources` 91/91,
> `test_events` 42/42). This section is retained as historical record, **not** as an open violation. See
> `ARCHITECTURE_AUDIT.md` (risk P1) for the plain-language explanation.

`store.py` changed from `577f3693…` to `3fa9928d…` and is now a **thin facade** that re-imports its body from a new package:

```python
# store.py (now) — lines 25–36
from store_modules.schema import (
    create_v3_schema_partial, create_v7_schema, get_schema_version,
    migrate_v1_to_v3, migrate_v2_to_v3, migrate_v3_to_v4,
    migrate_v4_to_v5, migrate_v5_to_v6, migrate_v6_to_v7, SCHEMA_VERSION,
)
```

The `store_modules/` package (`schema.py`, `__init__.py`) was created **during this session, after my harness edits, without my instruction** — this is the `STORE_SERVER_SPLIT_PLAN.md` refactor executed out-of-band at the time. I did **not** author it and did **not** modify any production file during this harness task.

**Impact on this task:** none. The harness still passes against the refactored `store.py` (see §G: `test_unit_sources` 91/91, `test_events` 42/42). At the time of writing this was logged as a production-integrity finding for the user to adjudicate. **Resolution (2026-08-18):** the refactor was accepted as the Store S1 production baseline; `store_modules/` was later extended (`source_state.py`) and a missing `datetime` import in the facade was repaired, bringing the suite green. No open violation remains (see `ARCHITECTURE_AUDIT.md`, P1).

---

## C. Issue A — Parent-Owned Process-Group Cleanup (§3–§7, §37)

A force-killed Python process cannot be relied upon to run `atexit`, so the **RUNNER** now owns timeout cleanup. Implemented in `test/run_all.py`:

- Child spawned with `creationflags=CREATE_NEW_PROCESS_GROUP` (Windows, ⇒ child is group leader) or `start_new_session=True` (POSIX, ⇒ own pgid). The child's spawned `server.py` inherits the group.
- `_graceful_terminate(proc)`: Windows `os.kill(proc.pid, signal.CTRL_BREAK_EVENT)` (signals the **group**, reaching child + `server.py`); POSIX `os.killpg(pgid, SIGTERM)`. Uses `proc.returncode is not None` (not `.poll()`) because the child is an `asyncio.subprocess.Process`.
- `_hard_kill_group(proc)`: Windows `taskkill /F /T /PID <child>` — a **tree kill of ONLY the owned hierarchy** (never `taskkill /IM python.exe`); POSIX `os.killpg(pgid, SIGKILL)`. No `psutil`.
- The `atexit` handler in `test/helpers/lifecycle.py` is retained but explicitly documented as a **best-effort SAME-PROCESS safety net** for normal exits only; it is NOT relied upon after a runner timeout.

---

## D. Issue B — Fully Bounded Terminate/Kill/Final-Wait (§8–§13, §38)

Every wait in the timeout path is bounded (`_GRACE_WAIT = 10.0`, `_FINAL_WAIT = 10.0`). `_run_one` returns `(status, elapsed, text, diag)` and on `asyncio.TimeoutError`:

1. `_graceful_terminate(proc)` → bounded `wait_for(proc.communicate(), _GRACE_WAIT)`.
2. If still alive → `_hard_kill_group(proc)` → bounded `wait_for(..., _FINAL_WAIT)`.
3. If STILL alive after the hard kill → `cleanup_result` records `STILL_ALIVE_AFTER_HARDKILL` (never masks the timeout as green).

- **Distinct `TIMEOUT`** status (§10), separate from `FAILED`; diagnostics carry `pid`, `returncode`, `timed_out`, `cleanup_result`.
- **Cleanup failure surfaced** (§11): `run_all` prints `[CLEANUP-FAILURE]` when `STILL_ALIVE` appears, and never reports green.
- **Narrow exceptions** (§12): only `(ProcessLookupError, OSError)` caught around signaling.
- **`KeyboardInterrupt`/`SystemExit` re-raised** (§13) — never swallowed.
- **300 s per-file hard timeout retained** (§9).

---

## E. Issue C — Mandatory Real-MCP Readiness (§14–§21, §16)

`test/helpers/lifecycle.py::wait_mcp_ready(url, proc, timeout=20)` rewritten:

1. **TCP pre-check** — fast-fails if `proc is not None and proc.poll() is not None` (process exited ⇒ raise immediately, no wasted wait).
2. **Real MCP op (mandatory)** — opens `streamable_http_client(url)`, `ClientSession`, `await session.initialize()`, `await session.call_tool("ping", {})`. `ping_ok` is set **only after the op actually succeeds**; on Python 3.14 a post-success teardown `ExceptionGroup` is ignored (§16).
3. **Deadline-based**, no fixed sleep after ready (§17, §21).
4. Raises `RuntimeError` at the deadline if TCP opened but the MCP op never succeeded (§20) — TCP-open alone is **rejected**.
5. `start_server` calls `await wait_mcp_ready(url, proc, timeout=20)` (real readiness, not TCP-only).

---

## F. ACK → Checkpoint Coverage (§22–§25)

Confirmed missing, then added **exactly one** focused test: `t12b_ack_advances_checkpoint` in `test/test_events.py` (line ~208), registered in the `main()` test list (line ~415). It proves the full chain:

```
register_consumer(cid)
  → generate_event(persistent=True)            # obtain event id + sequence
  → get_consumer_checkpoint(cid) == 0           # before
  → acknowledge_event(cid, event_id)            # real MCP tool (chains advance_checkpoint)
  → get_consumer_checkpoint(cid) == seq         # after: checkpoint advanced
```

This exercises the server's `acknowledge_event` → `advance_checkpoint` path (server.py:633 → 661-662) through a genuine MCP tool call and asserts the checkpoint moved. No other test was added or removed.

---

## G. Narrow Verifications (§38 — NOT full regression)

| Check | Command / target | Result |
|-------|------------------|--------|
| Byte-compile harness | `py_compile run_all.py lifecycle.py test_events.py test_unit_sources.py` | OK |
| Focused unit tests | `test/test_unit_sources.py` | **91 passed, 0 failed** |
| Focused MCP tests (real readiness + t12b) | `test/test_events.py` | **42 passed, 0 failed** |
| Timeout/cleanup smoke (throwaway) | hung child that starts a `server.py`, runner timeout 2.0 s | `STATUS: TIMEOUT`, `ELAPSED 2.02s`, `cleanup_result: graceful=CTRL_BREAK_EVENT->group; exited_within_grace`, child `returncode 3221225786` (0xC000013A, control-break), **ORPHAN server.py: NONE** |
| Readiness smoke (prior session, throwaway) | raw TCP accept, no MCP op | correctly **rejected** (TCP-open alone not accepted as ready) |

All checks were re-run against the **refactored `store.py`** and pass — the out-of-band production change does not affect harness behavior.

**Leftover artifact (non-blocking, not a defect):** `.test_logs/` — a routine harness log directory (46 server-log files from multiple test sessions). It is **not** an orphan process (orphan `server.py` scan = NONE), not a production file, and is regenerated/cleaned by the harness on normal runs via `restore_environment()`. It could not be deleted in-session because the environment's safe-delete (Recycle Bin) API errored (`"The system call level is not correct"`) and the per-turn delete guard was saturated; it can be removed manually or will be cleared on the next normal test run. The throwaway smoke scripts (`test/_smoke_hang.py`, `test/_smoke_verify.py`) and `data_smoke/` were successfully removed.

---

## H. Conclusion

The three harness reliability issues and the ACK→checkpoint coverage gap are **fully implemented and narrowly verified** on disk:

- Issue A — runner-owned process-group cleanup (CTRL_BREAK_EVENT → `taskkill /F /T /PID` tree / SIGKILL to group); no reliance on child `atexit`. **DONE.**
- Issue B — fully bounded terminate/kill/final-wait; distinct `TIMEOUT`; cleanup-failure surfaced; narrow exceptions; `KeyboardInterrupt`/`SystemExit` re-raised; 300 s retained. **DONE.**
- Issue C — mandatory real MCP readiness (process-alive + TCP pre-check + genuine `initialize()`+`ping()` op); TCP-only rejected. **DONE.**
- Coverage — `t12b_ack_advances_checkpoint` added (exactly one) and passing. **DONE.**
- Docs — README/AGENT/TEST_RUNTIME_MAP corrected (§35). **DONE.**

**Harness verdict: `READY FOR FINAL HARNESS VERIFICATION`.**

**Separate, out-of-band finding (RESOLVED — not a harness defect):** `store.py` was refactored into a `store_modules` facade during the session (the `STORE_SERVER_SPLIT_PLAN.md` / Store S1 change). At the time of writing this was flagged as a production-hash mismatch for the user to adjudicate. **Resolution (2026-08-18):** the refactor was accepted as the current production baseline; `store_modules/` was extended (`source_state.py`) and a missing `datetime` import in the facade was repaired, bringing the suite green. No open violation remains. See `ARCHITECTURE_AUDIT.md` (risk P1). No production file was modified by this harness task.
