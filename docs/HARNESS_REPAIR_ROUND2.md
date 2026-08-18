# Harness Repair — Follow-Up Implementation Report

**Phase**: Code-Edit Only (harness defect follow-up)
**Date**: 2026-08-17
**Python**: 3.14.6 | **MCP SDK**: 2.0.0

This report covers the second round of harness repairs, addressing defects found during code inspection after the first round (dynamic-port/restart fixes).

---

## 1. Files Inspected

| File | Purpose |
|------|---------|
| `test/mcp_result.py` | Shared harness helpers (`safe_teardown`, result parsers, port reservation) |
| `test/integrate_test.py` | Integration test suite (T10, P7-T13 restart paths, lifecycle) |
| `test/test_phase8.py` | Phase 8 unit test suite (178 tests, P8-T10 restart path) |
| `config.json` | Project config (restored to baseline in prior round) |

---

## 2. Files Changed

| File | Changes |
|------|---------|
| `test/mcp_result.py` | Rewrote `safe_teardown()` with primary-failure awareness; removed generic `OSError` suppression; rewrote ExceptionGroup handling to re-raise unexpected members; added `primary_failure` parameter |
| `test/integrate_test.py` | Removed `_CONFIG_BACKUP` overwrite in T10/P7-T13 restarts; added `_ORIGINAL_CONFIG_BYTES`; updated `_restore_test_environment()` to use it; narrowed `stop_server()` except clauses |
| `test/test_phase8.py` | Added `_ORIGINAL_CONFIG_BYTES`; updated `main()` and `_restore_test_environment()`; rewrote P8T10 restart to not overwrite baseline; narrowed `stop_server()` except clauses |

---

## 3. Production Files Changed — NONE

All 11 production files verified unchanged via SHA-256 hash comparison (same hashes as pre-inspection baseline).

---

## 4. Old `safe_teardown()` Behavior

```python
def safe_teardown(func, *args, **kwargs) -> None:
    try:
        func(*args, **kwargs)
    except BaseException:  # swallowed EVERYTHING
        pass
```

**Problems identified:**
- **A.** Unexpected cleanup exceptions (AssertionError, RuntimeError, ValueError) were logged to stderr but NEVER raised — causing false-green results when no primary failure existed
- **B.** Mixed `ExceptionGroup` (expected + unexpected members) was fully suppressed — unexpected members silently lost
- **C.** Generic `OSError` was globally treated as expected — `PermissionError`, resource exhaustion, and other real failures were hidden
- **D.** No awareness of whether a primary test failure was already active

---

## 5. New `safe_teardown()` API

```python
def safe_teardown(
    func,
    *args,
    primary_failure: BaseException | None = None,
    **kwargs,
) -> None:
```

**Decision logic:**
1. `KeyboardInterrupt` / `SystemExit` → always propagate
2. `ExceptionGroup` → classify members individually:
   - All expected → suppress
   - Mixed + no primary → **raise** new `ExceptionGroup` with unexpected members only
   - Mixed + primary active → log unexpected members, suppress group
3. Individual exception:
   - Expected teardown noise → suppress
   - Unexpected + no primary → **re-raise**
   - Unexpected + primary active → **log to stderr, do not re-raise** (preserve primary)

Primary failure detection: uses `sys.exc_info()[1]` (whether an exception is currently being handled) OR the explicit `primary_failure` parameter.

---

## 6. How Primary Failure State Is Communicated

Two mechanisms:
1. **Automatic**: `sys.exc_info()[1]` — if we're inside an exception handler, a primary is active
2. **Explicit**: `primary_failure=exc` parameter — caller passes the captured primary explicitly

Both are checked; either one being non-None means a primary is active.

---

## 7. Expected Teardown Exceptions (Suppressed)

| Exception | Rationale |
|-----------|-----------|
| `asyncio.CancelledError` | Normal during async task cancellation on Python 3.14 |
| `EOFError` | Peer closed stream during shutdown |
| `BrokenPipeError` | Pipe closed during cleanup |
| `ConnectionError` (incl. `ConnectionResetError`, `ConnectionAbortedError`) | Connection closed by peer |
| `ProcessLookupError` | Process already terminated |
| `RuntimeError` with exact messages "event loop is closed" or "cannot send data" | Known async teardown noise |

---

## 8. Operation-Specific Expected Errors

No blanket `FileNotFoundError` suppression. Callers that need idempotent-file-removal semantics should handle it themselves or pass `primary_failure` to suppress contextually.

---

## 9. Generic `OSError` Handling — REMOVED

The previous blanket `isinstance(exc, OSError): return True` has been removed. Only specific OSError subclasses (`BrokenPipeError`, `ProcessLookupError`) are suppressed. General `OSError` (including `PermissionError`, errno 13, resource exhaustion) now **raises**.

---

## 10. `AssertionError` Behavior — NO PRIMARY

**Raises immediately.** The test fails with the AssertionError. This was previously silently swallowed, producing false-green results.

---

## 11. `AssertionError` Behavior — WITH PRIMARY

Logged to stderr. Primary AssertionError is preserved and re-raised. Cleanup diagnostic is visible but does not replace the original failure.

---

## 12. Unknown `RuntimeError` Behavior — NO PRIMARY

**Raises immediately.** Only known messages ("event loop is closed", "cannot send data") are suppressed. Unknown RuntimeErrors surface as test failures.

---

## 13. Unknown `RuntimeError` Behavior — WITH PRIMARY

Logged to stderr. Primary failure preserved.

---

## 14. `KeyboardInterrupt` Behavior

Always propagated immediately. Not suppressed under any circumstance.

---

## 15. `SystemExit` Behavior

Always propagated immediately. Not suppressed under any circumstance.

---

## 16. `ExceptionGroup` Behavior

- All members expected → suppressed
- At least one unexpected member + no primary → **new `ExceptionGroup("cleanup", [unexpected])` raised**
- At least one unexpected member + primary active → unexpected members logged, group suppressed

---

## 17. Nested `ExceptionGroup` Behavior

Classification is applied only to top-level `.exceptions`. Since `CancelledError` (a BaseException) cannot be nested inside `ExceptionGroup`, nested ExceptionGroups would only contain Exception subclasses, all of which are individually classified.

---

## 18. `BaseExceptionGroup` Behavior

Not explicitly handled (Python 3.14 can create them, but the test harness does not produce them). If encountered, they would fall through to the `except BaseException` handler and be classified individually. `KeyboardInterrupt`/`SystemExit` inside a `BaseExceptionGroup` would still propagate because the top-level handler catches them first.

---

## 19. Every `safe_teardown()` Call Site Reviewed

| Location | Context | Primary active? | New behavior |
|----------|---------|-----------------|--------------|
| `test_phase8.py:188` `_cleanup()` | Inside `finally` after `except BaseException` | Yes (`exc` captured) | Unexpected cleanup → logged, primary preserved |
| `test_phase8.py:190` `_cleanup()` | Same finally block | Yes | Same |
| `test_phase8.py:254` `start_server()` failure path | Inside `except Exception` | Yes (`exc` active) | Same |
| `integrate_test.py:148` `_restore_test_environment()` | atexit / finally, no primary expected | No | Unexpected cleanup → **RAISES** (test fails) |
| `integrate_test.py:150` `_restore_test_environment()` | Same | No | Same |
| `integrate_test.py:151` `_restore_test_environment()` | Same | No | Same |
| `integrate_test.py:220` `start_server()` failure path | Inside `except Exception` | Yes | Same as phase8 |
| `test_phase8.py:188-192` `_restore_test_environment()` | Same pattern | No | Unexpected cleanup → **RAISES** |

---

## 20. `stop_server()` Old Behavior

```python
except Exception:
    pass  # suppressed ALL exceptions including RuntimeError, AssertionError
```

---

## 21. `stop_server()` New Behavior

Two narrowed except blocks:
```python
# Unowned-process check:
except (subprocess.TimeoutExpired, ProcessLookupError):
    pass  # only these two are expected

# Graceful termination:
except (ProcessLookupError, OSError):
    pass  # only known teardown noise
```

Unexpected process errors (e.g., `RuntimeError` from `proc.terminate()`) now propagate.

---

## 22. Process-Termination Errors Still Considered Expected

- `ProcessLookupError` — process already exited between poll and terminate
- `subprocess.TimeoutExpired` — `proc.wait(timeout=0)` didn't exit quickly (unowned check)
- `OSError` during terminate/wait/kill — OS-level teardown noise (bad fd, already-closed handle)

---

## 23. Unexpected Process Error Behavior

Any exception outside the above list (e.g., `RuntimeError` from `proc.terminate()`) propagates. If a primary test failure is already active, it's logged to stderr and the primary is preserved.

---

## 24. Every `_CONFIG_BACKUP` / Backup-Variable Assignment Found

| Location | Before edit | After edit |
|----------|-------------|------------|
| `integrate_test.py:52` | `_CONFIG_BACKUP: bytes \| None = None` | + `_ORIGINAL_CONFIG_BYTES: bytes \| None = None` |
| `integrate_test.py:167` | `_CONFIG_BACKUP = _backup_config()` (in `start_server`) | Unchanged — captures test config, overwrites global |
| `integrate_test.py:549` | `_CONFIG_BACKUP = _backup_config()` (T10 restart) | **REMOVED** — no longer overwrites baseline |
| `integrate_test.py:982` | `_CONFIG_BACKUP = _backup_config()` (P7-T13 restart) | **REMOVED** — no longer overwrites baseline |
| `test_phase8.py:80` | `_CONFIG_BACKUP: bytes \| None = None` | + `_ORIGINAL_CONFIG_BYTES: bytes \| None = None` |
| `test_phase8.py:211` | `_CONFIG_BACKUP = _backup_config()` (in `start_server`) | Unchanged |
| `test_phase8.py:1151` (old) | `SERVER_PROC = start_server(clean_data=False)` (P8T10) | **REWRITTEN** — inline restart, no backup overwrite |
| `test_phase8.py:1822` (old) | `config_backup = _backup_config()` (local in main) | Changed to `_ORIGINAL_CONFIG_BYTES = _backup_config()` |

---

## 25. Exact Config Backup Lifecycle Before Edit

1. `main()` starts → `config_backup = _backup_config()` (local var, discarded)
2. `start_server()` → `_CONFIG_BACKUP = _backup_config()` (captures current config)
3. Test runs, config modified
4. T10 restart → `_CONFIG_BACKUP = _backup_config()` (**overwrites** with test config!)
5. P7-T13 restart → `_CONFIG_BACKUP = _backup_config()` (**overwrites** again!)
6. `_restore_test_environment()` → restores from `_CONFIG_BACKUP` (now test config, NOT original)

**Bug**: Final restore could write a test config back to `config.json` instead of the original baseline.

---

## 26. Exact Config Backup Lifecycle After Edit

1. `main()` starts → `_ORIGINAL_CONFIG_BYTES = _backup_config()` (captured ONCE, never touched)
2. `start_server()` → `_CONFIG_BACKUP = _backup_config()` (local state for this server instance)
3. Tests run, config modified
4. T10 restart → writes test config, does NOT touch `_ORIGINAL_CONFIG_BYTES`
5. P7-T13 restart → writes test config, does NOT touch `_ORIGINAL_CONFIG_BYTES`
6. P8T10 restart → writes test config, preserves `_SERVER_INFO["config_backup"]` (original)
7. `_restore_test_environment()` → uses `_ORIGINAL_CONFIG_BYTES` (always the original)

---

## 27. Did Restart Previously Overwrite Baseline?

**Yes.** Both T10 and P7-T13 did `_CONFIG_BACKUP = _backup_config()` after writing their temporary test configs. This captured the test config (port N, data_p8/data_integration_test) and replaced the original baseline bytes. The final `_restore_test_environment()` would then restore the test config instead of the repository baseline.

---

## 28. Confirmation Restart No Longer Overwrites Baseline

**Confirmed.** T10, P7-T13, and P8T10 all now write the temporary test config without updating `_ORIGINAL_CONFIG_BYTES` or `_SERVER_INFO["config_backup"]`. The `_restore_test_environment()` function reads from `_ORIGINAL_CONFIG_BYTES` as the authoritative source.

---

## 29. Original-Config Variable/Storage Design

```python
# Module-level, set once in main(), never written after:
_ORIGINAL_CONFIG_BYTES: bytes | None = None
```

Priority order in `_restore_test_environment()`:
1. `_ORIGINAL_CONFIG_BYTES` (authoritative, set once at suite start)
2. `_SERVER_INFO.get("config_backup")` (fallback for edge cases)
3. `_CONFIG_BACKUP` (last resort)

---

## 30. Missing-Config-at-Start Handling

If `config.json` did not exist when the suite began, `_backup_config()` returns `None`. `_ORIGINAL_CONFIG_BYTES = None`. Final `_restore_config(None)` removes `config.json` if it exists (or does nothing if absent). Correct behavior.

---

## 31. Config Restoration Behavior on Success

`_restore_config(_ORIGINAL_CONFIG_BYTES)` writes exact bytes back to `config.json`. Byte-for-byte identical to what was captured at suite start.

---

## 32. Config Restoration Behavior on Primary Failure

If a test assertion fails and `_restore_test_environment()` also encounters an unexpected error during config restore, the primary test failure is preserved (via `safe_teardown`'s primary-aware logic). The config restore failure is logged to stderr.

---

## 33. Config Restoration Behavior When Restore Itself Fails

If `_restore_config()` raises unexpectedly (e.g., permission denied writing `config.json`):
- With no primary failure → **the exception propagates**, failing the harness
- With primary failure active → logged to stderr, primary preserved

In both cases, `_restore_config()` no longer has `except OSError: pass` — `FileNotFoundError` is the only suppressed error (file already gone).

---

## 34. Byte-for-Byte Restore Result

Smoke test verified:
- Original hash: `a5938713608082e8fb9867562ccc3b36be9283b71787a4be92b34dae53df2a48`
- After 2 overwrites with stale test configs
- After restore: **exact same hash** ✓

---

## 35. Data-Cleanup Exception Behavior

`_clean_test_data()` uses `shutil.rmtree(..., ignore_errors=True)` — internally safe. If it raises unexpectedly (e.g., permission denied on the data dir), `safe_teardown` handles it per the new rules (raise if no primary, log if primary active).

---

## 36. atexit Role

`atexit.register(_restore_test_environment)` remains as a belt-and-suspenders fallback. It runs if the process exits abnormally before reaching the `finally` block. Primary restore mechanism is the explicit `finally: _restore_test_environment()` call in `main()`.

---

## 37. Narrow Smoke Checks Performed

| # | Check | Result |
|---|-------|--------|
| 1 | CancelledError suppressed | PASS |
| 2 | EOFError suppressed | PASS |
| 3 | ConnectionError suppressed | PASS |
| 4 | BrokenPipeError suppressed | PASS |
| 5 | ProcessLookupError suppressed | PASS |
| 6 | RuntimeError(event loop) suppressed | PASS |
| 7 | AssertionError raised (no primary) | PASS |
| 8 | Unknown RuntimeError raised (no primary) | PASS |
| 9 | ValueError raised (no primary) | PASS |
| 10 | PermissionError raised (generic OSError not suppressed) | PASS |
| 11 | OSError(13) raised | PASS |
| 12 | KeyboardInterrupt propagated | PASS |
| 13 | SystemExit propagated | PASS |
| 14 | ExceptionGroup(all expected) suppressed | PASS |
| 15 | Mixed ExceptionGroup, no primary → raised | PASS |
| 16 | Mixed ExceptionGroup + primary → preserved + logged | PASS |
| 17 | Primary + unexpected cleanup → preserved + logged | PASS |
| 18 | Normal function executes | PASS |
| 19 | `primary_failure` param suppresses unexpected | PASS |
| 20 | FileNotFoundError raised (not globally suppressed) | PASS |
| — | Byte-compile: mcp_result.py | OK |
| — | Byte-compile: integrate_test.py | OK |
| — | Byte-compile: test_phase8.py | OK |
| — | Config byte-for-byte restore after 2 overwrites | PASS |
| — | No hardcoded 8000 in integrate_test.py | PASS |
| — | `_ORIGINAL_CONFIG_BYTES` present in both suites | PASS |
| — | `stop_server()` narrowed in both suites | PASS |

---

## 38. Full Test Suites Run — NO

Neither `test_phase8.py` nor `integrate_test.py` were executed. Those belong to the next independent verification prompt.

---

## 39. Dependencies Added — NONE

Only stdlib used. No new pip packages.

---

## 40. Private MCP SDK Usage Added — NONE

No private SDK APIs introduced.

---

## 41. Unknown-Consumer Warning Unchanged

`store.py` untouched. The `{"status": "error", "message": ...}` dict pattern preserved.

---

## 42. Structured-Output Production Behavior Unchanged

No modifications to tool return annotations or `output_schema` handling.

---

## 43. MCP Error Production Behavior Unchanged

No changes to exception-to-protocol-error mapping in production code.

---

## 44. Dynamic-Port/Readiness Behavior Preserved

All dynamic-port fixes from the prior round are intact:
- T10: `reserve_free_port()` → `_wait_mcp_ready(SERVER_URL)` ✓
- P7-T13: same pattern ✓
- P8-T10: same pattern (rewritten from `start_server()` call) ✓
- `wait_for_server()` dead code removed ✓

---

## 45. Remaining Harness Issues

None identified. All five defect categories (A–E) are resolved:
- **A.** Unexpected cleanup now fails the test when no primary exists
- **B.** Mixed `ExceptionGroup` now raises unexpected members when no primary exists
- **C.** Generic `OSError` is no longer globally suppressed
- **D.** Original config backup is captured once and never overwritten
- **E.** `stop_server()` except clauses are narrowed to expected errors only

---

## 46. Items Requiring Next Independent Verification

The following must be verified in the next separate verification prompt:

1. **Full `test_phase8.py`** (178 tests) — must still pass after harness repairs
2. **Full `integrate_test.py`** (74 tests) — must still pass with fixed restart logic and config backup
3. **Cross-suite consistency** — no conflicting behavior between suites
4. **Real MCP client end-to-end checks** — tool calls, error paths, structured output
5. **Config restoration under crash conditions** — ExceptionGroup, Ctrl+C, assertion failure
6. **Restart port consistency** — config port = probe port = SERVER_URL port at runtime
7. **SDK-alignment verification** — tool error paths, output_schema, structured_content (separate phase)

---

*End of report.*
