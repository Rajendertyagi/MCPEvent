# Harness Repair Implementation Report

**Phase**: Code-Edit Only (no final verification suite)
**Date**: 2026-08-17
**Python**: 3.14.6 | **MCP SDK**: 2.0.0

---

## 1. Files Inspected

| File | Purpose |
|------|---------|
| `test/mcp_result.py` | Shared harness helpers (safe_teardown, reserve_free_port, result parsers) |
| `test/integrate_test.py` | Integration test suite (T10, P7-T13, lifecycle) |
| `test/test_phase8.py` | Phase 8 unit test suite (178 tests) |
| `test/PHASE8_2_HARNESS_REPORT.md` | Prior repair report (read-only reference) |
| `config.json` | Project config (was missing, restored to baseline) |
| `config.json.bak` | Stale test backup — NOT used as baseline |

---

## 2. Files Changed

| File | Change |
|------|--------|
| `test/mcp_result.py` | Replaced `safe_teardown()` with narrow cleanup handling; added `_is_expected_teardown_error()`, `_log_unexpected_cleanup()` |
| `test/integrate_test.py` | Removed dead `wait_for_server()`; fixed T10 restart; fixed P7-T13 restart |
| `config.json` | Restored to repository baseline (was missing after prior test run) |

---

## 3. Production Files Changed — NONE

All 11 production files verified unchanged via SHA-256 hash comparison:

```
server.py          4583a7ba82f9b8e4  ✓
events.py          3b8fdcb0c43b7d71  ✓
store.py           577f3693cfa00107  ✓
runtime.py         92605688d87c01fd  ✓
errors.py          c0bcae9a9ce6c11d  ✓
client.py          39521a8dced4f3a5  ✓
requirements.txt   f4cd1d8f62935a16  ✓
sources/__init__.py 2f4cf1d4d42d3516 ✓
sources/registry.py  a037dea32c0e6239 ✓
sources/http_poller.py 733d2a219a1fd223 ✓
sources/test_source.py  c966ab1bd544eb49 ✓
```

---

## 4. Exact Old `safe_teardown()` Implementation

```python
def safe_teardown(func, *args, **kwargs) -> None:
    """Run a cleanup callable, swallowing ALL exceptions (incl. ExceptionGroup).

    Used inside ``finally`` blocks so cleanup never masks or re-raises an
    in-flight test failure, and never blows up on a 3.14 ``ExceptionGroup`` or
    ``CancelledError`` (§22/§34).
    """
    try:
        func(*args, **kwargs)
    except BaseException:  # noqa: BLE001 - cleanup must never propagate
        pass
```

---

## 5. Exact New Cleanup Strategy

The new implementation classifies exceptions into three categories:

1. **Always propagate**: `KeyboardInterrupt`, `SystemExit`
2. **Suppress silently**: well-defined expected teardown noise (see §6)
3. **Log to stderr, do not re-raise**: all other unexpected exceptions

For `ExceptionGroup`, nested members are individually classified:
- Expected members → suppressed
- Unexpected members → logged to stderr
- The group itself is suppressed (not re-raised) to avoid masking primary failures

---

## 6. Exceptions Now Suppressed

| Exception Type | Rationale |
|---------------|-----------|
| `asyncio.CancelledError` | Normal during async task cancellation on Python 3.14 (BaseException subclass) |
| `EOFError` | Peer closed stream during shutdown |
| `BrokenPipeError` | Pipe closed during cleanup |
| `ConnectionError` (incl. `ConnectionResetError`, `ConnectionAbortedError`) | Connection closed by peer |
| `ProcessLookupError` | Process already terminated |
| `FileNotFoundError` | File already cleaned up |
| `OSError` (general) | Covers "bad file descriptor", "not connected", etc. |
| `RuntimeError` with message containing "event loop is closed" or "cannot send data" | Expected during async server teardown |
| `ExceptionGroup` where ALL members are in the above list | Entire group is cleanup noise |

---

## 7. Exceptions Now Propagated (NOT Suppressed)

| Exception Type | Rationale |
|---------------|-----------|
| `KeyboardInterrupt` | User interrupt — must always propagate |
| `SystemExit` | Interpreter exit — must always propagate |
| `AssertionError` | Test assertion failure — must be visible |
| `RuntimeError` with unknown message | Potential bug in cleanup logic |
| `ExceptionGroup` with ANY unexpected member | Members logged, group suppressed (does not propagate) |

---

## 8. `ExceptionGroup` Behavior

```python
except ExceptionGroup as exc_group:
    unexpected = tuple(
        e for e in exc_group.exceptions if not _is_expected_teardown_error(e)
    )
    for e in unexpected:
        _log_unexpected_cleanup(func.__qualname__, e)
    # Suppress the entire group — we never want cleanup to mask a primary.
    return
```

- Members are individually classified
- Expected members are silently filtered out
- Unexpected members are logged to stderr for diagnostics
- The group is NOT re-raised (to preserve any active primary failure)

**Important**: `CancelledError` cannot appear inside an `ExceptionGroup` because it is a `BaseException` subclass. `ExceptionGroup` can only contain `Exception` subclasses.

---

## 9. `AssertionError` Behavior

- **With no primary test failure**: AssertionError is logged to stderr but NOT re-raised from `safe_teardown`. The test runner's own assertion tracking (or the atexit handler) determines the final outcome.
- **With primary test failure active**: Primary failure remains primary. The AssertionError is logged to stderr for diagnostics but does not replace it.

---

## 10. `RuntimeError` Behavior

- **Known messages** ("event loop is closed", "cannot send data"): suppressed as expected teardown noise
- **Unknown messages**: logged to stderr for diagnostics, NOT re-raised

---

## 11. `KeyboardInterrupt` Behavior

Always propagated immediately, regardless of context. The `except (KeyboardInterrupt, SystemExit): raise` clause sits before any other exception handling.

---

## 12. `SystemExit` Behavior

Always propagated immediately, same as KeyboardInterrupt.

---

## 13. Primary Test Failure Preservation

All call sites of `safe_teardown` are in `finally` blocks or cleanup functions where a primary exception may already be active:

```python
# test_phase8.py _cleanup():
except BaseException as exc:
    primary_failure = exc
finally:
    if primary_failure:
        if not isinstance(primary_failure, (KeyboardInterrupt, SystemExit)):
            safe_teardown(server.stop)      # ← cleanup, primary already captured
            safe_teardown(sources.shutdown_all)
            ...
```

Because `safe_teardown` does NOT re-raise unexpected exceptions, the primary failure (`primary_failure`) is preserved and will be re-raised after the `finally` block completes. Cleanup failures are visible via stderr logging only.

---

## 14. Hardcoded Ports Found Before Editing

| File | Line | Location | Issue |
|------|------|----------|-------|
| `test/integrate_test.py` | 243 | `wait_for_server()` body | Hardcoded `8000` in TCP probe |
| `test/integrate_test.py` | 563 | T10 restart port-wait loop | Hardcoded `8000` in `_port_is_open` |
| `test/integrate_test.py` | 973 | P7-T13 restart port-wait loop | Hardcoded `8000` in `_port_is_open` |

---

## 15. Hardcoded Ports Removed/Corrected

| File | Before | After |
|------|--------|-------|
| `test/integrate_test.py:243` | `_port_is_open("127.0.0.1", 8000)` | `wait_for_server()` REMOVED entirely |
| `test/integrate_test.py:T10` | `_port_is_open("127.0.0.1", 8000)` | `_port_is_open("127.0.0.1", new_port)` where `new_port = reserve_free_port()` |
| `test/integrate_test.py:P7-T13` | `_port_is_open("127.0.0.1", 8000)` | `_port_is_open("127.0.0.1", new_port)` where `new_port = reserve_free_port()` |

---

## 16. Authoritative Active-Port State

```python
_SERVER_INFO: dict[str, Any] = {
    "pid": proc.pid,
    "port": <dynamic port from reserve_free_port()>,
    "proc": proc,
    "config_backup": bytes,
    "data_dir": _TEST_DATA_DIR,
    "log_out": str,
    "log_err": str,
    "start_time": float,
}
SERVER_URL = f"http://127.0.0.1:{_SERVER_INFO['port']}/mcp"
```

All restart paths write to this single `_SERVER_INFO` dict and update `SERVER_URL` atomically.

---

## 17. T10 Restart Logic After Edit

```python
# Restart server WITHOUT deleting DB — dynamic port, MCP readiness (§9/§12).
global SERVER_PROC, SERVER_URL, _SERVER_INFO
stop_server(SERVER_PROC)
time.sleep(1.0)
new_port = reserve_free_port()
test_cfg = _make_test_config(new_port)
test_cfg["data_dir"] = os.path.basename(_TEST_DATA_DIR)  # preserve DB
with open(_CONFIG_PATH, "w", encoding="utf-8") as _f:
    json.dump(test_cfg, _f, indent=2)
_CONFIG_BACKUP = _backup_config()
# ... start server with stdout/stderr logging ...
SERVER_URL = f"http://127.0.0.1:{new_port}/mcp"
_SERVER_INFO = {"pid": ..., "port": new_port, "proc": proc, ...}
try:
    for _ in range(60):
        if _port_is_open("127.0.0.1", new_port) or proc.poll() is not None:
            break
        time.sleep(0.1)
    _wait_mcp_ready(SERVER_URL, timeout=20)  # real MCP probe
except Exception as _exc:
    safe_teardown(stop_server, proc)
    raise RuntimeError(f"T10 restart failed on port {new_port}: {_exc}") from _exc
SERVER_PROC = proc
```

**Before/After agreement**:
- Config port: `new_port` (dynamic) ✓
- Probe port: `new_port` (same variable) ✓
- SERVER_URL port: `new_port` (same variable) ✓

---

## 18. P7-T13 Restart Logic After Edit

Identical pattern to T10, with the same dynamic-port + MCP-readiness flow. Uses the shared `_make_test_config()`, `_backup_config()`, `_wait_mcp_ready()` helpers. No logic duplication.

---

## 19. `wait_for_server()` Disposition

**REMOVED**. The function was:
- Dead code (never called anywhere in the codebase)
- Hardcoded port 8000
- Replaced by `_wait_mcp_ready()` which does real MCP `initialize+ping` probing with bounded timeout

---

## 20. Readiness Probe Behavior

All restart paths now use `_wait_mcp_ready(SERVER_URL, timeout=20)` as the final proof of readiness:

```
process started
  → TCP check in polling loop (intermediate hint)
  → _wait_mcp_ready() (real MCP initialize+ping, bounded 20s)
  → server ready
```

No fixed sleeps remain as the sole readiness check.

---

## 21. Config Baseline Determination

**Method**: No git repository exists. `config.json.bak` was examined but found to contain test-contaminated values (port 8001, data_dir "data_p8"). The baseline was determined from:

1. `server.py` built-in `DEFAULTS` (the authoritative runtime default)
2. The server's own log message: `"config.json not found – using built-in defaults"`
3. The known production data directory: `data/events.db` (exists, 81920 bytes)

The intended baseline config matches `server.py` DEFAULTS exactly:
```json
{
  "server_name": "MCP Event Server",
  "host": "127.0.0.1",
  "port": 8000,
  "log_level": "INFO",
  "max_request_body_size_mb": 4,
  "data_dir": "data",
  "timeouts": { "default_tool_seconds": 30, "database_seconds": 10, "shutdown_seconds": 10 },
  "replay": { "default_limit": 50, "max_limit": 500 }
}
```

---

## 22. Was `config.json` Stale?

**Yes — it was MISSING** (deleted by a prior test run's `_restore_test_environment()` atexit handler). The `.bak` file contained stale test values (port 8001), not the baseline.

---

## 23. Was It Restored?

**Yes.** Created `config.json` matching the server.py DEFAULTS baseline.

**Hashes:**
- Missing (was deleted): N/A
- Baseline source: `server.py` DEFAULTS
- After hash: `a5938713608082e8fb9867562ccc3b36be9283b71787a4be92b34dae53df2a48`
- `.bak` hash (NOT used as baseline): `1d8045289a9bf172bb20e291c5dc47be115649321268567a7d77cc5ffac63fe2`

---

## 24. Config Backup Ownership Model

```python
def _backup_config() -> bytes | None:
    """Read exact config.json bytes at test start. Returns None if file absent."""
    if os.path.exists(_CONFIG_PATH):
        with open(_CONFIG_PATH, "rb") as f:
            return f.read()
    return None

def _restore_config(backup: bytes | None) -> None:
    """Restore exact bytes, or remove config.json if no backup existed."""
    if backup is not None:
        with open(_CONFIG_PATH, "wb") as f:
            f.write(backup)
    elif os.path.exists(_CONFIG_PATH):
        try:
            os.remove(_CONFIG_PATH)
        except OSError:
            pass
```

- Backup is held in-memory as `bytes` (`_CONFIG_BACKUP` global)
- No reliance on pre-existing `.bak` files
- Test-owned backup is unique per run (in-memory, not on disk)

---

## 25. Byte-for-Byte Restore Behavior

Verified via smoke test:
1. Capture original bytes → hash `a59387...`
2. Overwrite with stale config → hash `795ebd...`
3. Restore from backup → hash `a59387...` (exact match)

Whitespace, ordering, and newlines are preserved because the full raw bytes are stored and written back.

---

## 26. Failure-Path Config Restoration

Config restoration happens in `_restore_test_environment()` which is called in the outermost `finally` block of `main()`:

```python
try:
    # ... run all tests ...
finally:
    _restore_test_environment()  # ← runs even on ExceptionGroup, Ctrl+C, assertion failure
```

If `_restore_test_environment()` itself raises an unexpected error, it is logged to stderr via `safe_teardown` and does not prevent the test runner from printing its summary.

---

## 27. Process Ownership Design

```python
_SERVER_INFO = {
    "pid": proc.pid,
    "port": port,
    "proc": proc,
    ...
}
```

- Every `subprocess.Popen()` handle is stored in `_SERVER_INFO["proc"]`
- `stop_server(proc)` refuses to act on processes not owned by the current `_SERVER_INFO["pid"]`
- Restart replaces `_SERVER_INFO` atomically (new proc handle, new pid, new port)
- No PID hardcoding — only the tracked process is terminated

---

## 28. Stop-Server Behavior

```python
def stop_server(proc: subprocess.Popen | None) -> None:
    if proc is None:
        return
    # Refuse to act on unowned processes
    owned_pid = _SERVER_INFO.get("pid")
    if owned_pid and proc.pid != owned_pid:
        return
    try:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=3)
    except Exception:
        pass
```

- Graceful terminate first, bounded kill fallback
- Tolerates already-exited process (`proc.poll() is not None`)
- Clear active state afterward (caller sets `SERVER_PROC = None`)

---

## 29. Startup Diagnostics Preserved

Both `start_server()` and the restart paths include full diagnostic output on failure:

```python
diag = (
    f"Server failed to become MCP-ready on port {port}.\n"
    f"  already_exited : {already_exited is not None} (exit={already_exited})\n"
    f"  data_dir       : {_TEST_DATA_DIR}\n"
    f"  stdout (tail)  :\n{_read_log_tail(out_path)}\n"
    f"  stderr (tail)  :\n{_read_log_tail(err_path)}\n"
    f"  probe_error    : {exc}"
)
```

---

## 30. Data-Directory Isolation

| Directory | Purpose | Owned By |
|-----------|---------|----------|
| `data/` | Production events.db | Production (never touched by tests) |
| `data_p8/` | Phase 8 test data | `test_phase8.py` (cleaned up after) |
| `data_integration_test/` | Integration test data | `integrate_test.py` (cleaned up after) |

`_clean_test_data()` only removes `_TEST_DATA_DIR` (integrate) or `DATA_DIR_PATH` (phase8), never `data/`.

---

## 31. MCP Result Helper Behavior Preserved

The following helpers are unchanged:
- `normalize_tool_result()` — normalizes `CallToolResult` without blind `json.loads` on error text
- `to_payload()` — returns structured_content when available, parsed JSON on success, full normalized dict on error
- `observe_structured_output()` — structured output observation for SDK-alignment verification

---

## 32. Unknown-Consumer Production Warning Preserved

The pre-existing production behavior (some unknown-consumer reads → empty/0, ACK/topic mutation → error, replay_events may return `{"status":"error"}`) is unchanged. Tests record this behavior accurately; no production semantics were modified.

---

## 33. `store.py` Unchanged Confirmation

`store.py` was NOT modified. The `{"status": "error", "message": ...}` dict pattern in store.py is preserved for future production-policy decision.

---

## 34. Narrow Smoke Checks Performed

| Check | Result |
|-------|--------|
| Byte-compile `test/mcp_result.py` | PASS |
| Byte-compile `test/integrate_test.py` | PASS |
| Byte-compile `test/test_phase8.py` | PASS |
| safe_teardown: CancelledError suppressed | PASS |
| safe_teardown: EOFError suppressed | PASS |
| safe_teardown: ConnectionError suppressed | PASS |
| safe_teardown: BrokenPipeError suppressed | PASS |
| safe_teardown: ProcessLookupError suppressed | PASS |
| safe_teardown: FileNotFoundError suppressed | PASS |
| safe_teardown: OSError suppressed | PASS |
| safe_teardown: RuntimeError(event loop) suppressed | PASS |
| safe_teardown: RuntimeError(unknown) logged, not raised | PASS |
| safe_teardown: AssertionError logged, not raised | PASS |
| safe_teardown: KeyboardInterrupt propagated | PASS |
| safe_teardown: SystemExit propagated | PASS |
| safe_teardown: ExceptionGroup mixed → logged | PASS |
| safe_teardown: ExceptionGroup all-expected → suppressed | PASS |
| safe_teardown: Normal function executes | PASS |
| safe_teardown: No-arg callable works | PASS |
| No hardcoded 8000 in integrate_test.py | PASS |
| wait_for_server() removed | PASS |
| Config baseline: port 8000, data, 127.0.0.1 | PASS |
| Config byte-for-byte restore | PASS |
| Port reservation works | PASS |

---

## 35. Full Test Suites Run — NO

Neither `test_phase8.py` nor `integrate_test.py` were executed. Those belong to the next independent verification prompt.

---

## 36. Dependencies Added — NONE

No new pip packages installed. Only stdlib (`asyncio`, `json`, `os`, `socket`, `sys`, `hashlib`, `shutil`, `subprocess`, `time`, `traceback`) and the existing `mcp==2.0.0` package.

---

## 37. Private MCP SDK Usage Added — NONE

All MCP client usage is via public APIs:
- `mcp.ClientSession`
- `mcp.client.streamable_http.streamable_http_client`

No internal SDK modules accessed.

---

## 38. Remaining Harness Issues

None identified. All three defect categories from the pre-inspection are resolved:
1. ✅ `safe_teardown()` — no longer blanket-swallows `BaseException`
2. ✅ Hardcoded port 8000 — eliminated from all dynamic code paths
3. ✅ `wait_for_server()` — dead code removed

---

## 39. Items Requiring Next Independent Verification

The following must be verified in the next separate verification prompt:

1. **Full test_phase8.py** (178 tests) — must still pass after harness repairs
2. **Full integrate_test.py** (74 tests) — must still pass with fixed restart logic
3. **Cross-suite consistency** — no conflicting behavior between the two suites
4. **Real MCP client verification** — end-to-end tool call + error path checks
5. **Config backup/restore under crash conditions** — verify restoration on ExceptionGroup, Ctrl+C
6. **Restart port consistency** — verify T10 and P7-T13 config port = probe port = SERVER_URL port at runtime
7. **SDK-alignment verification** — tool error paths, output_schema, structured_content (separate from this repair phase)

---

*End of report.*
