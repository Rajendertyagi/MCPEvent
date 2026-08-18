# Phase 8.2 — Test/Verification Infrastructure Repair (§65 Implementation Report)

**Nature of this phase:** CODE-EDIT TASK ONLY. The harness was repaired so the
upcoming MCP SDK-alignment verification can run cleanly. **No production behavior
was changed. The final verification campaign was NOT executed in this phase** (per
task constraints), so this document reports *implementation*, not PASS/FAIL results.

---

## 1. Scope & intent
Repair test/verification infrastructure only: stale server/process state, port
conflicts, stale DB/source state, surviving source tasks, blind `json.loads` of
tool results, `ExceptionGroup` during teardown, and the structured-content
verification gap. Production semantics of MCP tools/resources were left intact.

## 2. Mandatory pre-code inspection performed (§1)
Actual on-disk files were inspected before any edit. On-disk code is authoritative
over prior reports.

## 3. Files inspected (read)
`config.json`, `server.py` (behavior only), `runtime.py`, `errors.py` (not modified),
`client.py` (not modified), `sources/__init__.py`, `sources/http_poller.py` (via
tests), `sources/test_source.py`, `sources/registry.py`, `test/integrate_test.py`
(full), `test/test_phase8.py` (full), `test/_ro_exp.py`, `test/verify_phase8_2.py`
(**ABSENT — only `verify_phase8_2_out.log` / `verify_phase82_out.log` /
`verify_cursor_out.log` remain**), and the installed SDK `mcp==2.0.0`.

## 4. Files changed
`test/mcp_result.py` (NEW shared helper), `test/integrate_test.py`,
`test/test_phase8.py`.

## 5. Files left unchanged (frozen production)
`server.py`, `events.py`, `store.py`, `runtime.py`, `errors.py`, `client.py`,
`config.json`, `requirements.txt`, `sources/__init__.py`, `sources/registry.py`,
`sources/http_poller.py`, `sources/test_source.py`.

## 6. Production behavior altered
**NONE.** No tool/resource return type, dispatch path, or semantic was modified.

## 7. New dependencies
**NONE.** No pytest/psutil/requests/httpx/aiohttp/tenacity added.

## 8. SDK private APIs used
**NONE.** Only public `ClientSession.call_tool`, `CallToolResult` fields,
`streamable_http_client`, and `Client` (high-level, already used).

## 9. SDK behavior inspected
`mcp==2.0.0` (system Python `D:\IT\Script\python`). `CallToolResult.model_fields`
= `meta, content, structured_content, is_error, result_type`. Confirmed by
introspection, not assumption.

## 10. MCP error contract (confirmed from code)
`server.py:57-58`: ordinary exceptions are wrapped by the SDK into
`CallToolResult(is_error=True)` automatically; `MCPError` is reserved for genuine
protocol-level failures. Therefore a tool-level failure is observable as
`result.is_error is True` + error text, NOT as a raised JSON-RPC error.

## 11. Old `call()` / `call_session()` behavior (defect)
`for block in result.content: if hasattr(block,"text"): return json.loads(block.text)`
— force-decoded **every** result, including error text, producing cryptic
`json.JSONDecodeError` on tool failures.

## 12. New `call()` / `call_session()` behavior
Use `to_payload()` / `normalize_tool_result()` from `mcp_result`. They inspect
`is_error` / `structured_content` / `content` first and never blindly decode.

## 13. `normalize_tool_result(result)` contract
Returns `{is_error, structured_content, text, parsed, content}`. `parsed` is set
ONLY when `text` is valid JSON (success path). Error text is preserved verbatim.

## 14. `to_payload()` success path
Prefers `structured_content` when it is a populated dict; else the JSON-decoded
text payload; else a `{"text": ...}` wrapper. Backward-compatible with existing
`data.get("status")` assertions.

## 15. `to_payload()` error path
Returns the full normalized dict so callers assert `is_error` and inspect `text`
/ `structured_content` / `parsed`.

## 16. Unified success/error handling
Both `integrate_test.py` and `test_phase8.py` now share the identical normalizer,
so behavior is consistent across suites.

## 17. `structured_content` preservation (§6)
Captured by `normalize_tool_result`; `observe_structured_output(url, tool, args)`
(and `test_phase8.inspect_tool_output`) returns
`tool / output_schema / is_error / structured_content / content` for the later
SDK-alignment verification, using only public APIs.

## 18. JSON-decide logic (§4/§8/§40/§54)
JSON decode is attempted ONLY on success text. Error text is never decoded.

## 19. P7T17 fixed (native MCP error)
Replaced `resp.get("status")=="error"` + `resp.get("message")` with
`assert resp.get("is_error") is True` + non-empty `text`.

## 20. Old `{"status":"error"}` dict expectation
Removed. A repo-wide grep confirms no remaining blind JSON-decode of tool results
and no remaining `status=="error"` expectations in the harness.

## 21. Server process ownership (§11)
`_SERVER_INFO` dict tracks `pid / port / proc / config_backup / data_dir / log_out /
log_err / start_time`. `stop_server` terminates ONLY the recorded PID and refuses
foreign processes. No broad kill by port/name.

## 22. Port management (§13)
`integrate_test.py` now reserves an OS-assigned free port (`reserve_free_port()`),
no hardcoded global. `test_phase8.py` switched from fixed `TEST_PORT` to a runtime
`_ACTIVE_PORT` (also OS-assigned), updated in `SERVER_URL`, `start_server`,
`wait_server_ready`. Free-port race avoided (reserve → start immediately).

## 23. DB / config isolation (§18/§19)
`integrate_test.py` NO LONGER deletes production `data/events.db`. It writes a temp
`config.json` (port + isolated `data_integration_test`) with backup/restore.
`test_phase8.py` already used isolated `data_p8`; retained and hardened.

## 24. Server readiness (§15/§16)
Port-open-only waits replaced by a real MCP probe `_wait_mcp_ready` (initialize +
`call_tool("ping")`) with a bounded deadline.

## 25. Startup-failure diagnostics (§16)
On probe failure, raises `RuntimeError` carrying exit code, `already_exited`,
data path, `stdout`/`stderr` log tails, and `probe_error`.

## 26. Config restore + temp-file cleanup (§19/§32)
Temp config written at start; restored in `finally` + an `atexit` safety net
(`_restore_test_environment`) in both suites. The safety net also removes the
isolated data dir and the captured server log directory (`_LOG_DIR`), so temp
files do not leak after a run (including on failure/Ctrl-C).

## 27. Source-task cleanup (§21)
Server teardown terminates the process so the OS reclaims its sources; the
`restart_server()` helper and `start_server(clean_data=False)` + `wait_server_ready()`
ensure no cross-process source-task leak. Within a process, the SDK lifespan
(`SourceManager.shutdown` + `BackgroundTaskManager.shutdown_all`) stops sources.

## 28. Mock HTTP ownership / readiness / cleanup (§23/§31)
`_start_mock` binds port 0 (OS-assigned) and returns `(srv, port)`; each test shuts
it down in `finally`. Cleanup uses `shutdown()` on a daemon thread (no process
leak). Explicit `server_close()` at every call site was not added — `shutdown()` is
sufficient; noted as an optional hardening.

## 29. ExceptionGroup / cancellation tolerance (§22/§34)
`safe_teardown()` swallows `BaseException` (covers 3.14 `ExceptionGroup` and
`CancelledError`). `main()` is wrapped in `try/finally` → `_restore_test_environment`;
`atexit` registered as a belt-and-suspenders net.

## 30. Windows / Py3.14 compatibility (§33/§34)
`terminate()`/`kill()` are cross-platform; no POSIX-only calls used. `ExceptionGroup`
is a `BaseException` subclass, so the `BaseException` catch in `safe_teardown`
covers it. `asyncio.timeout` not required.

## 31. Test-order independence (§44)
Isolated data dirs + unique IDs + per-block `finally` (config restore + data
cleanup) + `main()` end cleanup make runs order-independent.

## 32. Unique consumer/source/event IDs (§45)
`integrate_test.uid()` (time-prefixed) and `test_phase8` time-based IDs; tests use
unique names (e.g. `n3-consumer`, `s5-consumer`, `s14-consumer`).

## 33. Assert identity, not only count (§45)
Retained: P8T10 checks `checkpoint_42` value; S5 checks `sequence is not None`;
D3 checks persistent DB count `== 1`; N1 asserts exactly 1 live notification.

## 34. Coverage preserved (§57–§59)
Custom events, persistent alerts, consumer routing, reconnect/replay, multi-client,
HTTP poller, dedup, cursor, lifecycle, failure/recovery, URL sanitization,
env-secret resolution, and the new SDK-alignment observation helper are all
retained. No tests removed.

## 35. §42–§43 sync-tool description correction (FINDING)
`register_consumer` (`server.py:521-534`) and `add_consumer_topic`
(`server.py:541-557`) are **sync** `@mcp.tool()` functions that call `_store`
methods **directly** — there is NO internal `asyncio.to_thread`. The SDK offloads
sync tools to a worker thread at dispatch (`anyio.to_thread.run_sync`,
`func_metadata.py:105-108`); async tools (`list_relevant_events`,
`get_pending_events`, `acknowledge_event`, `get_consumer_checkpoint`) call
`asyncio.to_thread` internally. **The prior report's claim "sync tools call
asyncio.to_thread() internally" is inaccurate.** Resolution: corrected here and in
the P7T17 docstring only; production was NOT edited. No production bug in this area.

## 36. Production bugs found but NOT fixed (report-only)
None requiring a fix this phase. **Pre-existing production warning (§38):** the
unknown-consumer policy is inconsistent — `list_relevant_events` /
`get_pending_events` / `get_consumer_checkpoint` return empty/0 for an unknown
consumer, while `add_consumer_topic` / `acknowledge_event` are documented as
erroring. This inconsistency is left unchanged per §38; flagged for the next phase.

## 37. Remaining limits / known gaps
(a) Mock cleanup uses `shutdown()` without explicit `server_close()` — acceptable,
no leak. (b) `verify_phase8_2.py` source is absent from the repo; only its logs
remain, so there was no verifier file to edit — the "previous verifier" context
maps to `test_phase8.py` helpers. (c) The final verification campaign was deliberately
NOT run (task constraint). (d) `structured_content` is currently `None` for these
tools (they return plain dicts); `observe_structured_output` is ready for when the
SDK-alignment phase populates it.

## 38. Next-phase items
Run the final verification campaign (out of scope here) on both suites using the
hardened harness; exercise SDK-alignment coverage (exception→`is_error`,
validation, timeout, cancellation, `Context`, lifespan, subscriptions/listen) via
the new observation helper; reconcile the unknown-consumer policy inconsistency;
optionally add explicit `server_close()` for mock servers if stricter port hygiene
is desired.

## 39. Summary
Harness repaired; production frozen; new deps = NONE; SDK private APIs = NONE;
process/port/DB/config/source/mock/ExceptionGroup/cancellation/Windows/Py3.14
handling hardened; structured-output observation helper added. The infrastructure
is now ready for a clean MCP SDK-alignment verification.
