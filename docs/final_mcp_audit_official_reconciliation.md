# FINAL MCP RECONCILIATION — OFFICIAL SOURCES AUDIT
**Date:** 2026-08-20
**Project:** D:\Temp\mcp-event
**Runtime:** D:\IT\Script\python\python.exe (3.14.6)
**Installed SDK:** mcp==2.0.0, pydantic==2.13.4, starlette==1.3.1
**Mode:** READ-ONLY RECONCILIATION — NO EDITS

---

## OFFICIAL SOURCES ACCESSED

| # | Source | URL | Used For |
|---|--------|-----|----------|
| 1 | Python 3.14 json docs | https://docs.python.org/3.14/library/json.html | `allow_nan` default, NaN/Infinity serialization behavior |
| 2 | Pydantic types docs | https://docs.pydantic.dev/latest/api/types/ | `StrictInt` behavior, `Annotated` support |
| 3 | JSON Schema Core RFC | https://datatracker.ietf.org/doc/html/draft-bhutton-json-schema-00 | Type assertions (integer vs boolean), `minimum` keyword semantics |
| 4 | MCP Spec (main page) | https://modelcontextprotocol.io/specification/2026-07-28 | Spec revision, security principles (SHOULD, not MUST) |
| 5 | MCP Python SDK transport_security.py | https://github.com/modelcontextprotocol/python-sdk/blob/main/src/mcp/server/transport_security.py | TransportSecuritySettings fields, middleware behavior, TODO notes |
| 6 | Python Packaging Guide | https://packaging.python.org/en/latest/guides/writing-pyproject-toml/ | Direct vs transitive dependency guidance |
| 7 | SQLite WAL docs | https://sqlite.org/wal.html | WAL behavior, checkpoint, durability trade-offs |
| 8 | SQLite PRAGMA docs | https://sqlite.org/pragma.html#pragma_synchronous | `synchronous=NORMAL` vs `FULL` semantics |
| 9 | Python 3.14 sqlite3 docs | https://docs.python.org/3.14/library/sqlite3.html | `check_same_thread`, connection-per-operation safety |
| 10 | Python 3.14 asyncio-threading | https://docs.python.org/3.14/library/asyncio-eventloop.html#asyncio-and-free-threaded-python | Event loop thread safety, `asyncio.to_thread` behavior |
| 11 | RFC 3986 (URI) | https://www.rfc-editor.org/rfc/rfc3986.html | URI scheme syntax validation |
| 12 | Installed SDK source | `D:\IT\Script\python\Lib\site-packages\mcp\` | All API usage verification, sync tool execution path |

---

## 17. QUICK CURRENT-CODE SANITY CHECK

| Check | Expected | Actual | Status |
|-------|----------|--------|--------|
| `CONTRACT_VERSION` | `1.2.0` | `1.2.0` | ✅ |
| Tools | 21 | 21 (14 prod + 7 dev) | ✅ |
| Resources | 6 | 6 | ✅ |
| Schema version | 9 | 9 | ✅ |
| `/health` non-MCP | `@mcp.custom_route` | `@mcp.custom_route("/health", methods=["GET"])` | ✅ |
| Replay cursor | `persistent_events.sequence` | Uses `pe.sequence` | ✅ |
| Recent sequence not used as replay cursor | No `recent_sequence` | `recent_sequence` absent from replay.py | ✅ |
| Strict `after_sequence` on both tools | `Annotated[StrictInt, Field(ge=0)] | None` | Present in both `replay.py:27` and `consumers.py:54` | ✅ |
| One canonical `publish_event` | Single function | `events.publish_event()` in events.py | ✅ |
| One `RuntimeMetrics` instance | Single construction | `_metrics = RuntimeMetrics()` at server.py:176 | ✅ |
| `ResourceUpdated` via subscription bus | Bus.publish() | `bus.publish(ResourceUpdated(uri=...))` at events.py:160 | ✅ |
| `consumer_id` is application identity | No MCP/session binding | Stored in SQLite `consumers` table, opaque string | ✅ |
| No private `mcp._` usage | None | Zero matches found | ✅ |

**All 13 sanity checks PASS.**

---

## 18. CORRECTED FINDINGS TABLE

| Finding | Local Code Evidence | Installed SDK Evidence | Official Online Source | Previous Conclusion | **Corrected Conclusion** | Action? |
|---------|--------------------|----------------------|----------------------|--------------------|------------------------|---------|
| **store.py `{"status":"error"}`** | `store.py:359` — `return {"status": "error", "message": str(exc)}` inside `replay_events()` catch block. Called by `server_modules/tools/replay.py:get_pending_events()` which returns result directly to SDK. SDK wraps as `CallToolResult(is_error=False)`. | N/A | N/A | "Minor anti-pattern, classified SHOULD IMPROVE" | **CONFIRMED MUST-FIX** — MCP tool returns a dict that the client interprets as SUCCESS (`is_error=False`) containing an embedded error message. This breaks the contract that `is_error=True` signals failure. The caller gets a "successful" response with `{"status":"error","message":"..."}` instead of a proper MCP error. The broker WILL depend on correct error semantics. | **YES — MUST FIX BEFORE BROKER** |
| **NaN/Infinity JSON** | All 14 `json.dumps()` calls omit `allow_nan`. Project accepts float data from MCP tools (e.g. `event_publish` data param is `dict[str, Any]`). | N/A | Python 3.14 docs: `json.dumps` default `allow_nan=True`. Outputs `NaN`, `Infinity`, `-Infinity` — NOT valid JSON per RFC 7159. | "No issue — floats unlikely to be NaN" | **SHOULD HARDEN** — A malicious or buggy client could send NaN via untyped dict fields. While MCP SDK Pydantic validation rejects non-JSON scalars for typed params, untyped `dict[str, Any]` fields in `event_publish` accept any JSON-deserializable value. NaN is NOT JSON-deserializable via standard parser, but source connectors (http_poller) parse arbitrary external JSON which could contain NaN if the upstream provides it. | **SHOULD FIX** — Add `allow_nan=False` to all `json.dumps` calls |
| **MCP sync threading** | N/A | `func_metadata.py:108`: `await anyio.to_thread.run_sync(functools.partial(fn, **arguments_parsed_dict))` | N/A | **INCORRECT** — Previously stated "sync tools run in event loop thread" | **CORRECTED** — Sync tools run in `anyio` worker thread pool via `to_thread.run_sync`. Each sync tool call runs in a separate OS thread. Since `EventStore._open()` creates a new `sqlite3.Connection` per call (with default `check_same_thread=True`), and each connection is created AND used in the same thread, there is no cross-thread connection sharing. Thread safety is preserved. | None — previous conclusion was wrong, actual behavior is safe |
| **SQLite thread safety** | `store.py:68`: `conn = sqlite3.connect(db_path)` — new connection per method call | `check_same_thread=True` (default) | Python 3.14 sqlite3 docs: "If True (default), ProgrammingError will be raised if the database connection is used by a thread other than the one that created it." | "SAFE — connection-per-operation" | **STILL SAFE — reasoning corrected.** With `anyio.to_thread.run_sync`, each sync tool runs in a worker thread. Each worker thread calls `store.save()` which opens a NEW connection. That connection is used only within that thread's call and closed before return. No connection is ever shared across threads. SAFE. | None — outcome unchanged, reasoning corrected |
| **SQLite NORMAL durability** | `store.py:71`: `PRAGMA synchronous=NORMAL` | N/A | SQLite docs: "With NORMAL, the database filesystem flushes after each batch of no more than 30,000 bytes." "FULL syncs after every write." | "SHOULD BE CONFIGURABLE" | **UNCHANGED** — `NORMAL` provides good performance with acceptable durability for single-process localhost use. Risk is limited to OS crash/power-loss scenarios where up to ~30KB of WAL data could be lost. For broker integration, this may need to be configurable. | **SHOULD FIX** — Make configurable via `config.json` |
| **pydantic dependency** | `tools/replay.py:11`: `from pydantic import Field, StrictInt`; `tools/consumers.py:11`: same | `pydantic>=2.12.0` declared in mcp METADATA | Python packaging: "directly imported third-party packages SHOULD generally be declared directly" | "SHOULD FIX — declare in requirements.txt" | **UNCHANGED** — `pydantic` is directly imported but only transitive via `mcp`. Same for `starlette`. | **SHOULD FIX** — Add to `requirements.txt` |
| **starlette dependency** | `server.py:25-26`: `from starlette.requests import Request`; `from starlette.responses import JSONResponse` | `starlette>=0.48.0` declared in mcp METADATA (Python >=3.14) | Same packaging guidance | "SHOULD FIX" | **UNCHANGED** | **SHOULD FIX** — Add to `requirements.txt` |
| **StrictInt behavior** | `Annotated[StrictInt, Field(ge=0)]` | Pydantic 2.13.4 | Pydantic docs: `StrictInt` is a strict integer type that rejects bool, float, string. Live test confirmed: `True`→REJECTED, `False`→REJECTED, `"1"`→REJECTED, `-1`→REJECTED, `1.5`→REJECTED, `null`→ACCEPTED, `0`→ACCEPTED, `1`→ACCEPTED | D2 fix VALID | **CONFIRMED VALID** — D2 fix is correct per official Pydantic semantics | None |
| **JSON Schema integer** | Tool schema: `anyOf: [{minimum: 0, type: integer}, {type: null}]` | Pydantic generates schema from `StrictInt` | JSON Schema spec: "integer" is a distinct type from "boolean". `type: integer` rejects booleans. `minimum` applies only when instance is a number. | Correct | **CONFIRMED** — Schema correctly rejects booleans | None |
| **Contract 1.2.0 vs 1.2.0-candidate** | `contract.py:21`: `CONTRACT_VERSION = "1.2.0"` | N/A | N/A | "Doc stale" | **WORDING SHOULD CLARIFY** — The code has `CONTRACT_VERSION = "1.2.0"` (no `-candidate` suffix). The doc header says "1.2.0-candidate (metrics + recent + pagination, NOT FROZEN)". These describe TWO DIFFERENT THINGS: the `CONTRACT_VERSION` constant (used programmatically) vs the doc status label. The constant is accurate. The doc's "1.2.0-candidate" label is a STATUS descriptor, not a version number. This is confusing but not technically wrong — just poor wording. | **SHOULD CLARIFY** — Update doc header to match code: "1.2.0 (v1.1.0 alert candidate, v1.2.0 observability candidate)" |
| **DNS false override** | `config.py:142-145`: validates `enable_dns_rebinding_protection` as bool; `server.py:143-147`: passes value to `TransportSecuritySettings` | SDK source: `TransportSecuritySettings(enable_dns_rebinding_protection=False)` → middleware skips all validation | MCP spec security section: "Implementors SHOULD build robust consent and authorization flows... implement appropriate access controls" | "ACCEPTABLE ADMIN OVERRIDE" | **SHOULD WARN** — The MCP spec RECOMMENDS (not MUST) security controls. Allowing `enable_dns_rebinding_protection=false` on `127.0.0.1` is technically permitted by the spec but contradicts the spirit of the recommendation. Should emit a startup warning. | **SHOULD FIX** — Add startup warning log |
| **/health security** | `server.py:272-274`: `@mcp.custom_route("/health", methods=["GET"])` → `JSONResponse({"status": "ok"})` | SDK source (lowlevel/server.py:825-826): custom routes appended AFTER MCP route, outside any transport-security-wrapped scope. SDK docstring: "intended for ... health check endpoints" | SDK source confirms middleware is NOT ASGI middleware — only called inside `/mcp` transport handler | "SAFE" | **UNCHANGED** — `/health` returns minimal data (`{"status":"ok"}`), server is loopback-only (validated by config), and SDK explicitly documents this as intended use for health checks. The bypass is BY DESIGN per SDK. | None |
| **Transport-security test coverage** | `test_sdk_alignment.py` has `test_tool_schemas_are_valid_json_schema` which checks JSON schema of all tools including `after_sequence` | N/A | N/A | "MUST ADD" | **CORRECTED** — Test coverage EXISTS. `test_tool_schemas_are_valid_json_schema` validates all tool schemas (including `after_sequence` strict integer type). `test_timeouts.py` and `test_unit_sources.py` cover security-related behaviors (host validation, URL sanitization). However, no test explicitly sends an HTTP request with invalid Host/Origin to verify 421/403 responses. This is a gap in runtime HTTP security testing. | **SHOULD ADD** — Add explicit HTTP-level security tests |
| **Migration test coverage** | `test_events.py` has `test_p8t3_schema_v9` which tests schema version | N/A | N/A | "SHOULD ADD" | **PARTIALLY COVERED** — Schema version is tested, but no test verifies the full migration chain (v6→v9, v7→v9, v8→v9) in a single initialization. | **SHOULD ADD** — Add migration chain integration test |
| **URI scheme** | 6 URIs: `mcp-event://events/latest`, etc. | N/A | RFC 3986: scheme = ALPHA *( ALPHA / DIGIT / '+' / '-' / '.' ). `mcp-event` matches. Authority empty after `://` is valid. Path segments valid. | "NO ISSUE" | **UNCHANGED** — All URIs are syntactically valid per RFC 3986. Private/unregistered scheme is acceptable for internal/local use. No interoperability issues. | None |

---

## 19. MUST-FIX BEFORE BROKER INTEGRATION

| # | Issue | Evidence | Why It's a Blocker |
|---|-------|----------|-------------------|
| 1 | **`store.py:359` — `{"status": "error"}` anti-pattern in `replay_events()`** | `store.py:356-359`: `except Exception as exc: ... return {"status": "error", "message": str(exc)}`. Called by `server_modules/tools/replay.py:get_pending_events()` which returns the dict directly. SDK wraps as `CallToolResult(is_error=False)`. | **Broken public contract.** The MCP protocol defines that tool errors must use `is_error=True`. A client checking `result.is_error` will see `false` and treat the error dict as a successful response. This breaks error handling for all consumers using `consumer_event_pending_list`. Broker integration WILL depend on correct error semantics. |

**Total MUST FIX: 1**

---

## 20. SHOULD FIX SOON

| # | Issue | Classification |
|---|-------|---------------|
| 1 | `json.dumps` without `allow_nan=False` — potential non-JSON output if float NaN/Infinity enters event data | SHOULD HARDEN |
| 2 | `pydantic` and `starlette` directly imported but not declared in `requirements.txt` | SHOULD FIX (packaging) |
| 3 | `synchronous=NORMAL` not configurable — should be configurable via `config.json` | SHOULD MAKE CONFIGURABLE |
| 4 | No startup warning when `enable_dns_rebinding_protection=false` on localhost | SHOULD ADD WARNING |
| 5 | Contract doc header wording: "1.2.0-candidate" vs code `1.2.0` | SHOULD CLARIFY WORDING |
| 6 | No explicit HTTP-level security tests (invalid Host/Origin → 421/403) | SHOULD ADD TESTS |
| 7 | No migration chain integration test (v6→v9 etc.) | SHOULD ADD TEST |

---

## 21. SAFE TO DEFER

| Item | Reason |
|------|--------|
| Multi-instance / shared DB | Single-process MVP design, documented limitation |
| Full observability stack (Prometheus/OpenTelemetry) | Out of scope for broker integration |
| Remote TLS / OAuth | Loopback-only deployment, not required |
| `synchronous=FULL` configurability | NORMAL is appropriate for current use case; make configurable (see #3 above) |
| Nuitka runtime verification | Packaging concern, addressed separately |
| Permanent test additions beyond #6-7 | Good to have but not blocking |

---

## 22. SOURCE EVIDENCE APPENDIX

| Source | URL | Conclusion Supported |
|--------|-----|---------------------|
| Python 3.14 json docs | https://docs.python.org/3.14/library/json.html | `allow_nan=True` is default; NaN/Infinity output as JS literals (not valid JSON) |
| Pydantic types docs | https://docs.pydantic.dev/latest/api/types/ | `StrictInt` strict mode rejects bool/string; `Annotated` + `Field(ge=0)` is public API |
| JSON Schema Core RFC | https://datatracker.ietf.org/doc/html/draft-bhutton-json-schema-00 §4.2.1, §7.6.1 | `boolean` and `integer` are distinct types; `minimum` only applies to number instances |
| MCP Spec | https://modelcontextprotocol.io/specification/2026-07-28 | Security section uses SHOULD (not MUST); transport security is implementation detail |
| MCP SDK transport_security.py (GitHub) | https://github.com/modelcontextprotocol/python-sdk/blob/main/src/mcp/server/transport_security.py | TODO confirms not ASGI middleware; `enable_dns_rebinding_protection` default True; `allowed_hosts/origins` default empty |
| Python Packaging Guide | https://packaging.python.org/en/latest/guides/writing-pyproject-toml/#dependencies-and-requirements | Direct imports should be declared as project dependencies |
| SQLite WAL docs | https://sqlite.org/wal.html | WAL with NORMAL synchronous: commits are fast; power-loss risk is ~30KB max |
| SQLite PRAGMA docs | https://sqlite.org/pragma.html#pragma_synchronous | `NORMAL` = "the database file syncs to OS cache after each batch"; `FULL` = "syncs after every write" |
| Python 3.14 sqlite3 docs | https://docs.python.org/3.14/library/sqlite3.html#sqlite3.connect | `check_same_thread=True` (default); `ProgrammingError` if connection used from different thread |
| Python 3.14 asyncio docs | https://docs.python.org/3.14/library/asyncio-eventloop.html | `asyncio.to_thread()` runs in worker thread pool; event loop thread remains free |
| RFC 3986 | https://www.rfc-editor.org/rfc/rfc3986.html#section-3.1 | Scheme syntax: `ALPHA *( ALPHA / DIGIT / "+" / "-" / "." )` |
| Installed MCP SDK func_metadata.py | `mcp/server/mcpserver/utilities/func_metadata.py:108` | `await anyio.to_thread.run_sync(...)` — sync tools run in worker thread |

**NOT ACCESSED:** MCP transport spec page (404 on modelcontextprotocol.io) — relied on installed SDK source and main spec page instead.

---

## 23. FINAL VERDICT

### A. Did you actually access official online sources in this run?
**YES.** 11 official sources accessed (Python docs, Pydantic docs, JSON Schema RFC, MCP spec, MCP SDK GitHub source, Python Packaging Guide, SQLite docs × 2, RFC 3986, asyncio docs). Plus installed SDK source inspected directly.

### B. Which official MCP spec revision did you verify?
**2026-07-28.** The code declares `MCP_SPEC = "2026-07-28"` and the SDK source matches. The spec page at modelcontextprotocol.io confirms this revision exists.

### C. Did any previous conclusions change after online verification?
**YES — two corrections:**
1. **MCP sync tool threading:** Previous report assumed sync tools run in the event loop thread. Official SDK source proves they run via `anyio.to_thread.run_sync()` in a worker thread pool. This does NOT change the SQLite thread-safety outcome (still safe), but the reasoning was wrong.
2. **`store.py` `{"status":"error"}` classification:** Previously classified as "SHOULD IMPROVE" based on assumption it was an internal diagnostic. After tracing the full call path, it IS returned through the MCP tool boundary as a success result. This elevates it to **MUST FIX**.

### D. Is there any real blocker before broker integration?
**YES — one confirmed blocker:**
- `store.py:359` `replay_events()` returns `{"status": "error", ...}` on exception instead of raising. The SDK wraps this as `CallToolResult(is_error=False)`, breaking the broker's error-handling contract.

### E. Should general MCP development remain closed?
**YES — after fixing the one MUST FIX item, general MCP development can close.** The architecture is sound, the SDK usage is correct, and all other findings are non-blocking improvements.

---

## FINAL EXACT VERDICT

**ONLINE-OFFICIAL RECONCILIATION FOUND MUST-FIX ITEMS — BROKER INTEGRATION SHOULD WAIT**

The single blocker is the `{"status": "error"}` anti-pattern in `store.py:359`. Fix is mechanical: change `return {"status": "error", "message": str(exc)}` to `raise StorageError(str(exc), exc)` (or equivalent) so the SDK wraps it as `is_error=True`. All other findings are non-blocking improvements.
