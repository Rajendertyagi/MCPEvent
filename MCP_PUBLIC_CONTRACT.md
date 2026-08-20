# MCP Event Server — Public Contract

**Version:** 1.0.0 (FROZEN) · 1.1.0-candidate (alert engine) · 1.2.0-candidate (metrics + recent + pagination, NOT FROZEN)
**MCP Spec:** 2026-07-28
**Status:** FROZEN (v1.0.0); v1.1.0-candidate additive — pending independent verification
**Last reviewed:** 2026-08-18
**Frozen:** 2026-08-18 — frozen only after independent naming-migration verification (verdict: PUBLIC MCP NAMING MIGRATION v1 VERIFIED — READY TO FREEZE)

---

## 1. Endpoint & Transport

| Item | Value | Classification |
|------|-------|---------------|
| Transport | `streamable-http` | **FREEZE** |
| Path | `/mcp` | **FREEZE** |
| Host | Configurable (`config.json` → `host`) | Runtime config — not protocol |
| Port | Configurable (`config.json` → `port`) | Runtime config — not protocol |
| `stateless_http` | `True` | **FREEZE** |
| `json_response` | `True` | **FREEZE** |
| `max_request_body_size` | From config (`max_request_body_size_mb * 1024 * 1024`) | Runtime config |
| `transport_security` | Explicit `TransportSecuritySettings` (DNS rebinding protection enabled; localhost-only allowed hosts/origins) | Runtime config — constructed from `config.json` keys `enable_dns_rebinding_protection`, `allowed_hosts`, `allowed_origins` |

**Client connection:** Connect to `http://{host}:{port}/mcp`. Use the MCP Python SDK `streamable_http_client` with `ClientSession`.

**HTTP liveness endpoint (not MCP):** `GET /health` → `200 {"status": "ok"}`. Unauthenticated. Does not require MCP initialization. Intended for local watchdog / process supervisor.

---

## 2. Production Tools (14)

These are the tools the broker project should depend on.

| Tool | Parameters | Description | Status |
|------|-----------|-------------|--------|
| `system_ping` | *(none)* | Health check. Returns `{"status": "ok", "message": "...", "timestamp": "..."}` | **FREEZE** |
| `event_publish` | `event_type` (str), `source` (str), `data` (dict, optional), `persistent` (bool, default False), `routing` (dict, optional) | Publish an event. If `persistent=True`, stores to SQLite and returns `sequence`. | **FREEZE** |
| `event_list` | `limit` (int, default 10) | Returns recent in-memory events from history buffer (max 50). | **FREEZE** |
| `consumer_register` | `consumer_id` (str) | Register a durable consumer identity. Idempotent. Also creates checkpoint at 0. | **FREEZE** |
| `consumer_topic_add` | `consumer_id` (str), `topic` (str) | Assign a topic to a consumer for topic-based routing. | **FREEZE** |
| `consumer_event_list` | `consumer_id` (str), `after_sequence` (int, optional), `limit` (int, default 50) | List persistent events relevant to a consumer, ordered by sequence ASC. Marks delivery. | **FREEZE** |
| `consumer_event_pending_list` | `consumer_id` (str), `limit` (int, default 50), `after_sequence` (int|null, optional) | Replay unacknowledged persistent events from consumer's checkpoint (or from explicit `after_sequence` for pagination). Primary reconnect tool. Returns `next_after_sequence` for paging. | **FREEZE** + **v1.2.0-candidate** |
| `consumer_event_acknowledge` | `consumer_id` (str), `event_id` (str) | ACK an event for a consumer. Idempotent. Advances checkpoint. | **FREEZE** |
| `consumer_checkpoint_get` | `consumer_id` (str) | Get the consumer's current durable checkpoint sequence. | **FREEZE** |
| `alert_create` | `consumer_id` (str), `source` (str), `field_path` (str), `operator` (str), `value` (JSON scalar), `name` (str, optional), `event_type` (str, optional), `one_shot` (bool, default True) | Create a generic alert definition. Fires when a `source` event satisfies `field_path <operator> value`. | **v1.1.0-candidate** |
| `alert_list` | `consumer_id` (str), `enabled` (bool, optional) | List alert definitions owned by a consumer. | **v1.1.0-candidate** |
| `alert_get` | `consumer_id` (str), `alert_id` (str) | Get a single alert definition (ownership-checked). Raises `AlertNotFoundError` if missing. | **v1.1.0-candidate** |
| `alert_enable` | `consumer_id` (str), `alert_id` (str) | Enable a disabled alert. Returns `changed=true` only if it was disabled. | **v1.1.0-candidate** |
| `alert_disable` | `consumer_id` (str), `alert_id` (str) | Disable an alert (stops evaluation, no deletion). Returns `changed=true` only if it was enabled. | **v1.1.0-candidate** |

---

## 3. Dev/Test Tools (7)

These are available but intended for development and testing. The broker project **should not depend on these**.

| Tool | Parameters | Description | Status |
|------|-----------|-------------|--------|
| `dev_progress_test` | `total` (int) | Demonstrates progress reporting via MCP Context. | **DEV ONLY** |
| `dev_long_running_test` | `duration_seconds` (float), `cancel_check_interval` (float) | Cancellable long-running operation for timeout/cancellation testing. | **DEV ONLY** |
| `dev_background_publish_test` | `event_type` (str), `persistent` (bool), `routing` (dict, optional) | Publishes an event from background context (no tool request). | **DEV ONLY** |
| `dev_task_list` | *(none)* | Lists background task names and statuses. | **DEV ONLY** |
| `dev_source_start` | `name` (str), `event_type` (str), `persistent` (bool), `delay_seconds` (float) | Starts a background coroutine that publishes after a delay. | **DEV ONLY** |
| `dev_source_fail` | `name` (str), `delay_seconds` (float) | Starts a background coroutine that raises RuntimeError. | **DEV ONLY** |
| `dev_source_stop` | `name` (str) | Stops a background test source. | **DEV ONLY** |

---

## 4. Resources (6)

| Resource URI | Data Shape | Description | Status |
|-------------|-----------|-------------|--------|
| `mcp-event://events/latest` | Event dict (see §5) | The most recently published event. Updated via `ResourceUpdated` notification. | **FREEZE** |
| `mcp-event://events/pending` | Array of event dicts (newest first, max 100) | All persistent events, newest first. | **FREEZE** |
| `mcp-event://events/recent` | Array of event dicts (newest first, max 200) | Bounded durable observational journal. Includes both persistent and nonpersistent events. **NOT pending delivery, NOT replay API, NOT ACKable, NOT checkpoint input.** Restart-safe. | **v1.2.0-candidate** |
| `mcp-event://system/info` | Dict with server metadata | Server name, version, features, limits, endpoint, uptime, counts. See §17 for field classification. | **FREEZE** |
| `mcp-event://system/metrics` | Dict with operational metrics | Process-wide counters: events, alerts, notifications, sources, recent history, system aggregates. No secrets or payloads. | **v1.2.0-candidate** |
| `mcp-event://sources/status` | Dict of source status objects | Status of each registered source connector. Includes name, type, state, error, cursor, dedup stats. Secrets are sanitized. | **FREEZE** |

---

## 5. Event Schema

### Published Event Shape

```json
{
  "id": "32-char lowercase UUID v4 hex",
  "type": "dot.namespaced.identifier",
  "source": "source-identifier",
  "timestamp": "ISO 8601 with UTC offset",
  "data": {},
  "persistent": false,
  "sequence": 42,
  "routing": {
    "targets": ["consumer_id_1"],
    "topics": ["alpha", "beta"]
  }
}
```

| Field | Type | Required | Frozen? | Notes |
|-------|------|----------|---------|-------|
| `id` | string (32 chars) | Always | **FREEZE** | UUID v4 hex. Collision-resistant. Stable identity. |
| `type` | string | Always | **FREEZE** | Dot-namespaced. Strip whitespace. Convention: `domain.action` (e.g. `alert.triggered`, `broker.price_changed`). |
| `source` | string | Always | **FREEZE** | Identifies the origin connector/instance. Strip whitespace. NOT the connector type — the instance name. |
| `timestamp` | string | Always | **FREEZE** | UTC ISO 8601. Generated server-side at publish time. |
| `data` | object | Always | **FREEZE** | Arbitrary JSON-compatible dict. Empty `{}` when none. |
| `persistent` | bool | Always | **FREEZE** | True if stored to SQLite. False for transient events. |
| `sequence` | int | When persistent | **FREEZE** | Monotonic SQLite auto-increment. Assigned at publish time. Only present when `persistent=True`. |
| `routing` | object | Optional | **FREEZE** | Present only when routing metadata was provided at publish. See §15. |

### Event ID Contract

- Format: `uuid.uuid4().hex` → 32 lowercase hex characters
- Generation: server-side at publish time
- Stability: immutable once assigned
- Broker should treat `id` as the stable event identity

### Event Type Convention

Recommended convention: `domain.action`

Examples:
- `alert.triggered` — alert engine fired
- `broker.price_changed` — market data update
- `source.failed` — source connector failure
- `system.warning` — server-side warning

The broker project should adopt this convention for its own event types.

---

## 6. Routing Contract

Routing metadata is **frozen at publication time**. It is never recomputed from current consumer subscriptions/topics.

| Routing value | Meaning |
|--------------|---------|
| `null` / absent | Broadcast — event is relevant to ALL registered consumers |
| `{"targets": ["c1", "c2"]}` | Targeted — event is relevant ONLY to listed consumers |
| `{"topics": ["alpha", "beta"]}` | Topic-based — event is relevant to consumers whose subscribed topics intersect |
| `{"targets": [...], "topics": [...]}` | Both — relevant to listed targets OR consumers with matching topics |
| `{"targets": []}` | Broadcast — empty targets list is treated as no-target filter (all consumers) |
| `{"topics": []}` | Broadcast — empty topics list is treated as no-topic filter (all consumers) |

**Important:** Routing is materialized into `consumer_event_state` at publish time. A consumer's later topic changes do NOT affect historical event relevance.

---

## 7. Consumer Identity Contract

| Aspect | Rule |
|--------|------|
| Meaning | Stable, durable application-level identity |
| Who creates | The client/application via `consumer_register` |
| Lifetime | Persistent across server restarts (stored in SQLite) |
| Re-registration | Idempotent — repeated calls are no-ops |
| Case | Case-sensitive (treated as opaque string) |
| Characters | Any non-empty string (no validation beyond non-empty after trim) |
| NOT bound to | MCP session, HTTP connection, Context, ClientSession, transport |

---

## 8. Delivery Contract

| Aspect | Rule |
|--------|------|
| `delivered_at` | First time the event was retrieved via `consumer_event_list` or `consumer_event_pending_list` for this consumer |
| Preserved | First delivery time is preserved on subsequent replays (CASE WHEN NULL) |
| NOT delivery | `ResourceUpdated` notification, publication, or SQLite write alone does NOT count as delivery |
| Idempotent | Repeated calls to `mark_delivered` for same (consumer, event) pair preserve first timestamp |

---

## 9. Acknowledgement Contract

| Aspect | Rule |
|--------|------|
| `consumer_event_acknowledge(consumer_id, event_id)` | Marks an event as processed by a consumer |
| Idempotent | Repeated calls succeed silently; first `acknowledged_at` timestamp is preserved |
| Per-consumer | Each consumer has independent ACK state per event |
| Does NOT delete | The persistent event remains in `persistent_events` after ACK |
| Checkpoint advance | After ACK, `advance_checkpoint` may advance the consumer's durable cursor |
| Unknown consumer | Raises `ConsumerNotFoundError` (internal application exception) → SDK wraps as `CallToolResult(is_error=True)`; client sees semantic message `consumer not found: <consumer_id>` |
| Unknown event | Raises `ValueError("event not found: ...")` → SDK wraps as error |
| Irrelevant event | Raises `ValueError("event ... is not relevant to consumer ...")` → SDK wraps as error |

---

## 10. Checkpoint Contract

**Invariant:** The checkpoint is the highest persistent sequence `N` such that there is no relevant, unacknowledged event with sequence ≤ `N`.

| Aspect | Rule |
|--------|------|
| Purpose | Durable cursor for replay/reconnect |
| Monotonic | Never regresses — `MAX(current, candidate)` |
| Gap-tolerant | Irrelevant events (not in consumer's `consumer_event_state`) do NOT block advancement |
| Initial value | 0 (created at `consumer_register`) |
| Advance trigger | Called after `consumer_event_acknowledge`; can also be called independently |

---

## 11. Replay Contract

| Aspect | Rule |
|--------|------|
| Tool | `consumer_event_pending_list(consumer_id, limit=50)` |
| Cursor | Starts from consumer's checkpoint (`last_sequence`) |
| Filter | Events with `sequence > checkpoint`, relevant to consumer, unacknowledged |
| Order | Ascending by sequence (no OFFSET pagination) |
| Default limit | 50 |
| Max limit | 500 (`MAX_REPLAY_LIMIT`) |
| Semantics | At-least-once — events may be returned multiple times if not ACKed |
| Delivery marking | Returned events are marked as delivered (preserving first delivery time) |
| Unknown consumer | Returns `{"status": "error", "message": "consumer not found: ..."}` |

---

## 12. Subscription / Notification Contract

| Aspect | Rule |
|--------|------|
| Mechanism | MCP `subscriptions/listen` + `ResourceUpdated` |
| Resource | `mcp-event://events/latest` |
| Trigger | Every call to `publish_event(persistent=True/False)` |
| Content | Notification carries URI only; client must `read_resource` or query tools for payload |
| Durable vs live | `ResourceUpdated` is a **live signal only** — it does NOT carry event history |
| Reconnect flow | Client receives `ResourceUpdated` → reads `mcp-event://events/latest` → calls `consumer_event_pending_list` for history |
| Missing bus | If bus not initialized, notification is silently skipped (clients can always poll) |

---

## 13. Error Contract

| Pattern | Behavior |
|---------|----------|
| Validation error (bad params) | Raises `ValidationError` → SDK wraps as `CallToolResult(is_error=True)` |
| Storage error (DB failure) | Raises `StorageError` → SDK wraps as `CallToolResult(is_error=True)` |
| Timeout error | Raises `OperationTimeoutError` → SDK wraps as `CallToolResult(is_error=True)` |
| Consumer not found | Raises `ConsumerNotFoundError` (internal application exception) → SDK wraps as `CallToolResult(is_error=True)`; client sees semantic message `consumer not found: <consumer_id>` |
| Protocol error | Reserved for `MCPError` (genuine protocol-level failures only) |

**Unknown-consumer policy (v1.0.0):** All five production operations that require an existing consumer — `consumer_topic_add`, `consumer_event_list`, `consumer_event_pending_list`, `consumer_event_acknowledge`, `consumer_checkpoint_get` — raise `ConsumerNotFoundError` for an unregistered consumer. The MCP SDK exposes this as `CallToolResult(is_error=True)` with the semantic message `consumer not found: <consumer_id>`. `consumer_register` remains an idempotent create/register operation. `ConsumerNotFoundError` is an **internal application/domain exception**, not a public MCP protocol type; broker clients must depend on `is_error=True` and the semantic message, not on Python exception class names.

---

## 14. Source Contract

| Concept | Meaning |
|---------|---------|
| Source type | Implementation class (e.g. `http_poller`, `test_source`) — mapped via `sources/registry.py` `SOURCE_TYPES` |
| Source name | Instance identifier from config (`source_name` key). Used for cursor/dedup identity. |
| Cursor | Durable progress marker stored in `source_state` table under key `"cursor"` |
| Dedup identity | Composite `(source_name, external_id)` in `source_seen_items` table |
| Status | Available via `mcp-event://sources/status` resource. URL secrets sanitized. |

---

## 15. `mcp-event://events/pending` Resource (formerly `alerts://pending`)

**RESOLVED BEFORE v1.0.0 FREEZE:** The resource was renamed from `alerts://pending` to `mcp-event://events/pending`. It returns ALL persistent events (not just "alerts"), newest first (max 100). The old `alerts://pending` URI no longer exists. The generic persistent-event listing is intentionally generic; a future alert engine will use separate `alert_definitions` / `alert_triggers` tables (see §16) and will NOT reuse this resource.

---

## 16. Generic Alert Engine (v1.1.0-candidate — NOT FROZEN)

Implemented in v1.1.0-candidate (additive on frozen v1.0.0). The engine is generic: it
matches a published event against persisted alert definitions and, on a match, publishes a
canonical `alert.triggered` event via the same `publish_event()` path used by all sources.
No broker-specific logic is present.

| Concept | Implemented name / pattern |
|---------|---------------------------|
| Alert definition | Table `alerts` (single table; supersedes the reserved `alert_definitions` + `alert_triggers` split from the pre-freeze plan) |
| Alert identity | `alert_id` (UUID v4 hex), `consumer_id` (owner) |
| Condition | `field_path` (dotted, e.g. `data.price`), `operator` (`eq`, `ne`, `gt`, `gte`, `lt`, `lte`), `value` (JSON scalar) |
| Optional scoping | `event_type` (null = any type from `source`), `source` (required) |
| Lifecycle | `enabled` (bool), `one_shot` (bool, default True → auto-disables after one trigger) |
| Trigger event | `type="alert.triggered"`, `source="alert_engine"`, `persistent=True`, `routing.targets=[consumer_id]` |
| Trigger payload | `alert_id`, `consumer_id`, `matched_event_id`, `matched_event_type`, `matched_source`, `field_path`, `operator`, `expected_value`, `observed_value`, `one_shot` (+ `name` when set) |

**Semantics (v1.1.0-candidate):**
- Field absence (`field_path` not present in `data`) → condition does NOT match. JSON `null` is a legitimate value that participates in `eq`/`ne`.
- `eq`/`ne`: scalar equality (type-aware; bool distinct from numeric). `gt`/`gte`/`lt`/`lte`: numeric only (bool is NOT numeric); non-numeric observed/expected → no match (not an error). No string→number coercion.
- `alert_enable` / `alert_disable` return `changed=true` only when the state actually flips; idempotent calls return `changed=false`. `AlertNotFoundError` is raised for unknown/non-owned alerts; `ConsumerNotFoundError` for unknown consumers.
- Recursion guard: `alert.triggered` events are never re-evaluated as alert input.
- Per-alert concurrency lock prevents double-trigger within a process; `one_shot` alerts disable atomically after the trigger event is published.

**No naming conflicts** with the frozen v1.0.0 event schema or tool names. The `alerts://pending`
URI was renamed to `mcp-event://events/pending` before freeze (see §15).

---

## 17. mcp-event://system/info Field Classification

| Field | Classification | Notes |
|-------|---------------|-------|
| `name` | **FREEZE** | Server identity from config |
| `version` | **FREEZE** | App version (semver) |
| `purpose` | Internal | Descriptive, not contract-critical |
| `transport` | **FREEZE** | Fixed: `streamable-http` |
| `endpoint` | **FREEZE** | Derived from config host/port |
| `python` | Internal | Build detail — broker should not depend on |
| `mcp_sdk` | Internal | SDK version — may change |
| `mcp_spec` | **FREEZE** | Protocol spec date |
| `event_resource` | **FREEZE** | URI constant |
| `events_pending_resource` | **FREEZE** | URI constant (`mcp-event://events/pending`) |
| `info_resource` | **FREEZE** | URI constant (`mcp-event://system/info`) |
| `event_count` | Diagnostic | Dynamic counter |
| `persistent_event_count` | **FREEZE** | Count of all persistent events |
| `consumer_count` | Diagnostic | Dynamic counter |
| `uptime_seconds` | Diagnostic | Derivable |
| `started_at` | Diagnostic | Derivable |
| `features` | **FREEZE** | Feature capability map |
| `limits` | **FREEZE** | Config-driven limits |

---

## 18. Compatibility Policy

### After contract freeze:

| Change type | Allowed? | Notes |
|------------|----------|-------|
| Add new tool | ✅ Additive | Must not conflict with existing names |
| Add optional event fields | ✅ Additive | Consumers should ignore unknown fields |
| Add new resource URI | ✅ Additive | Must not conflict with existing URIs |
| Rename tool | ❌ Breaking | Requires contract version bump |
| Rename resource URI | ❌ Breaking | Requires contract version bump |
| Change event field meaning | ❌ Breaking | Must not silently change semantics |
| Change routing semantics | ❌ Breaking | Routing freeze at publication is a hard invariant |
| Remove tool | ❌ Breaking | Must deprecate first |
| Change default parameter values | ⚠️ Risky | May break clients relying on defaults |
| Change error shape | ⚠️ Risky | SDK wraps all app exceptions as `is_error=True` |

### Recommended versioning approach:

Add a `contract_version` field to `mcp-event://system/info` when ready:
```json
{"contract_version": "1.0.0", ...}
```

This allows the broker project to declare compatibility requirements.

---

## 19. Broker Project Integration Rules

### ✅ Safe to depend on:

- MCP tool names and parameters (as listed in §2)
- Resource URIs (as listed in §4)
- Event schema fields (as listed in §5)
- Routing semantics (as listed in §6)
- Consumer identity semantics (as listed in §7)
- Checkpoint/replay semantics (as listed in §§9-11)
- Subscription/notification model (as listed in §12)

### ❌ Must NOT depend on:

- `store_modules/*` — internal persistence modules
- `server_modules/*` — internal server modules
- `EventStore` class directly — use MCP tools
- SQLite schema details — use tools
- Internal logger names
- Process IDs or internal globals
- Test tool names (prefix `_test` or listed in §3)

---

## 19. Canonical Contract Module

Public MCP contract identifiers are canonically defined in:

    server_modules/contract.py

Production code imports from this module rather than redefining literals.
External clients depend on literal protocol values, not internal Python module paths.

Ownership model:
    MCP public contract identifiers  → server_modules/contract.py
    Runtime/deployment defaults       → server_modules/config.py
    Schema/migration version          → store_modules/schema.py
    Domain exceptions                 → errors.py
    Event-core behavior               → events.py
    Source type registry              → sources/registry.py

Engineering principle: ONE STABLE CONCEPT → ONE CANONICAL OWNER → IMPORT/REUSE.

---

## 19b. mcp-event://system/metrics Resource (v1.2.0-candidate)

**Status:** CANDIDATE · NOT FROZEN

Returns a JSON object with process-wide operational counters. No secrets, payloads, tokens, or filesystem paths are exposed. All counters are process-lifetime; they reset on restart.

```json
{
  "started_at": "ISO8601",
  "uptime_seconds": 123.4,
  "events": {
    "published_total": 0,
    "persistent_total": 0,
    "nonpersistent_total": 0,
    "publication_failures_total": 0,
    "alert_triggered_total": 0
  },
  "alerts": {
    "evaluations_total": 0,
    "matches_total": 0,
    "failures_total": 0
  },
  "notifications": {
    "attempted_total": 0,
    "failed_total": 0
  },
  "sources": {
    "published_total": 0,
    "failures_total": 0
  },
  "recent_history": {
    "failures_total": 0,
    "count": 0,
    "capacity": 200
  },
  "system": {
    "persistent_event_count": 0,
    "persistent_high_water": 0,
    "consumer_count": 0,
    "pending_deliveries": 0
  }
}
```

**Counter semantics:**
- `events.publication_failures_total` counts `publish_event` attempts that fail **before** successful acceptance (validation failures + persistent-save failures). It does NOT count recent-journal, notification, or alert-evaluation failures (those have their own counters).
- `alerts.evaluations_total` increments once per event handed to the evaluator (not once per candidate).
- `alerts.matches_total` increments per matching alert definition.
- `alerts.failures_total` counts unexpected evaluator exceptions.
- `sources.published_total` is a distinct dimension from `events.published_total`; a source event increments both (source-originated subset).
- `system.persistent_event_count = COUNT(*)`; `system.persistent_high_water = MAX(sequence)`.

---

## 19c. mcp-event://events/recent Resource (v1.2.0-candidate)

**Status:** CANDIDATE · NOT FROZEN

Returns the bounded durable observational event journal (max 200 events, newest-first).

**Semantics:**
- **Observational only** — not pending delivery, not replay API, not ACKable, not checkpoint input.
- Includes both `persistent=True` and `persistent=False` events.
- Survives restart (hydrated from SQLite on startup).
- A nonpersistent event in this journal does NOT enter `consumer_event_state`, pending, ACK, or checkpoint.

**Response shape:** Array of event dicts (same schema as §5), newest first.

---

## 19d. consumer_event_pending_list Pagination (v1.2.0-candidate)

**Status:** CANDIDATE · NOT FROZEN

The tool now accepts an optional `after_sequence` parameter for pagination:

```
consumer_event_pending_list(
    consumer_id: str,
    limit: int = 50,
    after_sequence: int | None = None
)
```

- `after_sequence = None` → existing checkpoint-based behavior (unchanged).
- `after_sequence = N` → returns relevant pending events with `sequence > N`.
- Returns `next_after_sequence` which is valid input for the next page.
- No OFFSET; ascending sequence; max limit 500.
- ACK and checkpoint semantics unchanged.
- Validation rejects `bool`, float, negative, or string values.

---

## 20. Open Issues (Tracked Separately — Non-Blocking for Frozen Contract)

| # | Issue | Severity | Status |
|---|-------|----------|--------|
| 1 | `alerts://pending` → `mcp-event://events/pending` rename | **RESOLVED** | Renamed before freeze; old URI removed |
| 2 | `persistent_alert_count` → `persistent_event_count` rename | **RESOLVED** | Renamed before freeze |
| 3 | Unknown consumer behavior inconsistency | **RESOLVED** | All five consumer-requiring tools now raise `ConsumerNotFoundError` → `is_error=True` with `consumer not found: <id>` |
| 4 | `mcp-event://system/info` exposes `python` and `mcp_sdk` versions | **LOW** | Classified as diagnostic (not frozen broker dependency) |
| 5 | `json_response=True` + background publication acceptance test not yet run | **INFO** | Tracked separately; architecture supports it; not a contract blocker |
| 6 | Test harness teardown port-race (WinError 10048/10053) discovered during verification | **HARNESS** | Tracked separately; NOT a contract defect; does not affect the frozen public contract |

---

## 21. Contract Status

| Section | Status |
|---------|--------|
| Endpoint & Transport | ✅ FROZEN |
| Production Tools (14) | ✅ FROZEN (9) + **v1.1.0-candidate** (5 alert) |
| Dev/Test Tools (7) | ✅ IDENTIFIED — not for broker dependency |
| Resources (4 + 2 candidate) | ⚠️ 4 FROZEN + 2 CANDIDATE |
| Event Schema | ✅ FROZEN |
| Routing | ✅ FROZEN |
| Consumer Identity | ✅ FROZEN |
| Delivery | ✅ FROZEN |
| ACK | ✅ FROZEN |
| Checkpoint | ✅ FROZEN |
| Replay | ✅ FROZEN |
| Subscription/Notification | ✅ FROZEN |
| Error Contract | ✅ FROZEN |
| Source Contract | ✅ FROZEN |
| Versioning Policy | ✅ FROZEN |
| Generic Alert Engine (5 tools) | ⚠️ v1.1.0-candidate — NOT FROZEN |
| Observability / Metrics (1 resource) | ⚠️ v1.2.0-candidate — NOT FROZEN |
| Durable Recent History (1 resource) | ⚠️ v1.2.0-candidate — NOT FROZEN |
| Replay Pagination (`after_sequence`) | ⚠️ v1.2.0-candidate — NOT FROZEN |

---

## 22. Verdict

```text
PUBLIC MCP CONTRACT v1.0.0 — FROZEN
```

**Frozen:** 2026-08-18 — frozen only after independent naming-migration verification (verdict: PUBLIC MCP NAMING MIGRATION v1 VERIFIED — READY TO FREEZE).

**Resolved before freeze:**
- `alerts://pending` → `mcp-event://events/pending` (old URI removed)
- `persistent_alert_count` → `persistent_event_count`
- Unknown-consumer policy normalized: all five consumer-requiring tools raise `ConsumerNotFoundError` → `is_error=True` with `consumer not found: <id>`

**Non-blocking items tracked separately (do NOT affect the frozen contract):**
- Empirical `json_response=True` + background notification acceptance test (architecture supports it; not a contract blocker)
- Test harness teardown port-race (WinError 10048/10053) discovered during verification — harness defect, not a contract defect

**Compatibility policy (post-freeze):** Renaming/removing tools or resources, changing required parameters, event-field semantics, routing semantics, consumer identity, ACK/checkpoint/replay semantics requires an explicit breaking/versioned contract decision. Additive tools, resources, and optional event fields remain backward-compatible where appropriate.

**CONTRACT_VERSION is `1.2.0-candidate` — not frozen.**

Historical frozen baseline is `1.0.0`. The v1.1.0-candidate alert engine and v1.2.0-candidate
observability features (metrics, recent-events journal, replay pagination) are additive and
not yet frozen. The production tool surface is 14 tools (9 original frozen + 5 alert candidate)
plus 6 resources (4 frozen + 2 candidate).
