# MCP Event Server — Test Suite

Feature-focused, fast, and hang-safe regression tests for the MCP event server
(`server.py`, `events.py`, `store.py`, `runtime.py`, `sources/`).

## Layout

```
test/
  run_all.py            # regression driver: isolated subprocess per file, hard 300s/file timeout
  helpers/              # lifecycle, mcp, wait, mock_http, runner — shared test support
  mcp_result.py         # SDK result normalization + safe teardown
  test_*.py             # one feature per file (the units of the suite)
  config.json           # base server config (overwritten per-test, restored after)
TEST_RUNTIME_MAP.md     # per-file server usage, harness behavior, run guidance
```

Production source under `test/` is **frozen** — do not edit `server.py`, `events.py`,
`store.py`, `runtime.py`, `errors.py`, `client.py`, `config.json`, `requirements.txt`,
or `sources/*` as part of test work.

## Run

```bash
# Focused (preferred while debugging one feature):
python test/test_acknowledgement.py
python test/run_all.py --group fast

# Full regression (CI / release only — slow path):
python test/run_all.py
python test/run_all.py --timeout 300
```

See `TEST_RUNTIME_MAP.md` for the server-usage matrix, the bounded-wait strategy, and
why you should run focused, not full, during development.

## Principles

- **No server unless the test needs the MCP/HTTP boundary.** Application logic
  (store, events, routing, checkpoint/ack, background tasks, source lifecycle, dedup)
  is tested directly against the real objects with an in-memory stub bus.
- **One server per D-level file** (exceptions: restart tests in `test_errors.py` /
  `test_reconnect.py`, and `test_subscriptions.py` whose tests need different source
  configs).
- **Bounded waits, no long fixed sleeps.** Readiness/counts are polled with timeouts.
- **Hard timeout (300 s/file) + parent-owned cleanup** — the runner terminates the child's
  process group, so a hung file fails fast (reported as **TIMEOUT**) and any `server.py`
  it spawned is killed with it. No reliance on the child's `atexit` (which can't run after
  a hard kill).

## Deleted (do not recreate)

`test/test_phase8.py`, `test/integrate_test.py` — replaced by the per-feature files above.
