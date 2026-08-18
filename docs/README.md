# Test Suite Documentation

## Quick Reference

Run individual feature tests:
```bash
python test/test_unit_sources.py          # No server needed
python test/test_source_dedup.py          # Source dedup behavior
python test/test_source_lifecycle.py      # Source enable/disable/failure
python test/test_events.py                # Core event flow
python test/test_consumers.py             # Consumer/topic behavior
python test/test_acknowledgement.py       # Ack/checkpoint logic
python test/test_reconnect.py             # Restart persistence
python test/test_errors.py                # Error handling
python test/test_timeouts.py              # Timeout behavior
python test/test_background_tasks.py      # Background tasks
python test/test_subscriptions.py         # Live notifications
python test/test_performance.py           # Performance sanity
python test/test_lifespan.py              # Server lifespan
python test/test_sdk_alignment.py         # MCP SDK integration
python test/test_multi_client.py          # Concurrent clients
```

Run full regression:
```bash
python test/run_all.py
```

## Workflow

When working on a feature:

1. **Run the smallest relevant test first**
   - Changing event model → `test_events.py`
   - Changing routing → `test_consumers.py`
   - Changing replay → `test_events.py` (T9)
   - Changing source cursor → `test_source_lifecycle.py` (S7)
   - Changing MCP error behavior → `test_errors.py` + `test_sdk_alignment.py`

2. **Run related feature tests**
   - After event changes: `test_events.py` + `test_consumers.py` + `test_acknowledgement.py`

3. **Run full regression when ready**
   - `python test/run_all.py`

## Test Levels

### Level 1 — Unit Tests (no server)
- `test_unit_sources.py` — Fastest, pure Python

### Level 2 — Feature Tests (server needed)
- `test_source_dedup.py`, `test_source_lifecycle.py`, `test_events.py`, etc.
- Each starts its own server, runs tests, cleans up

### Level 3 — Full Regression
- `test/run_all.py` — Runs all feature tests in order

## Key Design Decisions

- **Each test file is independently runnable** — no dependency on execution order
- **Shared helpers** in `test/helpers/` prevent code duplication
- **Dynamic ports** — no hardcoded ports that could conflict
- **Config isolation** — original config.json is captured once and restored after each test
- **Data isolation** — each test uses isolated data directories

## Known Issues

- MCP SDK `streamable_http_client` can intermittently raise `ExceptionGroup` on Python 3.14
- The readiness probe uses TCP check as primary indicator (MCP ping is best-effort)
- Some tests may fail if stale Python processes exist from previous runs
