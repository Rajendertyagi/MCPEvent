# AGENT.md — working in the test suite

Guidance for an agent (or human) modifying or extending the MCP event server tests.

## Scope guard

- **Production code is frozen.** Do not edit `server.py`, `events.py`, `store.py`,
  `runtime.py`, `errors.py`, `client.py`, `config.json`, `requirements.txt`, or
  `sources/*` during test work. This is a TEST/HARNESS task, not a product change.
- The deleted monoliths `test/test_phase8.py` and `test/integrate_test.py` must **not**
  be recreated, wrapped, or re-added to `run_all.py`.

## When adding or changing a test

1. **Prefer a direct (0-server) test.** Import the real objects
   (`EventStore`, `events`, `runtime.BackgroundTaskManager`, `sources.build_source_manager`,
   `sources.create_publisher`, `sources.http_poller.HttpJsonPoller`) and an injectable
   `_StubBus` (`async def publish(self, item): ...`). See `test_acknowledgement.py`,
   `test_consumers.py`, `test_source_dedup.py`, `test_source_lifecycle.py`,
   `test_background_tasks.py`.
2. **Only start a server when you test the MCP/HTTP boundary** (tools, resources,
   subscriptions, restart, multi-client, performance). Use `helpers.lifecycle.start_server`
   / `stop_server` / `restore_environment` and `helpers.mcp.call`.
3. **Mirror the real MCP tool in direct tests.** The `acknowledge_event` tool chains
   `store.acknowledge_event` **and** `store.advance_checkpoint` — a direct test that calls
   only the first will see a checkpoint stuck at 0. Use an `_ack(store, cid, eid)` helper.
4. **Register consumers BEFORE publishing** if the test will `replay_events` them.
   `register_consumer` does not backfill existing events; `consumer_event_state` is
   materialized at publish time. (See `test_source_lifecycle.py` S5/S14.)
5. **Use bounded waits, not fixed sleeps.** `helpers.wait.wait_for_value` /
   `wait_until`; every `helpers.mcp.call` already honors `MCP_CALL_TIMEOUT`.
6. **Don't orphan a server.** If a file starts a server at file scope, every test must
   use it or explicitly `stop_server()` before starting another; otherwise the module-global
   `_server_proc` is overwritten and the old server leaks (port held after exit).

## Running

- While iterating on ONE feature: run that file directly, or `--group <name>`.
  **Do not** run the full `run_all.py` regression in a loop — it is the slow path.
- `run_all.py` runs each file as an isolated subprocess with a **hard 300 s/file timeout**
  and the RUNNER owns cleanup via **process-group signaling** (Windows `CTRL_BREAK_EVENT`
  then `taskkill /F /T /PID` tree; POSIX `SIGKILL` to the group). It does **not** rely on
  the child's `atexit` — a force-killed process can't run it. A hung file fails fast as
  **TIMEOUT** and any `server.py` it spawned is killed with it (no orphan).

## Gotchas already fixed (don't reintroduce)

- `helpers/mcp.py` needs `import time` (used by `wait_source_ready` / `wait_for_event_count`).
- The real store method is `add_topic`, not `add_consumer_topic`.
- `BackgroundTaskManager.cancel` removes the task from its dict but does **not** call
  `task.cancel()`; the source loop stops via its `stop_event` and sets `_state="stopped"`.
  Wait for the terminal state, not `active_count == 0`.

See `TEST_RUNTIME_MAP.md` for the full matrix.
