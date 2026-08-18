# Store & Server Split Plan — Facade Package Pattern

**Goal**: Split `store.py` (1153 lines) and `server.py` (947 lines) into focused submodules while preserving all existing imports so tests require zero changes.

---

## Directory Structure After Split

```
store/
├── __init__.py          # Facade: re-exports EventStore, SCHEMA_VERSION, MAX_REPLAY_LIMIT
├── _base.py             # EventStore base class (shared state, _open(), migrations)
├── schema.py            # Schema creation & migration methods
├── events.py            # save(), list_pending(), list_relevant_events(), count()
├── consumers.py         # register_consumer(), add_topic(), acknowledge_event(), checkpoints
├── replay.py            # replay_events(), is_event_relevant()
└── source_state.py      # get/set_source_state(), source_item_seen(), prune_source_seen_items()

server/
├── __init__.py          # Facade: re-exports everything server.py currently exposes
├── config.py            # DEFAULTS, _load_config(), ConfigError, validate_config()
├── resources.py         # event_latest(), alerts_pending(), server_info(), sources_status()
├── tools/
│   ├── __init__.py      # Re-exports all tool handlers
│   ├── event_tools.py   # generate_event(), list_events()
│   ├── consumer_tools.py # register_consumer(), add_consumer_topic(), get_pending_events(), acknowledge_event(), get_consumer_checkpoint()
│   ├── query_tools.py   # list_relevant_events()
│   └── test_tools.py    # progress_report_test(), long_running_test(), background_publish_test(), start/stop_failing_source()
├── lifecycle.py         # _print_banner(), _print_shutdown(), main startup
└── helpers.py           # _log_startup(), _log_error(), _run_with_timeout()
```

---

## store/ — Facade Design

### `store/__init__.py`
```python
"""SQLite persistence layer — facade preserving backward-compatible imports."""
from ._base import EventStore  # noqa: F401
from .schema import SCHEMA_VERSION, MAX_REPLAY_LIMIT  # noqa: F401

__all__ = ["EventStore", "SCHEMA_VERSION", "MAX_REPLAY_LIMIT"]
```

### `store/_base.py` (moved from store.py lines 33-185, 652-670)
```python
"""EventStore core: init, connection management, schema version, helper methods."""
import json
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

SCHEMA_VERSION = 7
MAX_REPLAY_LIMIT = 500

class EventStore:
    def __init__(self, db_path: str) -> None: ...
    @staticmethod
    def _open(db_path: str) -> sqlite3.Connection: ...
    def _ensure_directory(self) -> None: ...
    def _get_schema_version(conn) -> int: ...
    @staticmethod
    def _row_to_event(row) -> dict: ...
    @property
    def db_path(self) -> str: ...
    # Delegates to submodules:
    def save(self, ...): from .events import save; return save(self, ...)
    # ... etc
```

### `store/schema.py` (moved from store.py lines 103-490)
```python
"""Schema creation and migration logic."""
# Contains: _create_v7_schema(), _migrate_v1_to_v3(), _migrate_v2_to_v3(),
#            _migrate_v3_to_v4(), _migrate_v4_to_v5(), _migrate_v5_to_v6(),
#            _migrate_v6_to_v7(), _create_v3_schema_partial()
```

### `store/events.py` (moved from store.py lines 492-670)
```python
"""Event persistence: save, list_pending, list_relevant_events, count."""
def save(store, ...): ...
def list_pending(store, limit): ...
def list_relevant_events(store, ...): ...
def count(store): ...
```

### `store/consumers.py` (moved from store.py lines 675-916)
```python
"""Consumer registry, topics, acknowledgements, checkpoints."""
def register_consumer(store, consumer_id): ...
def add_topic(store, consumer_id, topic): ...
def acknowledge_event(store, consumer_id, event_id): ...
def get_checkpoint(store, consumer_id): ...
def advance_checkpoint(store, consumer_id, seq): ...
```

### `store/replay.py` (moved from store.py lines 918-1025)
```python
"""Event replay and relevance checking."""
def replay_events(store, consumer_id, limit): ...
def is_event_relevant(store, routing, consumer_id, topics): ...
```

### `store/source_state.py` (moved from store.py lines 1027-1153)
```python
"""Source state (cursors) and deduplication."""
def get_source_state(store, source_name, key): ...
def set_source_state(store, source_name, key, value): ...
def source_item_seen(store, source_name, external_id): ...
def mark_source_item_seen(store, source_name, external_id, seen_at): ...
def prune_source_seen_items(store, source_name, max_items): ...
```

---

## server/ — Facade Design

### `server/__init__.py`
```python
"""MCP Event Server — facade preserving backward-compatible imports."""
# Re-export everything the old server.py exposed
from .config import DEFAULTS, ConfigError, validate_config  # noqa: F401
from .helpers import _log_startup, _log_error  # noqa: F401
from .resources import (  # noqa: F401
    event_latest, alerts_pending, server_info, sources_status
)
from .tools import (  # noqa: F401
    ping, generate_event, list_events, register_consumer,
    add_consumer_topic, list_relevant_events, get_pending_events,
    acknowledge_event, get_consumer_checkpoint,
    progress_report_test, long_running_test,
    background_publish_test, list_background_tasks,
    start_test_source, start_failing_source, stop_test_source,
)
from .lifecycle import _print_banner, _print_shutdown  # noqa: F401

# Module-level constants (migrated from server.py top-level)
SERVER_VERSION = "0.2.0"
MCP_SPEC = "2026-07-28"
EVENT_RESOURCE_URI = "event://latest"
INFO_RESOURCE_URI = "server://info"
ALERTS_RESOURCE_URI = "alerts://pending"
SOURCES_RESOURCE_URI = "sources://status"
DEFAULT_CONFIG_PATH = "config.json"

__all__ = [
    "EventStore", "ConfigError", "DEFAULTS",
    "SERVER_VERSION", "MCP_SPEC",
    "EVENT_RESOURCE_URI", "INFO_RESOURCE_URI",
    "ALERTS_RESOURCE_URI", "SOURCES_RESOURCE_URI",
    # ... all tool handlers
]
```

### `server/config.py` (moved from server.py lines 61-256)
```python
"""Configuration loading and validation."""
DEFAULTS = { ... }
def _load_config(config_path=DEFAULT_CONFIG_PATH): ...
class ConfigError(Exception): ...
def validate_config(config): ...
```

### `server/helpers.py` (moved from server.py lines 97-128)
```python
"""Logging helpers."""
def _log_startup(msg): ...
def _log_error(msg, exc=None): ...
```

### `server/resources.py` (moved from server.py lines 352-429)
```python
"""MCP resource handlers."""
@mcp.resource(EVENT_RESOURCE_URI)
def event_latest(): ...

@mcp.resource(ALERTS_RESOURCE_URI)
def alerts_pending(): ...

@mcp.resource(INFO_RESOURCE_URI)
def server_info(): ...

@mcp.resource(SOURCES_RESOURCE_URI)
def sources_status(): ...
```

### `server/tools/__init__.py`
```python
"""Re-export all tool handlers."""
from .event_tools import generate_event, list_events  # noqa: F401
from .consumer_tools import register_consumer, add_consumer_topic, get_pending_events, acknowledge_event, get_consumer_checkpoint  # noqa: F401
from .query_tools import list_relevant_events  # noqa: F401
from .test_tools import (  # noqa: F401
    progress_report_test, long_running_test,
    background_publish_test, list_background_tasks,
    start_test_source, start_failing_source, stop_test_source,
)
```

### `server/tools/event_tools.py`
```python
"""Event-related MCP tools."""
@mcp.tool()
async def generate_event(...): ...

@mcp.tool()
def list_events(limit=10): ...
```

### `server/tools/consumer_tools.py`
```python
"""Consumer-related MCP tools."""
@mcp.tool()
def register_consumer(consumer_id): ...

@mcp.tool()
def add_consumer_topic(consumer_id, topic): ...

@mcp.tool()
async def get_pending_events(consumer_id, limit): ...

@mcp.tool()
async def acknowledge_event(consumer_id, event_id): ...

@mcp.tool()
async def get_consumer_checkpoint(consumer_id): ...
```

### `server/tools/test_tools.py`
```python
"""Test/debug MCP tools."""
@mcp.tool()
async def progress_report_test(total): ...

@mcp.tool()
async def long_running_test(duration_seconds, cancel_check_interval): ...

@mcp.tool()
async def background_publish_test(...): ...

@mcp.tool()
def list_background_tasks(): ...

@mcp.tool()
async def start_test_source(name, event_type, delay_seconds, ...): ...

@mcp.tool()
async def start_failing_source(name, delay_seconds): ...

@mcp.tool()
async def stop_test_source(name): ...
```

### `server/lifecycle.py` (moved from server.py lines 886-947)
```python
"""Startup banner, shutdown, main entry point."""
def _print_banner(): ...
def _print_shutdown(): ...

if __name__ == "__main__":
    _print_banner()
    # ... rest of main
```

---

## Import Compatibility Matrix

| Old Import | New Import | Test Impact |
|------------|------------|-------------|
| `import store as store_mod` | `import store as store_mod` (facade) | **None** |
| `from store import EventStore` | `from store import EventStore` (facade) | **None** |
| `store.register_consumer(...)` | `store.register_consumer(...)` (delegates) | **None** |
| `store.acknowledge_event(...)` | `store.acknowledge_event(...)` (delegates) | **None** |
| `store.replay_events(...)` | `store.replay_events(...)` (delegates) | **None** |
| `import server as server_mod` | `import server as server_mod` (facade) | **None** |
| `server_mod.EventStore` | `server_mod.EventStore` (re-exported) | **None** |

---

## Migration Steps

### Phase 1: Create `store/` package
1. Create `store/__init__.py` with facade re-exports
2. Extract schema/migration code → `store/schema.py`
3. Extract event persistence → `store/events.py`
4. Extract consumer logic → `store/consumers.py`
5. Extract replay logic → `store/replay.py`
6. Extract source state/dedup → `store/source_state.py`
7. Update `store.py` to be a thin facade that imports from submodules
8. Run `python test/test_acknowledgement.py` to verify

### Phase 2: Create `server/` package
1. Create `server/__init__.py` with facade re-exports
2. Extract config → `server/config.py`
3. Extract resources → `server/resources.py`
4. Extract tools by domain → `server/tools/*.py`
5. Extract lifecycle → `server/lifecycle.py`
6. Update `server.py` to be a thin facade
7. Run `python test/test_events.py` to verify

### Phase 3: Cleanup
1. Remove old `store.py` and `server.py` (or keep as thin wrappers)
2. Update any remaining direct imports
3. Run full suite: `python test/run_all.py`

---

## Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| Circular imports between submodules | Use lazy imports inside methods, not at module level |
| Test imports break | Facade pattern ensures `from store import EventStore` still works |
| `store.py` is imported as module (`import store`) | `store/__init__.py` must expose all public names |
| `server.py` as `__main__` entry | Keep `if __name__ == "__main__"` in `server/lifecycle.py`, re-export from `server/__init__.py` |
| R5 test checks `server.py` internals | Test checks for `HttpJsonPoller`/`TestSource` attributes — these won't exist in new structure, test may need update |

---

## Estimated Line Counts

| Module | Lines |
|--------|-------|
| `store/__init__.py` | ~30 |
| `store/_base.py` | ~100 |
| `store/schema.py` | ~390 |
| `store/events.py` | ~180 |
| `store/consumers.py` | ~170 |
| `store/replay.py` | ~110 |
| `store/source_state.py` | ~130 |
| **store/ total** | **~1110** |
| | |
| `server/__init__.py` | ~80 |
| `server/config.py` | ~120 |
| `server/helpers.py` | ~30 |
| `server/resources.py` | ~80 |
| `server/tools/__init__.py` | ~30 |
| `server/tools/event_tools.py` | ~60 |
| `server/tools/consumer_tools.py` | ~120 |
| `server/tools/query_tools.py` | ~40 |
| `server/tools/test_tools.py` | ~120 |
| `server/lifecycle.py` | ~70 |
| **server/ total** | **~750** |

---

## Decision Points

1. **Keep old files as thin wrappers?** Yes — preserves any external scripts that import `store` or `server` directly.
2. **Package vs flat files?** Package (`store/`, `server/`) is cleaner but requires `__init__.py` facades. Flat files (`store_events.py`, etc.) would break imports.
3. **Split server.py fully or partially?** Full split recommended — tool handlers are the largest concern (300+ lines).
4. **Test changes needed?** Zero if facade pattern is followed correctly. R5 test may need adjustment (see risks).

---

## Recommendation

Proceed with **Phase 1 first** (split `store.py`). It has cleaner boundaries and tests already use direct `EventStore` imports. After verifying Phase 1 passes, proceed with Phase 2.
