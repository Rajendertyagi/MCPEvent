"""
Static source-type registry.

This module maps configuration ``"type"`` strings to concrete source classes.
It uses explicit imports and a static dictionary — NOT dynamic import scanning,
entry points, or plugin directories — so the project remains compatible with
Nuitka onefile/standalone compilation.

To add a new built-in source:
    1. implement the source class (e.g. sources/my_source.py)
    2. import it here and add an entry to SOURCE_TYPES
    3. add a config entry with "type": "<key>"

No changes to server.py, events.py, store.py, or the MCP transport are required.
"""

from __future__ import annotations

from typing import Any, Callable, Type

from sources.http_poller import HttpJsonPoller
from sources.test_source import TestSource

# type key -> source class
SOURCE_TYPES: dict[str, Type[Any]] = {
    "http_poller": HttpJsonPoller,
    "test_source": TestSource,
}

# Re-export for convenience / typing clarity.
SourceFactory = Callable[[dict[str, Any]], Any]
