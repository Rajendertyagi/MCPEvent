#!/usr/bin/env python3
"""
Lifespan (app context) test.

Extracted from integrate_test.py (P7T19).

Run:
    python test/test_lifespan.py
"""

from __future__ import annotations

import asyncio
import os
import sys
import time

# Ensure project root is importable.
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_DIR = os.path.dirname(_SCRIPT_DIR)
for _p in (_PROJECT_DIR, _SCRIPT_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from helpers.lifecycle import (  # noqa: E402
    restore_environment,
    start_server,
)
from helpers.mcp import (  # noqa: E402
    call,
)
from helpers.runner import R  # noqa: E402


# ===================================================================
# Lifespan Test
# ===================================================================


async def p7t19_app_context_lifespan(runner: R) -> None:
    """P7T19: App context (lifespan configured) — server starts and responds."""
    name = "P7T19-lifespan"
    # Lifespan is configured if server starts and responds
    data = await call("system_ping")
    runner.assert_eq(name, data.get("status"), "ok")


# ===================================================================
# Main
# ===================================================================


async def main() -> bool:
    proc = await start_server()
    runner = R()
    try:
        print("  Lifespan Test")
        print("=" * 50)

        tests = [
            p7t19_app_context_lifespan,
        ]
        for fn in tests:
            try:
                await fn(runner)
            except Exception as exc:
                runner.fail(fn.__name__, str(exc))

    finally:
        restore_environment()
    success = runner.summary()
    sys.exit(0 if success else 1)
    return success


if __name__ == "__main__":
    asyncio.run(main())
