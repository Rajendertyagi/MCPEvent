#!/usr/bin/env python3
"""
Bounded async-wait helpers for the test harness.

These replace open-ended ``while True: await sleep`` polling loops that can
hang a test run for hours. Every waiter has a hard ``timeout`` (seconds) and
yields to the event loop between checks (no busy spin). On expiry it raises
``TimeoutError`` with a descriptive message so the failure is obvious and the
per-file runner can attribute it.

Predicates/getters may be plain callables or coroutine functions; exceptions
raised inside them are treated as "not yet satisfied" (never fatal) so a
transient error during warm-up does not abort the wait.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any, Awaitable, Callable

DEFAULT_TIMEOUT = 15.0
DEFAULT_INTERVAL = 0.1

# A predicate/getter: sync callable, coroutine function, or an awaitable object.
_Probe = Callable[[], Any] | Callable[[], Awaitable[Any]]


def _is_awaitable_fn(fn: _Probe) -> bool:
    return asyncio.iscoroutinefunction(fn)


async def _eval(probe: _Probe) -> Any:
    """Call a sync or async probe, unwrapping one level of coroutine."""
    result = probe() if not _is_awaitable_fn(probe) else probe()
    # A sync callable may itself return a coroutine in some wrappers; unwrap once.
    while asyncio.iscoroutine(result):
        result = await result
    return result


async def wait_until(
    predicate: _Probe,
    timeout: float = DEFAULT_TIMEOUT,
    interval: float = DEFAULT_INTERVAL,
    description: str = "condition",
) -> bool:
    """Poll ``predicate`` until it returns truthy (or a coroutine that resolves truthy).

    Returns ``True`` if satisfied. Raises ``TimeoutError`` after ``timeout`` seconds.
    Exceptions inside the probe are swallowed and treated as unsatisfied.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            ok = bool(await _eval(predicate))
        except Exception:
            ok = False
        if ok:
            return True
        await asyncio.sleep(interval)
    raise TimeoutError(f"wait_until timed out after {timeout:.1f}s: {description}")


async def wait_for_value(
    getter: _Probe,
    expected: Any,
    timeout: float = DEFAULT_TIMEOUT,
    interval: float = DEFAULT_INTERVAL,
    description: str = "value",
    eq: Callable[[Any, Any], bool] = lambda a, b: a == b,
) -> Any:
    """Poll ``getter`` until ``eq(value, expected)`` is true. Returns the last value.

    Raises ``TimeoutError`` (including the last seen value) on expiry.
    """
    deadline = time.monotonic() + timeout
    last: Any = None
    while time.monotonic() < deadline:
        try:
            last = await _eval(getter)
        except Exception:
            last = None
        if eq(last, expected):
            return last
        await asyncio.sleep(interval)
    raise TimeoutError(
        f"wait_for_value timed out after {timeout:.1f}s: {description} "
        f"(last={last!r}, expected={expected!r})"
    )


async def wait_for_event(
    probe: _Probe,
    timeout: float = DEFAULT_TIMEOUT,
    interval: float = DEFAULT_INTERVAL,
    description: str = "event",
) -> Any:
    """Poll ``probe`` until it returns a truthy / non-empty result. Returns that result.

    Useful for "an event has appeared" checks (e.g. live notification received,
    source status reached a target state, seen-items count grew). Raises
    ``TimeoutError`` on expiry.
    """
    deadline = time.monotonic() + timeout
    last: Any = None
    while time.monotonic() < deadline:
        try:
            last = await _eval(probe)
        except Exception:
            last = None
        if last:
            return last
        await asyncio.sleep(interval)
    raise TimeoutError(f"wait_for_event timed out after {timeout:.1f}s: {description}")


class wait_for:  # noqa: N801  (intentional lowercase to read like a keyword)
    """Context-manager / coroutine wrapper that bounds any awaitable.

    Usage::

        result = await wait_for(some_coro(), timeout=30)

    Raises ``asyncio.TimeoutError`` if the inner coroutine does not finish in time.
    Unlike a bare ``asyncio.wait_for``, this documents intent at the call site and
    keeps a single tunable default.
    """

    def __init__(self, coro: Awaitable[Any], timeout: float = DEFAULT_TIMEOUT) -> None:
        self._coro = coro
        self._timeout = timeout

    def __await__(self):
        return asyncio.wait_for(self._coro, timeout=self._timeout).__await__()
