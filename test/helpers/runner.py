#!/usr/bin/env python3
"""
Shared test runner — minimal pass/fail tracker used by all feature test files.
"""

from __future__ import annotations

from typing import Any


class R:
    """Minimal test runner that tracks pass/fail counts."""

    def __init__(self) -> None:
        self.passed = 0
        self.failed = 0
        self.total = 0
        self.failures: list[str] = []

    def ok(self, name: str) -> None:
        self.total += 1
        self.passed += 1
        print(f"  PASS  {name}")

    def fail(self, name: str, msg: str) -> None:
        self.total += 1
        self.failed += 1
        self.failures.append(f"{name}: {msg}")
        print(f"  FAIL  {name} \u2014 {msg}")

    def assert_eq(self, name: str, actual: Any, expected: Any) -> bool:
        if actual == expected:
            self.ok(name)
            return True
        self.fail(name, f"expected {expected!r}, got {actual!r}")
        return False

    def assert_true(self, name: str, cond: bool, msg: str = "") -> bool:
        if cond:
            self.ok(name)
            return True
        self.fail(name, msg or "assertion failed")
        return False

    def assert_false(self, name: str, cond: bool, msg: str = "") -> bool:
        if not cond:
            self.ok(name)
            return True
        self.fail(name, msg or "expected falsy")
        return False

    def assert_in(self, name: str, needle: Any, haystack: Any) -> bool:
        if needle in haystack:
            self.ok(name)
            return True
        self.fail(name, f"{needle!r} not in {haystack!r}")
        return False

    def assert_not_in(self, name: str, needle: Any, haystack: Any) -> bool:
        if needle not in haystack:
            self.ok(name)
            return True
        self.fail(name, f"{needle!r} must not be in {haystack!r}")
        return False

    def assert_le(self, name: str, actual: Any, expected: Any) -> bool:
        if actual <= expected:
            self.ok(name)
            return True
        self.fail(name, f"expected <= {expected!r}, got {actual!r}")
        return False

    def assert_ge(self, name: str, actual: Any, expected: Any) -> bool:
        if actual >= expected:
            self.ok(name)
            return True
        self.fail(name, f"expected >= {expected!r}, got {actual!r}")
        return False

    def summary(self) -> bool:
        print()
        print("=" * 50)
        print(f"  Results: {self.passed} passed, {self.failed} failed, {self.total} total")
        print("=" * 50)
        if self.failures:
            print()
            print("Failures:")
            for f in self.failures:
                print(f"  - {f}")
        print()
        return self.failed == 0
