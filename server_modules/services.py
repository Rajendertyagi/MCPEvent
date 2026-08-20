"""
Application services container for dependency passing.

Holds references to process-wide services that tools and resources need.
No request-scoped state lives here.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class Services:
    """Process-wide service dependencies for tool/resource registration."""

    store: Any           # EventStore
    subscription_bus: Any  # InMemorySubscriptionBus
    bg_task_manager: Any  # BackgroundTaskManager
    source_manager: Any   # SourceManager
    timeouts: dict[str, Any]
    replay_cfg: dict[str, Any]
    metrics: Any          # RuntimeMetrics
