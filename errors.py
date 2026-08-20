"""
Structured error types for the MCP Event Server.

All errors inherit from MCPEventServerError so tool handlers can catch them
uniformly. The SDK wraps non-MCPError exceptions as ToolError (isError=True);
MCPError subclasses are re-raised as protocol-level JSON-RPC errors.
"""

from __future__ import annotations


class MCPEventServerError(Exception):
    """Base for all application-level errors in this server."""


class ValidationError(MCPEventServerError):
    """Invalid input arguments."""


class ConsumerNotFoundError(ValidationError):
    """The referenced consumer does not exist."""

    def __init__(self, consumer_id: str) -> None:
        self.consumer_id = consumer_id
        super().__init__(f"consumer not found: {consumer_id}")


class EventNotFoundError(ValidationError):
    """The referenced persistent event does not exist."""

    def __init__(self, event_id: str) -> None:
        self.event_id = event_id
        super().__init__(f"event not found: {event_id}")


class AlertNotFoundError(ValidationError):
    """The referenced alert definition does not exist or is not owned by the consumer."""

    def __init__(self, alert_id: str) -> None:
        self.alert_id = alert_id
        super().__init__(f"alert not found: {alert_id}")


class EventNotRelevantError(MCPEventServerError):
    """The event exists but is not relevant to the consumer."""

    def __init__(self, event_id: str, consumer_id: str) -> None:
        self.event_id = event_id
        self.consumer_id = consumer_id
        super().__init__(
            f"event {event_id} is not relevant to consumer {consumer_id}"
        )


class StorageError(MCPEventServerError):
    """Database operation failed."""

    def __init__(self, message: str, original: Exception | None = None) -> None:
        super().__init__(message)
        self.__cause__ = original


class OperationTimeoutError(MCPEventServerError):
    """Operation exceeded its configured timeout."""

    def __init__(self, operation: str, timeout_seconds: float) -> None:
        self.operation = operation
        self.timeout_seconds = timeout_seconds
        super().__init__(
            f"operation '{operation}' timed out after {timeout_seconds}s"
        )
