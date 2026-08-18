"""
Server configuration loading and validation.

Extracted from server.py to keep the main module focused on MCP wiring.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any

logger = logging.getLogger("event_server")

# Default configuration values (overridden by config.json when present)
DEFAULTS: dict[str, Any] = {
    "server_name": "MCP Event Server",
    "host": "127.0.0.1",
    "port": 8000,
    "log_level": "INFO",
    "max_request_body_size_mb": 4,
    "data_dir": "data",
    "timeouts": {
        "default_tool_seconds": 30,
        "database_seconds": 10,
        "shutdown_seconds": 10,
    },
    "replay": {
        "default_limit": 50,
        "max_limit": 500,
    },
}


class ConfigError(Exception):
    """Raised when configuration is invalid."""


def load_config(config_path: str) -> dict[str, Any]:
    """
    Load and validate configuration from config.json.

    Returns a merged dict: explicit config values override defaults.
    Missing file is not an error — defaults are used.
    """
    config: dict[str, Any] = {k: dict(v) if isinstance(v, dict) else v for k, v in DEFAULTS.items()}

    if not os.path.isfile(config_path):
        logger.info("config.json not found — using built-in defaults")
        return config

    try:
        with open(config_path, "r", encoding="utf-8") as f:
            user_config = json.load(f)
    except json.JSONDecodeError as exc:
        raise ConfigError(
            "config.json contains invalid JSON: {0}".format(exc)
        ) from exc
    except OSError as exc:
        raise ConfigError(
            "Unable to read config.json: {0}".format(exc)
        ) from exc

    if not isinstance(user_config, dict):
        raise ConfigError("config.json must contain a JSON object at the top level")

    known_keys = set(DEFAULTS.keys()) | {
        "allowed_hosts",
        "allowed_origins",
        "enable_dns_rebinding_protection",
        "sources",
    }

    for key, value in user_config.items():
        if key.startswith("_"):
            continue  # comment keys are ignored silently
        if key not in known_keys:
            logger.warning("Unknown config key '%s' — ignored", key)
            continue
        if isinstance(value, dict) and key in config and isinstance(config[key], dict):
            config[key].update(value)
        else:
            config[key] = value

    return config


def validate_config(config: dict[str, Any]) -> None:
    """
    Validate configuration values. Raises ConfigError with a clear message on failure.
    """
    host = config.get("host")
    if not isinstance(host, str) or not host:
        raise ConfigError("'host' must be a non-empty string")
    if host in ("0.0.0.0", "::", "*"):
        raise ConfigError(
            "Refusing to bind to '{0}': this exposes the server to the network. "
            "Set 'host' to 127.0.0.1 (or a specific LAN IP) in config.json.".format(host)
        )

    port = config.get("port")
    if not isinstance(port, int) or port < 1 or port > 65535:
        raise ConfigError("'port' must be an integer between 1 and 65535")

    valid_levels = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")
    log_level = config.get("log_level", "INFO").upper()
    if log_level not in valid_levels:
        raise ConfigError(
            "'log_level' must be one of: {0}".format(", ".join(valid_levels))
        )
    config["log_level"] = log_level

    max_body_mb = config.get("max_request_body_size_mb")
    if not isinstance(max_body_mb, (int, float)) or max_body_mb < 0.1 or max_body_mb > 100:
        raise ConfigError("'max_request_body_size_mb' must be a number between 0.1 and 100")
    config["max_request_body_size"] = int(max_body_mb * 1024 * 1024)

    data_dir = config.get("data_dir", "data")
    if not isinstance(data_dir, str) or not data_dir:
        raise ConfigError("'data_dir' must be a non-empty string")

    # Validate timeouts
    timeouts = config.get("timeouts", {})
    for tk in ("default_tool_seconds", "database_seconds", "shutdown_seconds"):
        tv = timeouts.get(tk, DEFAULTS["timeouts"][tk])
        if not isinstance(tv, (int, float)) or tv <= 0:
            raise ConfigError(f"'timeouts.{tk}' must be a positive number")
        timeouts[tk] = tv
    config["timeouts"] = timeouts

    # Validate replay limits
    replay = config.get("replay", {})
    for rk in ("default_limit", "max_limit"):
        rv = replay.get(rk, DEFAULTS["replay"][rk])
        if not isinstance(rv, int) or rv < 1:
            raise ConfigError(f"'replay.{rk}' must be a positive integer")
    if replay.get("default_limit", 50) > replay.get("max_limit", 500):
        raise ConfigError("'replay.default_limit' must not exceed 'replay.max_limit'")
    config["replay"] = replay

    if "enable_dns_rebinding_protection" in config:
        val = config["enable_dns_rebinding_protection"]
        if not isinstance(val, bool):
            raise ConfigError("'enable_dns_rebinding_protection' must be a boolean")

    if "allowed_hosts" in config:
        val = config["allowed_hosts"]
        if not isinstance(val, list) or not all(isinstance(h, str) for h in val):
            raise ConfigError("'allowed_hosts' must be a list of strings")

    if "allowed_origins" in config:
        val = config["allowed_origins"]
        if not isinstance(val, list) or not all(isinstance(o, str) for o in val):
            raise ConfigError("'allowed_origins' must be a list of strings")
