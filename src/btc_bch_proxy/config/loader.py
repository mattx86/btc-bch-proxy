"""Configuration loading and validation utilities."""

from __future__ import annotations

from pathlib import Path
from typing import Union

import yaml
from pydantic import ValidationError

from btc_bch_proxy.config.models import Config


class ConfigError(Exception):
    """Configuration error."""

    pass


def load_config(path: Union[str, Path]) -> Config:
    """
    Load and validate configuration from a YAML file.

    Args:
        path: Path to the configuration file.

    Returns:
        Validated Config object.

    Raises:
        ConfigError: If the file cannot be read or the configuration is invalid.
    """
    path = Path(path)

    if not path.exists():
        raise ConfigError(f"Configuration file not found: {path}")

    if not path.is_file():
        raise ConfigError(f"Configuration path is not a file: {path}")

    try:
        with open(path, "r", encoding="utf-8") as f:
            raw_config = yaml.safe_load(f)
    except yaml.YAMLError as e:
        raise ConfigError(f"Invalid YAML in configuration file: {e}") from e
    except OSError as e:
        raise ConfigError(f"Cannot read configuration file: {e}") from e

    if raw_config is None:
        raise ConfigError("Configuration file is empty")

    if not isinstance(raw_config, dict):
        raise ConfigError(
            f"Configuration file must contain a YAML mapping (dict), "
            f"got {type(raw_config).__name__}"
        )

    try:
        return Config.model_validate(raw_config)
    except ValidationError as e:
        errors = []
        for error in e.errors():
            loc = ".".join(str(x) for x in error["loc"])
            msg = error["msg"]
            errors.append(f"  - {loc}: {msg}")
        raise ConfigError(f"Configuration validation failed:\n" + "\n".join(errors)) from e


def validate_config(path: Union[str, Path]) -> tuple[bool, str]:
    """
    Validate a configuration file without loading it fully.

    Args:
        path: Path to the configuration file.

    Returns:
        Tuple of (is_valid, message).
    """
    try:
        config = load_config(path)
        server_count = len(config.servers)
        schedule_count = len(config.schedule)
        return (
            True,
            f"Configuration valid: {server_count} servers, {schedule_count} schedule entries",
        )
    except ConfigError as e:
        return False, str(e)
