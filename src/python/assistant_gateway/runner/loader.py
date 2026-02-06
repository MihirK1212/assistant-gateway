"""
Dynamic module loading utilities for the gateway runner.

Provides functions to dynamically import modules and retrieve attributes
(like config or app) at runtime based on module path strings.
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path
from typing import Any, Union

from assistant_gateway.chat_orchestrator.core.config import GatewayConfig


def load_attribute(module_path: str) -> Any:
    """
    Load an attribute from a module using 'module.path:attribute' notation.

    Args:
        module_path: String in format 'module.path:attribute_name'
                    e.g., 'myapp.config:build_gateway_config'
                    or 'myapp.config:config'

    Returns:
        The loaded attribute (function, variable, class, etc.)

    Raises:
        ValueError: If the module_path format is invalid
        ImportError: If the module cannot be imported
        AttributeError: If the attribute doesn't exist in the module
    """
    if ":" not in module_path:
        raise ValueError(
            f"Invalid module path format: '{module_path}'. "
            "Expected format: 'module.path:attribute_name'"
        )

    module_name, attr_name = module_path.rsplit(":", 1)

    try:
        module = importlib.import_module(module_name)
    except ImportError as e:
        raise ImportError(f"Could not import module '{module_name}': {e}") from e

    try:
        return getattr(module, attr_name)
    except AttributeError as e:
        raise AttributeError(
            f"Module '{module_name}' has no attribute '{attr_name}'"
        ) from e


def load_config(
    config_path: str,
) -> GatewayConfig:
    """
    Load a GatewayConfig from a module path.

    The config_path can point to either:
    - A GatewayConfig instance: 'myapp.config:config'
    - A callable that returns GatewayConfig: 'myapp.config:build_gateway_config'

    Args:
        config_path: Module path in format 'module.path:attribute_name'

    Returns:
        GatewayConfig instance

    Raises:
        TypeError: If the loaded attribute is neither a GatewayConfig nor callable
    """
    attr = load_attribute(config_path)

    if isinstance(attr, GatewayConfig):
        return attr
    elif callable(attr):
        result = attr()
        if not isinstance(result, GatewayConfig):
            raise TypeError(
                f"Callable '{config_path}' returned {type(result).__name__}, "
                "expected GatewayConfig"
            )
        return result
    else:
        raise TypeError(
            f"'{config_path}' must be a GatewayConfig instance or a callable "
            f"that returns GatewayConfig, got {type(attr).__name__}"
        )


def add_path_to_sys(path: Union[str, Path]) -> None:
    """
    Add a path to sys.path if it's not already there.

    This is useful when loading modules from arbitrary locations
    that may not be in the default Python path.

    Args:
        path: Directory path to add to sys.path
    """
    path_str = str(Path(path).resolve())
    if path_str not in sys.path:
        sys.path.insert(0, path_str)
