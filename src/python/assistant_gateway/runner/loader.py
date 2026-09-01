from __future__ import annotations

import importlib
from typing import Any


def load_attribute(module_path: str) -> Any:
    """
    Load an attribute from a module using 'module.path:attribute' notation.

    eg: 'myapp.config:build_gateway_config' or 'myapp.config:config'
    """
    if ":" not in module_path:
        raise ValueError(f"Invalid module path format: '{module_path}'. Expected format: 'module.path:attribute_name'")

    module_name, attr_name = module_path.rsplit(":", 1)

    try:
        module = importlib.import_module(module_name)
    except ImportError as e:
        raise ImportError(f"Could not import module '{module_name}': {e}") from e

    try:
        return getattr(module, attr_name)
    except AttributeError as e:
        raise AttributeError(f"Module '{module_name}' has no attribute '{attr_name}'") from e
