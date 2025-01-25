"""Utility functions for the LLM module."""
import importlib
from types import ModuleType
from typing import Optional


def try_import(name: str, error: bool = False) -> Optional[ModuleType]:
    """Try importing the module and returns the module (or None).

    Args:
        name: The name of the module to import.
        error: Whether to raise an error if the module cannot be imported.

    Returns:
        The module, or None if it cannot be imported.

    Raises:
        ImportError: If error=True and the module is not installed.
    """
    try:
        return importlib.import_module(name)
    except ImportError:
        if error:
            raise ImportError(f"Could not import {name}")
    return None
