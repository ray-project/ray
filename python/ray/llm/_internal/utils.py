"""Utility functions for the LLM module."""
import importlib
import logging
from types import ModuleType
from typing import Optional

logger = logging.getLogger(__name__)


def try_import(
    name: str, warning: bool = False, error: bool = False
) -> Optional[ModuleType]:
    """Try importing the module and returns the module (or None).

    Args:
        name: The name of the module to import.
        warning: Whether to log a warning if the module cannot be imported. The
            priority is higher than error.
        error: Whether to raise an error if the module cannot be imported.

    Returns:
        The module, or None if it cannot be imported.

    Raises:
        ImportError: If error=True and the module is not installed.
    """
    try:
        return importlib.import_module(name)
    except ImportError:
        if warning:
            logger.warning("Could not import %s", name)
        elif error:
            raise ImportError(f"Could not import {name}")
    return None
