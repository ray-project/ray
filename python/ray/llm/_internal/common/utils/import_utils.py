"""Utility functions for importing modules in the LLM module."""
import importlib
import logging
from types import ModuleType
from typing import Any, NoReturn, Optional, Type

logger = logging.getLogger(__name__)


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
        else:
            logger.warning("Could not import %s", name)
    return None


def raise_llm_engine_import_error(
    vllm_error: ImportError,
    sglang_error: ImportError,
) -> NoReturn:
    """Raise a descriptive ImportError when both vLLM and SGLang fail to import.

    Distinguishes between a package not being installed (ModuleNotFoundError
    whose .name matches the top-level package) and a broken installation
    (any other ImportError, e.g. a missing .so or a missing transitive dep).

    Args:
        vllm_error: The ImportError raised when importing vLLM.
        sglang_error: The ImportError raised when importing SGLang.
    """
    vllm_not_installed = (
        isinstance(vllm_error, ModuleNotFoundError) and vllm_error.name == "vllm"
    )
    sglang_not_installed = (
        isinstance(sglang_error, ModuleNotFoundError) and sglang_error.name == "sglang"
    )

    if vllm_not_installed and sglang_not_installed:
        raise ImportError(
            "Neither vLLM nor SGLang is installed. At least one is required "
            "for Ray Serve LLM protocol models. Install with: "
            "`pip install ray[llm]` or `pip install sglang[all,ray]`"
        )

    messages = []
    if not vllm_not_installed:
        messages.append(
            "vLLM is installed but failed to import. This may indicate a "
            "CUDA version mismatch or a missing vLLM dependency. "
            f"Original error: {vllm_error}"
        )
    if not sglang_not_installed:
        messages.append(
            "SGLang is installed but failed to import. This may indicate a "
            "missing SGLang dependency. "
            f"Original error: {sglang_error}"
        )
    # Chain to the error that is actually relevant: vLLM's if it is broken,
    # otherwise sglang's (i.e. vLLM was simply not installed).
    cause = vllm_error if not vllm_not_installed else sglang_error
    raise ImportError("\n".join(messages)) from cause


def load_class(path: str) -> Type[Any]:
    """Load class from string path."""
    if ":" in path:
        module_path, class_name = path.rsplit(":", 1)
    else:
        module_path, class_name = path.rsplit(".", 1)

    module = try_import(module_path, error=True)
    callback_class = getattr(module, class_name)

    return callback_class
