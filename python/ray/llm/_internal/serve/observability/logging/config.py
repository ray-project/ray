"""
Centralized logging configuration utilities for Ray LLM Serve.

This module provides utilities to control logging verbosity across different
components of the Ray LLM system, allowing users to reduce log noise while
maintaining important error and warning messages.
"""

import logging
from typing import Optional

from ray.llm._internal.serve.configs.constants import (
    RAYLLM_LOG_ENGINE_OPERATIONS,
    RAYLLM_LOG_LEVEL,
    RAYLLM_LOG_MODEL_OPERATIONS,
    RAYLLM_LOG_PREFIX_TREE_DEBUG,
    RAYLLM_LOG_REQUEST_FAILURES,
    RAYLLM_LOG_REQUEST_LIFECYCLE,
    RAYLLM_LOG_STREAMING_DETAILS,
)


def get_effective_log_level() -> str:
    """Get the effective log level for Ray LLM components."""
    return RAYLLM_LOG_LEVEL


def should_log_request_lifecycle() -> bool:
    """Check if request lifecycle events should be logged."""
    return RAYLLM_LOG_REQUEST_LIFECYCLE


def should_log_request_failures() -> bool:
    """Check if request failures should be logged."""
    return RAYLLM_LOG_REQUEST_FAILURES


def should_log_engine_operations() -> bool:
    """Check if engine operations should be logged."""
    return RAYLLM_LOG_ENGINE_OPERATIONS


def should_log_model_operations() -> bool:
    """Check if model operations should be logged."""
    return RAYLLM_LOG_MODEL_OPERATIONS


def should_log_streaming_details() -> bool:
    """Check if streaming details should be logged."""
    return RAYLLM_LOG_STREAMING_DETAILS


def should_log_prefix_tree_debug() -> bool:
    """Check if prefix tree debug operations should be logged."""
    return RAYLLM_LOG_PREFIX_TREE_DEBUG


def conditional_log(
    logger: logging.Logger,
    level: str,
    message: str,
    condition_func: Optional[callable] = None,
    *args,
    **kwargs
) -> None:
    """
    Log a message conditionally based on configuration.

    Args:
        logger: The logger instance to use
        level: Log level ('info', 'debug', 'warning', etc.)
        message: Message to log
        condition_func: Optional function that returns True if logging should occur
        *args: Additional positional arguments passed to the logging method
        **kwargs: Additional keyword arguments passed to the logging method
    """
    if condition_func is None or condition_func():
        log_method = getattr(logger, level.lower())
        log_method(message, *args, **kwargs)


def configure_logger_level(
    logger: logging.Logger, level_override: Optional[str] = None
) -> None:
    """
    Configure a logger's level based on the global configuration.

    Args:
        logger: Logger to configure
        level_override: Optional level override, defaults to global configuration
    """
    level_str = level_override or get_effective_log_level()
    try:
        level = getattr(logging, level_str.upper())
        logger.setLevel(level)
    except AttributeError:
        # Fallback to INFO if invalid level specified
        logger.setLevel(logging.INFO)
