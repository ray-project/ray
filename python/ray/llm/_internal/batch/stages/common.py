"""Shared utilities for stages."""

from typing import Any, Dict, List, Union

import numpy as np


def truncate_str(s: str, max_length: int, suffix: str = "...[truncated]") -> str:
    """Truncate a string to max_length, appending suffix if truncated."""
    if len(s) <= max_length:
        return s
    return s[:max_length] + suffix


def maybe_convert_ndarray_to_list(
    params: Union[np.ndarray, List[Any], Dict[str, Any]]
) -> Union[List[Any], Dict[str, Any]]:
    """Convert all ndarray to list in the params. This is because Ray Data
    by default converts all lists to ndarrays when passing data around, but
    vLLM expects lists.

    Args:
        params: The parameters to convert.

    Returns:
        The converted parameters.
    """
    if isinstance(params, dict):
        return {k: maybe_convert_ndarray_to_list(v) for k, v in params.items()}
    elif isinstance(params, list):
        return [maybe_convert_ndarray_to_list(v) for v in params]
    elif isinstance(params, np.ndarray):
        return params.tolist()
    return params


def drop_none_values(params: Any) -> Any:
    """Recursively drop dict entries whose value is None.

    When rows in one block carry different nested dicts (for example different JSON
    schemas in ``structured_outputs``), Arrow unifies them into one struct type, and each
    row comes back with the other rows' keys set to None. A JSON schema with
    ``"properties": {"name": {...}, "status": None}`` is invalid, so the structured
    output backend rejects the request. None items inside lists are kept, so values like
    ``"enum": ["a", None]`` are unchanged.

    Args:
        params: The parameters to clean.

    Returns:
        The parameters without None-valued dict entries.
    """
    if isinstance(params, dict):
        return {k: drop_none_values(v) for k, v in params.items() if v is not None}
    if isinstance(params, list):
        return [drop_none_values(v) for v in params]
    return params
