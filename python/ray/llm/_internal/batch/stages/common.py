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


def to_token_id_list(value: Any, name: str) -> List[int]:
    """Validate that `value` is a list of integer token IDs and return it as one.

    Args:
        value: A list or ndarray of token IDs.
        name: The column name, used in error messages.

    Returns:
        The token IDs as a list of Python ints.
    """
    value = maybe_convert_ndarray_to_list(value)
    if not isinstance(value, list) or not all(
        isinstance(token_id, (int, np.integer)) and not isinstance(token_id, bool)
        for token_id in value
    ):
        raise ValueError(f"'{name}' must be a list of integer token IDs.")
    return [int(token_id) for token_id in value]
