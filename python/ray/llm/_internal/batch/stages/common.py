"""
Shared utilities for stages.
"""

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

import numpy as np

if TYPE_CHECKING:
    import torch


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


def convert_to_embed_override_tensors(
    embeds: Any,
) -> Optional[List["torch.Tensor"]]:
    """Convert embedding overrides into the per-position tensors SGLang expects.

    Args:
        embeds: The overrides for one sequence: `None`, or an array, list, or
            tensor shaped `[num_overrides, hidden_dim]` or `[hidden_dim]`.

    Returns:
        A list of `[1, hidden_dim]` tensors, or `None` if there are no overrides.
    """
    if embeds is None:
        return None

    tensor = _to_stacked_tensor(embeds)
    if tensor is None or tensor.numel() == 0:
        return None

    if tensor.ndim == 1:
        tensor = tensor.unsqueeze(0)
    elif tensor.ndim != 2:
        raise ValueError(
            "Embedding overrides must be shaped [num_overrides, hidden_dim], "
            f"got {tuple(tensor.shape)}."
        )
    return [row.unsqueeze(0) for row in tensor]


def _to_stacked_tensor(embeds: Any) -> Optional["torch.Tensor"]:
    """Stack embedding overrides into one tensor.

    Torch tensors are stacked with `torch.stack` because numpy has no bfloat16
    dtype; everything else goes through `np.asarray`.
    """
    import torch

    if isinstance(embeds, torch.Tensor):
        return embeds

    if isinstance(embeds, (list, tuple, np.ndarray)):
        entries = list(embeds)
        if entries and all(isinstance(entry, torch.Tensor) for entry in entries):
            return _stack_tensor_entries(entries)

    array = np.asarray(embeds)
    if array.size == 0:
        return None
    if array.dtype == object:
        # Ray Data stores a nested list column as an object ndarray of ndarrays.
        return _stack_tensor_entries(
            [torch.as_tensor(np.asarray(entry)) for entry in array]
        )
    return torch.as_tensor(array)


def _stack_tensor_entries(entries: List["torch.Tensor"]) -> "torch.Tensor":
    """`torch.stack` that raises `ValueError` on mismatched shapes."""
    import torch

    try:
        return torch.stack(entries)
    except RuntimeError as e:
        shapes = [tuple(entry.shape) for entry in entries]
        raise ValueError(
            f"Embedding overrides must all have the same shape, got {shapes}."
        ) from e
