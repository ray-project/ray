import contextlib
from collections import deque
from functools import partial
from typing import Any, Dict, List, Optional, Tuple, Union

import tree

from ray._common.deprecation import deprecation_warning
from ray.rllib.utils.annotations import DeveloperAPI, PublicAPI, override
from ray.rllib.utils.filter import Filter
from ray.rllib.utils.filter_manager import FilterManager
from ray.rllib.utils.framework import (
    try_import_jax,
    try_import_tf,
    try_import_tfp,
    try_import_torch,
)
from ray.rllib.utils.numpy import (
    LARGE_INTEGER,
    MAX_LOG_NN_OUTPUT,
    MIN_LOG_NN_OUTPUT,
    SMALL_NUMBER,
    fc,
    lstm,
    one_hot,
    relu,
    sigmoid,
    softmax,
)
from ray.rllib.utils.schedules import (
    ConstantSchedule,
    ExponentialSchedule,
    LinearSchedule,
    PiecewiseSchedule,
    PolynomialSchedule,
)
from ray.rllib.utils.test_utils import (
    check,
    check_compute_single_action,
    check_train_results,
)
from ray.tune.utils import deep_update, merge_dicts


@DeveloperAPI
def add_mixins(base, mixins, reversed=False):
    """Returns a new class with mixins applied in priority order."""

    mixins = list(mixins or [])

    while mixins:
        if reversed:

            class new_base(base, mixins.pop()):
                pass

        else:

            class new_base(mixins.pop(), base):
                pass

        base = new_base

    return base


@DeveloperAPI
def force_list(
    elements: Optional[Any] = None, to_tuple: bool = False
) -> Union[List, Tuple]:
    """
    Makes sure `elements` is returned as a list, whether `elements` is a single
    item, already a list, or a tuple.

    Args:
        elements: The inputs as a single item, a list/tuple/deque of items, or None,
            to be converted to a list/tuple. If None, returns empty list/tuple.
        to_tuple: Whether to use tuple (instead of list).

    Returns:
        The provided item in a list of size 1, or the provided items as a
        list. If `elements` is None, returns an empty list. If `to_tuple` is True,
        returns a tuple instead of a list.
    """
    ctor = list
    if to_tuple is True:
        ctor = tuple
    return (
        ctor()
        if elements is None
        else ctor(elements)
        if type(elements) in [list, set, tuple, deque]
        else ctor([elements])
    )


@DeveloperAPI
def flatten_dict(nested: Dict[str, Any], sep="/", env_steps=0) -> Dict[str, Any]:
    """
    Flattens a nested dict into a flat dict with joined keys.

    Note, this is used for better serialization of nested dictionaries
    in `OfflinePreLearner.__call__` when called inside
    `ray.data.Dataset.map_batches`.

    Note, this is used to return a `Dict[str, numpy.ndarray] from the
    `__call__` method which is expected by Ray Data.

    Args:
        nested: A nested dictionary.
        sep: Separator to use when joining keys.

    Returns:
        A flat dictionary where each key is a path of keys in the nested dict.
    """
    flat = {}
    # `dm_tree.flatten_with_path`` returns a list of `(path, leaf)` tuples.
    for path, leaf in tree.flatten_with_path(nested):
        # Create a single string key from the path.
        key = sep.join(map(str, path))
        flat[key] = leaf

    return flat


@DeveloperAPI
def unflatten_dict(flat: Dict[str, Any], sep="/") -> Dict[str, Any]:
    """
    Reconstructs a nested dict from a flat dict with joined keys.

    Note, this is used for better deserialization ofr nested dictionaries
    in `Learner.update' calls in which a `ray.data.DataIterator` is used.

    Args:
        flat: A flat dictionary with keys that are paths joined by `sep`.
        sep: The separator used in the flat dictionary keys.

    Returns:
        A nested dictionary.
    """
    nested = {}
    for compound_key, value in flat.items():
        # Split all keys by the separator.
        keys = compound_key.split(sep)
        current = nested
        # Nest by the separated keys.
        for key in keys[:-1]:
            if key not in current:
                current[key] = {}
            current = current[key]
        current[keys[-1]] = value

    return nested


@DeveloperAPI
class NullContextManager(contextlib.AbstractContextManager):
    """No-op context manager"""

    def __init__(self):
        pass

    def __enter__(self):
        pass

    def __exit__(self, *args):
        pass


force_tuple = partial(force_list, to_tuple=True)

__all__ = [
    "add_mixins",
    "check",
    "check_compute_single_action",
    "check_train_results",
    "deep_update",
    "deprecation_warning",
    "fc",
    "force_list",
    "force_tuple",
    "flatten_dict",
    "unflatten_dict",
    "lstm",
    "merge_dicts",
    "one_hot",
    "override",
    "relu",
    "sigmoid",
    "softmax",
    "try_import_jax",
    "try_import_tf",
    "try_import_tfp",
    "try_import_torch",
    "ConstantSchedule",
    "DeveloperAPI",
    "ExponentialSchedule",
    "Filter",
    "FilterManager",
    "LARGE_INTEGER",
    "LinearSchedule",
    "MAX_LOG_NN_OUTPUT",
    "MIN_LOG_NN_OUTPUT",
    "PiecewiseSchedule",
    "PolynomialSchedule",
    "PublicAPI",
    "SMALL_NUMBER",
]
