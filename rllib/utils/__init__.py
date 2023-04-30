import contextlib
from functools import partial

from ray.rllib.utils.annotations import override, PublicAPI, DeveloperAPI
from ray.rllib.utils.deprecation import deprecation_warning
from ray.rllib.utils.filter import Filter
from ray.rllib.utils.filter_manager import FilterManager
from ray.rllib.utils.framework import (
    try_import_jax,
    try_import_tf,
    try_import_tfp,
    try_import_torch,
)
from ray.rllib.utils.numpy import (
    sigmoid,
    softmax,
    relu,
    one_hot,
    fc,
    lstm,
    SMALL_NUMBER,
    LARGE_INTEGER,
    MIN_LOG_NN_OUTPUT,
    MAX_LOG_NN_OUTPUT,
)
from ray.rllib.utils.pre_checks.env import check_env
from ray.rllib.utils.schedules import (
    LinearSchedule,
    PiecewiseSchedule,
    PolynomialSchedule,
    ExponentialSchedule,
    ConstantSchedule,
)
from ray.rllib.utils.test_utils import (
    check,
    check_compute_single_action,
    check_train_results,
    framework_iterator,
)
from ray.tune.utils import merge_dicts, deep_update


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
def force_list(elements=None, to_tuple=False):
    """
    Makes sure `elements` is returned as a list, whether `elements` is a single
    item, already a list, or a tuple.

    Args:
        elements (Optional[any]): The inputs as single item, list, or tuple to
            be converted into a list/tuple. If None, returns empty list/tuple.
        to_tuple: Whether to use tuple (instead of list).

    Returns:
        Union[list,tuple]: All given elements in a list/tuple depending on
            `to_tuple`'s value. If elements is None,
            returns an empty list/tuple.
    """
    ctor = list
    if to_tuple is True:
        ctor = tuple
    return (
        ctor()
        if elements is None
        else ctor(elements)
        if type(elements) in [list, set, tuple]
        else ctor([elements])
    )


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
    "check_env",
    "check_compute_single_action",
    "check_train_results",
    "deep_update",
    "deprecation_warning",
    "fc",
    "force_list",
    "force_tuple",
    "framework_iterator",
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
