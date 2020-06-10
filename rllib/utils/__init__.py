from functools import partial

from ray.rllib.utils.annotations import override, PublicAPI, DeveloperAPI
from ray.rllib.utils.framework import try_import_tf, try_import_tfp, \
    try_import_torch
from ray.rllib.utils.deprecation import deprecation_warning, renamed_agent, \
    renamed_class, renamed_function
from ray.rllib.utils.filter_manager import FilterManager
from ray.rllib.utils.filter import Filter
from ray.rllib.utils.numpy import sigmoid, softmax, relu, one_hot, fc, lstm, \
    SMALL_NUMBER, LARGE_INTEGER, MIN_LOG_NN_OUTPUT, MAX_LOG_NN_OUTPUT
from ray.rllib.utils.policy_client import PolicyClient
from ray.rllib.utils.policy_server import PolicyServer
from ray.rllib.utils.schedules import LinearSchedule, PiecewiseSchedule, \
    PolynomialSchedule, ExponentialSchedule, ConstantSchedule
from ray.rllib.utils.test_utils import check, framework_iterator, \
    check_compute_action
from ray.tune.utils import merge_dicts, deep_update


def add_mixins(base, mixins):
    """Returns a new class with mixins applied in priority order."""

    mixins = list(mixins or [])

    while mixins:

        class new_base(mixins.pop(), base):
            pass

        base = new_base

    return base


def force_list(elements=None, to_tuple=False):
    """
    Makes sure `elements` is returned as a list, whether `elements` is a single
    item, already a list, or a tuple.

    Args:
        elements (Optional[any]): The inputs as single item, list, or tuple to
            be converted into a list/tuple. If None, returns empty list/tuple.
        to_tuple (bool): Whether to use tuple (instead of list).

    Returns:
        Union[list,tuple]: All given elements in a list/tuple depending on
            `to_tuple`'s value. If elements is None,
            returns an empty list/tuple.
    """
    ctor = list
    if to_tuple is True:
        ctor = tuple
    return ctor() if elements is None else ctor(elements) \
        if type(elements) in [list, tuple] else ctor([elements])


force_tuple = partial(force_list, to_tuple=True)


# TODO(sven): remove at some point.
def try_import_tree():
    try:
        import tree
        return tree
    except (ImportError, ModuleNotFoundError):
        raise ModuleNotFoundError(
            "`dm-tree` is not installed! Run `pip install dm-tree`.")


__all__ = [
    "add_mixins",
    "check",
    "check_compute_action",
    "deprecation_warning",
    "fc",
    "force_list",
    "force_tuple",
    "framework_iterator",
    "lstm",
    "one_hot",
    "relu",
    "sigmoid",
    "softmax",
    "deep_update",
    "merge_dicts",
    "override",
    "renamed_function",
    "renamed_agent",
    "renamed_class",
    "try_import_tf",
    "try_import_tfp",
    "try_import_torch",
    "try_import_tree",
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
    "PolicyClient",
    "PolicyServer",
    "PolynomialSchedule",
    "PublicAPI",
    "SMALL_NUMBER",
]
