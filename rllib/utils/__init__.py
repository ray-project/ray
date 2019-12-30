from ray.rllib.utils.annotations import override, PublicAPI, DeveloperAPI
from ray.rllib.utils.framework import try_import_tf, try_import_tfp, \
    try_import_torch
from ray.rllib.utils.deprecation import deprecation_warning, renamed_agent, \
    renamed_class, renamed_function
from ray.rllib.utils.filter_manager import FilterManager
from ray.rllib.utils.filter import Filter
from ray.rllib.utils.numpy import sigmoid, softmax, relu, one_hot, fc, lstm, \
    SMALL_NUMBER, LARGE_INTEGER
from ray.rllib.utils.policy_client import PolicyClient
from ray.rllib.utils.policy_server import PolicyServer
from ray.rllib.utils.test_utils import check
from ray.tune.util import merge_dicts, deep_update


def add_mixins(base, mixins):
    """Returns a new class with mixins applied in priority order."""

    mixins = list(mixins or [])

    while mixins:

        class new_base(mixins.pop(), base):
            pass

        base = new_base

    return base


__all__ = [
    "add_mixins",
    "check",
    "deprecation_warning",
    "fc",
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
    "DeveloperAPI",
    "Filter",
    "FilterManager",
    "LARGE_INTEGER",
    "PolicyClient",
    "PolicyServer",
    "PublicAPI",
    "SMALL_NUMBER",
]
