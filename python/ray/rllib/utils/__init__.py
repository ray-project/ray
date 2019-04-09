import logging

from ray.rllib.utils.filter_manager import FilterManager
from ray.rllib.utils.filter import Filter
from ray.rllib.utils.policy_client import PolicyClient
from ray.rllib.utils.policy_server import PolicyServer
from ray.tune.util import merge_dicts, deep_update

logger = logging.getLogger(__name__)


def renamed_class(cls):
    class DeprecationWrapper(cls):
        def __init__(self, *args, **kwargs):
            old_name = cls.__name__.replace("Trainer", "Agent")
            new_name = cls.__name__
            logger.warn("DeprecationWarning: {} has been renamed to {}. ".
                        format(old_name, new_name) +
                        "This will raise an error in the future.")
            cls.__init__(self, *args, **kwargs)

    return DeprecationWrapper


__all__ = [
    "Filter",
    "FilterManager",
    "PolicyClient",
    "PolicyServer",
    "merge_dicts",
    "deep_update",
    "renamed_class",
]
