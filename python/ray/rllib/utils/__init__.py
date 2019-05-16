import logging
import os

from ray.rllib.utils.filter_manager import FilterManager
from ray.rllib.utils.filter import Filter
from ray.rllib.utils.policy_client import PolicyClient
from ray.rllib.utils.policy_server import PolicyServer
from ray.tune.util import merge_dicts, deep_update

logger = logging.getLogger(__name__)


def renamed_class(cls):
    """Helper class for renaming Agent => Trainer with a warning."""

    class DeprecationWrapper(cls):
        def __init__(self, config=None, env=None, logger_creator=None):
            old_name = cls.__name__.replace("Trainer", "Agent")
            new_name = cls.__name__
            logger.warn("DeprecationWarning: {} has been renamed to {}. ".
                        format(old_name, new_name) +
                        "This will raise an error in the future.")
            cls.__init__(self, config, env, logger_creator)

    DeprecationWrapper.__name__ = cls.__name__

    return DeprecationWrapper


def try_import_tf():
    if "RLLIB_TEST_NO_TF_IMPORT" in os.environ:
        logger.warning("Not importing TensorFlow for test purposes")
        return None

    try:
        import tensorflow as tf
        return tf
    except ImportError:
        return None


__all__ = [
    "Filter",
    "FilterManager",
    "PolicyClient",
    "PolicyServer",
    "merge_dicts",
    "deep_update",
    "renamed_class",
    "try_import_tf",
]
