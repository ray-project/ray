import logging
from types import FunctionType

import ray
import ray.cloudpickle as pickle
from ray.experimental.internal_kv import _internal_kv_initialized, \
    _internal_kv_get, _internal_kv_put
from ray.tune.error import TuneError

TRAINABLE_CLASS = "trainable_class"
ENV_CREATOR = "env_creator"
RLLIB_MODEL = "rllib_model"
RLLIB_PREPROCESSOR = "rllib_preprocessor"
RLLIB_ACTION_DIST = "rllib_action_dist"
KNOWN_CATEGORIES = [
    TRAINABLE_CLASS, ENV_CREATOR, RLLIB_MODEL, RLLIB_PREPROCESSOR,
    RLLIB_ACTION_DIST
]

logger = logging.getLogger(__name__)


def has_trainable(trainable_name):
    return _global_registry.contains(TRAINABLE_CLASS, trainable_name)


def get_trainable_cls(trainable_name):
    validate_trainable(trainable_name)
    return _global_registry.get(TRAINABLE_CLASS, trainable_name)


def validate_trainable(trainable_name):
    if not has_trainable(trainable_name):
        # Make sure everything rllib-related is registered.
        from ray.rllib import _register_all
        _register_all()
        if not has_trainable(trainable_name):
            raise TuneError("Unknown trainable: " + trainable_name)


def register_trainable(name, trainable):
    """Register a trainable function or class.

    This enables a class or function to be accessed on every Ray process
    in the cluster.

    Args:
        name (str): Name to register.
        trainable (obj): Function or tune.Trainable class. Functions must
            take (config, status_reporter) as arguments and will be
            automatically converted into a class during registration.
    """

    from ray.tune.trainable import Trainable
    from ray.tune.function_runner import wrap_function

    if isinstance(trainable, type):
        logger.debug("Detected class for trainable.")
    elif isinstance(trainable, FunctionType):
        logger.debug("Detected function for trainable.")
        trainable = wrap_function(trainable)
    elif callable(trainable):
        logger.warning(
            "Detected unknown callable for trainable. Converting to class.")
        trainable = wrap_function(trainable)

    if not issubclass(trainable, Trainable):
        raise TypeError("Second argument must be convertable to Trainable",
                        trainable)
    _global_registry.register(TRAINABLE_CLASS, name, trainable)


def register_env(name, env_creator):
    """Register a custom environment for use with RLlib.

    This enables the environment to be accessed on every Ray process
    in the cluster.

    Args:
        name (str): Name to register.
        env_creator (obj): Function that creates an env.
    """

    if not isinstance(env_creator, FunctionType):
        raise TypeError("Second argument must be a function.", env_creator)
    _global_registry.register(ENV_CREATOR, name, env_creator)


def _make_key(category, key):
    """Generate a binary key for the given category and key.

    Args:
        category (str): The category of the item
        key (str): The unique identifier for the item

    Returns:
        The key to use for storing a the value.
    """
    return (b"TuneRegistry:" + category.encode("ascii") + b"/" +
            key.encode("ascii"))


class _Registry:
    def __init__(self):
        self._to_flush = {}

    def register(self, category, key, value):
        if category not in KNOWN_CATEGORIES:
            from ray.tune import TuneError
            raise TuneError("Unknown category {} not among {}".format(
                category, KNOWN_CATEGORIES))
        self._to_flush[(category, key)] = pickle.dumps(value)
        if _internal_kv_initialized():
            self.flush_values()

    def contains(self, category, key):
        if _internal_kv_initialized():
            value = _internal_kv_get(_make_key(category, key))
            return value is not None
        else:
            return (category, key) in self._to_flush

    def get(self, category, key):
        if _internal_kv_initialized():
            value = _internal_kv_get(_make_key(category, key))
            if value is None:
                raise ValueError(
                    "Registry value for {}/{} doesn't exist.".format(
                        category, key))
            return pickle.loads(value)
        else:
            return pickle.loads(self._to_flush[(category, key)])

    def flush_values(self):
        for (category, key), value in self._to_flush.items():
            _internal_kv_put(_make_key(category, key), value, overwrite=True)
        self._to_flush.clear()


_global_registry = _Registry()
ray.worker._post_init_hooks.append(_global_registry.flush_values)
