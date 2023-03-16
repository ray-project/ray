import atexit
import logging
from functools import partial
from types import FunctionType
from typing import Callable, Optional, Type, Union

import ray
import ray.cloudpickle as pickle
from ray.experimental.internal_kv import (
    _internal_kv_get,
    _internal_kv_initialized,
    _internal_kv_put,
    _internal_kv_del,
)
from ray.tune.error import TuneError
from ray.util.annotations import DeveloperAPI

TRAINABLE_CLASS = "trainable_class"
ENV_CREATOR = "env_creator"
RLLIB_MODEL = "rllib_model"
RLLIB_PREPROCESSOR = "rllib_preprocessor"
RLLIB_ACTION_DIST = "rllib_action_dist"
RLLIB_INPUT = "rllib_input"
RLLIB_CONNECTOR = "rllib_connector"
TEST = "__test__"
KNOWN_CATEGORIES = [
    TRAINABLE_CLASS,
    ENV_CREATOR,
    RLLIB_MODEL,
    RLLIB_PREPROCESSOR,
    RLLIB_ACTION_DIST,
    RLLIB_INPUT,
    RLLIB_CONNECTOR,
    TEST,
]

logger = logging.getLogger(__name__)


def _has_trainable(trainable_name):
    return _global_registry.contains(TRAINABLE_CLASS, trainable_name)


@DeveloperAPI
def get_trainable_cls(trainable_name):
    validate_trainable(trainable_name)
    return _global_registry.get(TRAINABLE_CLASS, trainable_name)


@DeveloperAPI
def validate_trainable(trainable_name):
    if not _has_trainable(trainable_name):
        # Make sure everything rllib-related is registered.
        from ray.rllib import _register_all

        _register_all()
        if not _has_trainable(trainable_name):
            raise TuneError("Unknown trainable: " + trainable_name)


@DeveloperAPI
def is_function_trainable(trainable: Union[str, Callable, Type]) -> bool:
    """Check if a given trainable is a function trainable.
    Either the trainable has been wrapped as a FunctionTrainable class already,
    or it's still a FunctionType/partial/callable."""
    from ray.tune.trainable import FunctionTrainable

    if isinstance(trainable, str):
        trainable = get_trainable_cls(trainable)

    is_wrapped_func = isinstance(trainable, type) and issubclass(
        trainable, FunctionTrainable
    )
    return is_wrapped_func or (
        not isinstance(trainable, type)
        and (
            isinstance(trainable, FunctionType)
            or isinstance(trainable, partial)
            or callable(trainable)
        )
    )


@DeveloperAPI
def register_trainable(name: str, trainable: Union[Callable, Type], warn: bool = True):
    """Register a trainable function or class.

    This enables a class or function to be accessed on every Ray process
    in the cluster.

    Args:
        name: Name to register.
        trainable: Function or tune.Trainable class. Functions must
            take (config, status_reporter) as arguments and will be
            automatically converted into a class during registration.
    """

    from ray.tune.trainable import wrap_function
    from ray.tune.trainable import Trainable

    if isinstance(trainable, type):
        logger.debug("Detected class for trainable.")
    elif isinstance(trainable, FunctionType) or isinstance(trainable, partial):
        logger.debug("Detected function for trainable.")
        trainable = wrap_function(trainable, warn=warn)
    elif callable(trainable):
        logger.info("Detected unknown callable for trainable. Converting to class.")
        trainable = wrap_function(trainable, warn=warn)

    if not issubclass(trainable, Trainable):
        raise TypeError("Second argument must be convertable to Trainable", trainable)
    _global_registry.register(TRAINABLE_CLASS, name, trainable)


def _unregister_trainables():
    _global_registry.unregister_all(TRAINABLE_CLASS)


@DeveloperAPI
def register_env(name: str, env_creator: Callable):
    """Register a custom environment for use with RLlib.

    This enables the environment to be accessed on every Ray process
    in the cluster.

    Args:
        name: Name to register.
        env_creator: Callable that creates an env.
    """

    if not callable(env_creator):
        raise TypeError("Second argument must be callable.", env_creator)
    _global_registry.register(ENV_CREATOR, name, env_creator)


def _unregister_envs():
    _global_registry.unregister_all(ENV_CREATOR)


@DeveloperAPI
def register_input(name: str, input_creator: Callable):
    """Register a custom input api for RLlib.

    Args:
        name: Name to register.
        input_creator: Callable that creates an
            input reader.
    """
    if not callable(input_creator):
        raise TypeError("Second argument must be callable.", input_creator)
    _global_registry.register(RLLIB_INPUT, name, input_creator)


def _unregister_inputs():
    _global_registry.unregister_all(RLLIB_INPUT)


@DeveloperAPI
def registry_contains_input(name: str) -> bool:
    return _global_registry.contains(RLLIB_INPUT, name)


@DeveloperAPI
def registry_get_input(name: str) -> Callable:
    return _global_registry.get(RLLIB_INPUT, name)


def _unregister_all():
    _unregister_inputs()
    _unregister_envs()
    _unregister_trainables()


def _check_serializability(key, value):
    _global_registry.register(TEST, key, value)


def _make_key(prefix: str, category: str, key: str):
    """Generate a binary key for the given category and key.

    Args:
        prefix: Prefix
        category: The category of the item
        key: The unique identifier for the item

    Returns:
        The key to use for storing a the value.
    """
    return (
        b"TuneRegistry:"
        + prefix.encode("ascii")
        + b":"
        + category.encode("ascii")
        + b"/"
        + key.encode("ascii")
    )


class _Registry:
    def __init__(self, prefix: Optional[str] = None):
        """If no prefix is given, use runtime context job ID."""
        self._to_flush = {}
        self._prefix = prefix
        self._registered = set()
        self._atexit_handler_registered = False

    @property
    def prefix(self):
        if not self._prefix:
            self._prefix = ray.get_runtime_context().get_job_id()
        return self._prefix

    def _register_atexit(self):
        if self._atexit_handler_registered:
            # Already registered
            return

        if ray._private.worker.global_worker.mode != ray.SCRIPT_MODE:
            # Only cleanup on the driver
            return

        atexit.register(_unregister_all)
        self._atexit_handler_registered = True

    def register(self, category, key, value):
        """Registers the value with the global registry.

        Raises:
            PicklingError if unable to pickle to provided file.
        """
        if category not in KNOWN_CATEGORIES:
            from ray.tune import TuneError

            raise TuneError(
                "Unknown category {} not among {}".format(category, KNOWN_CATEGORIES)
            )
        self._to_flush[(category, key)] = pickle.dumps_debug(value)
        if _internal_kv_initialized():
            self.flush_values()

    def unregister(self, category, key):
        if _internal_kv_initialized():
            _internal_kv_del(_make_key(self.prefix, category, key))
        else:
            self._to_flush.pop((category, key), None)

    def unregister_all(self, category: Optional[str] = None):
        remaining = set()
        for (cat, key) in self._registered:
            if category and category == cat:
                self.unregister(cat, key)
            else:
                remaining.add((cat, key))
        self._registered = remaining

    def contains(self, category, key):
        if _internal_kv_initialized():
            value = _internal_kv_get(_make_key(self.prefix, category, key))
            return value is not None
        else:
            return (category, key) in self._to_flush

    def get(self, category, key):
        if _internal_kv_initialized():
            value = _internal_kv_get(_make_key(self.prefix, category, key))
            if value is None:
                raise ValueError(
                    "Registry value for {}/{} doesn't exist.".format(category, key)
                )
            return pickle.loads(value)
        else:
            return pickle.loads(self._to_flush[(category, key)])

    def flush_values(self):
        self._register_atexit()
        for (category, key), value in self._to_flush.items():
            _internal_kv_put(
                _make_key(self.prefix, category, key), value, overwrite=True
            )
            self._registered.add((category, key))
        self._to_flush.clear()


_global_registry = _Registry()
ray._private.worker._post_init_hooks.append(_global_registry.flush_values)


class _ParameterRegistry:
    def __init__(self):
        self.to_flush = {}
        self.references = {}

    def put(self, k, v):
        self.to_flush[k] = v
        if ray.is_initialized():
            self.flush()

    def get(self, k):
        if not ray.is_initialized():
            return self.to_flush[k]
        return ray.get(self.references[k])

    def flush(self):
        for k, v in self.to_flush.items():
            if isinstance(v, ray.ObjectRef):
                self.references[k] = v
            else:
                self.references[k] = ray.put(v)
        self.to_flush.clear()
