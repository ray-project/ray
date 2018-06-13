from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from types import FunctionType

import numpy as np
import pickle

import ray

TRAINABLE_CLASS = "trainable_class"
ENV_CREATOR = "env_creator"
RLLIB_MODEL = "rllib_model"
RLLIB_PREPROCESSOR = "rllib_preprocessor"
KNOWN_CATEGORIES = [
    TRAINABLE_CLASS, ENV_CREATOR, RLLIB_MODEL, RLLIB_PREPROCESSOR
]


def register_trainable(name, trainable):
    """Register a trainable function or class.

    Args:
        name (str): Name to register.
        trainable (obj): Function or tune.Trainable class. Functions must
            take (config, status_reporter) as arguments and will be
            automatically converted into a class during registration.
    """

    from ray.tune.trainable import Trainable, wrap_function

    if isinstance(trainable, FunctionType):
        trainable = wrap_function(trainable)
    if not issubclass(trainable, Trainable):
        raise TypeError("Second argument must be convertable to Trainable",
                        trainable)
    _global_registry.register(TRAINABLE_CLASS, name, trainable)


def register_env(name, env_creator):
    """Register a custom environment for use with RLlib.

    Args:
        name (str): Name to register.
        env_creator (obj): Function that creates an env.
    """

    if not isinstance(env_creator, FunctionType):
        raise TypeError("Second argument must be a function.", env_creator)
    _global_registry.register(ENV_CREATOR, name, env_creator)


def _redis_key(category, key):
    """Generate a Redis key for the given category and key.

    Args:
        category (str): The category of the item
        key (str): The unique identifier for the item

    Returns:
        The key to use for storing a the value in Redis.
    """
    return (
        b"TuneRegistry:" + category.encode("ascii") + b"/" +
        key.encode("ascii"))


def _ray_initialized():
    worker = ray.worker.get_global_worker()
    return hasattr(worker, "redis_client")


class _Registry(object):
    def __init__(self):
        self._to_flush = {}

    def register(self, category, key, value):
        if category not in KNOWN_CATEGORIES:
            from ray.tune import TuneError
            raise TuneError("Unknown category {} not among {}".format(
                category, KNOWN_CATEGORIES))
        self._to_flush[(category, key)] = pickle.dumps(value)
        if _ray_initialized():
            self.flush_values_to_redis()

    def contains(self, category, key):
        if _ray_initialized():
            redis_key = _redis_key(category, key)
            worker = ray.worker.get_global_worker()
            value = worker.redis_client.hget(redis_key, "value")
            return value is not None
        else:
            return (category, key) in self._to_flush

    def get(self, category, key):
        if _ray_initialized():
            redis_key = _redis_key(category, key)
            worker = ray.worker.get_global_worker()
            value = worker.redis_client.hget(redis_key, "value")
            if value is None:
                raise ValueError(
                    "Registry value for {}/{} doesn't exist.".format(
                        category, key))
            return pickle.loads(value)
        else:
            return pickle.loads(self._to_flush[(category, key)])

    def flush_values_to_redis(self):
        worker = ray.worker.get_global_worker()
        for (category, key), value in self._to_flush.items():
            redis_key = _redis_key(category, key)
            worker.redis_client.hset(redis_key, "value", value)
        self._to_flush.clear()


_global_registry = _Registry()
ray.worker._post_init_hooks.append(_global_registry.flush_values_to_redis)
