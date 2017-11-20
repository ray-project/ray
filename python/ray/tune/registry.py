from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from types import FunctionType

import ray
from ray.tune import TuneError
from ray.local_scheduler import ObjectID
from ray.tune.trainable import Trainable, wrap_function

TRAINABLE_CLASS = "trainable_class"
ENV_CREATOR = "env_creator"
KNOWN_CATEGORIES = [TRAINABLE_CLASS, ENV_CREATOR]


def register_trainable(name, trainable):
    """Register a trainable function or class.

    Args:
        name (str): Name to register.
        trainable (obj): Function or tune.Trainable clsas. Functions must
            take (config, status_reporter) as arguments and will be
            automatically converted into a class during registration.
    """

    if isinstance(trainable, FunctionType):
        trainable = wrap_function(trainable)
    if not issubclass(trainable, Trainable):
        raise TypeError(
            "Second argument must be convertable to Trainable", trainable)
    _default_registry.register(TRAINABLE_CLASS, name, trainable)


def register_env(name, env_creator):
    """Register a custom environment for use with RLlib.

    Args:
        name (str): Name to register.
        env_creator (obj): Function that creates an env.
    """

    if not isinstance(env_creator, FunctionType):
        raise TypeError(
            "Second argument must be a function.", env_creator)
    _default_registry.register(ENV_CREATOR, name, env_creator)


def get_registry():
    """Use this to access the registry. This requires ray to be initialized."""

    _default_registry.flush_values_to_object_store()

    # returns a registry copy that doesn't include the hard refs
    return _Registry(_default_registry._all_objects)


class _Registry(object):
    def __init__(self, objs={}):
        self._all_objects = objs
        self._refs = []  # hard refs that prevent eviction of objects

    def register(self, category, key, value):
        if category not in KNOWN_CATEGORIES:
            raise TuneError("Unknown category {} not among {}".format(
                category, KNOWN_CATEGORIES))
        self._all_objects[(category, key)] = value

    def contains(self, category, key):
        return (category, key) in self._all_objects

    def get(self, category, key):
        value = self._all_objects[(category, key)]
        if type(value) == ObjectID:
            return ray.get(value)
        else:
            return value

    def flush_values_to_object_store(self):
        for k, v in self._all_objects.items():
            if type(v) != ObjectID:
                obj = ray.put(v)
                self._all_objects[k] = obj
                self._refs.append(ray.get(obj))


_default_registry = _Registry()
