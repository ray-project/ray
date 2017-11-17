from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from types import FunctionType

import ray
from ray.local_scheduler import ObjectID
from ray.tune.trainable import Trainable, wrap_function

TRAINABLE_CLASS = "trainable_class"
ENV_CREATOR = "env_creator"
KNOWN_CATEGORIES = [TRAINABLE_CLASS, ENV_CREATOR]


def register_trainable(name, trainable):
    """Register a trainable function or class.

    Args:
        name (str): Name to register.
        trainable (obj): Function or tune.Trainable object. Functions must
            take (config, status_reporter) as arguments and will be
            automatically converted into a trainable obj during registration.
    """

    if isinstance(trainable, FunctionType):
        trainable = wrap_function(trainable)
    else:
        assert issubclass(trainable, Trainable), trainable
    _register(TRAINABLE_CLASS, name, trainable)


def register_env(name, env_creator):
    """Register a custom environment for use with RLlib.

    Args:
        name (str): Name to register.
        env_creator (obj): Function that creates an env.
    """

    assert issubclass(env_creator, FunctionType)
    _register(ENV_CREATOR, name, env_creator)


def _register(category, key, value):
    """Register a user-defined object in the tune object registry.

    Tune and RLLib internal classes will use this registry to lookup object
    values by name from Ray remote workers. This lets you e.g. specify
    custom algorithms in JSON configurations via string names.

    Args:
        category (str): Object category.
        key (str): Name to give this object.
        value (obj): Serializable object to associate with the specified name.
    """

    _default_registry.register(category, key, value)


def get_registry():
    """Use this to access the registry. This requires ray to be initialized."""

    _default_registry.flush_values_to_object_store()
    return _default_registry


class _Registry(object):
    def __init__(self):
        self._all_objects = {}

    def register(self, category, key, value):
        if category not in KNOWN_CATEGORIES:
            raise Exception("Unknown category {} not among {}".format(
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
                self._all_objects[k] = ray.put(v)


_default_registry = _Registry()
