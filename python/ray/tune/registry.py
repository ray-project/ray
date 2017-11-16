from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray

TRAINABLE_FUNCTION = "trainable_function"
TRAINABLE_CLASS = "trainable_class"
ENV_CREATOR = "env_creator"

KNOWN_CATEGORIES = [TRAINABLE_FUNCTION, TRAINABLE_CLASS, ENV_CREATOR]


def register(category, key, value):
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


class _Registry(object):
    def __init__(self):
        self._all_objects = {}

    def register(self, category, key, value):
        if category not in KNOWN_CATEGORIES:
            raise Exception("Unknown category {} not among {}".format(
                category, KNOWN_CATEGORIES))
        self._all_objects[(category, key)] = ray.put(value)

    def contains(self, category, key):
        return (category, key) in self._all_objects

    def get(self, category, key):
        return ray.get(self._all_objects[(category, key)])


_default_registry = _Registry()
