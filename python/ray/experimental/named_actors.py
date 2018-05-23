from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
import ray.cloudpickle as pickle
"""
This file contains functions intended to implement the named actor
"""


def _calculate_key_(name):
    """Generate a Redis key with the given name
    Args:
        name: THe name of the named-actor
    """
    return b"Actor:" + str.encode(name)


def get_actor(name):
    """Get a named actor which is previously created. If the actor
     doesn't exist, it will return an error.
    Args:
        name: The name of the named-actor.
    Returns:
        The ActorHandle object corresponding to the name.
    """
    worker = ray.worker.get_global_worker()
    actor_hash = _calculate_key_(name)
    pickled_state = worker.redis_client.hget(actor_hash, name)
    if pickled_state is None:
        raise ValueError("The actor with name={} doesn't exist".format(name))
    handle = pickle.loads(pickled_state)
    return handle


def register_actor(name, actor_handle):
    """Register a named actor under a string key.
   Args:
       name: The name of the named-actor.
       actor_handle: The actor object to be associated with this name
   """
    worker = ray.worker.get_global_worker()
    if type(name) != str:
        raise TypeError("You could only use string as key")
    if type(actor_handle) != ray.actor.ActorHandle:
        raise TypeError("You could only store named-actors.")
    actor_hash = _calculate_key_(name)
    pickled_state = pickle.dumps(actor_handle)
    is_existed = worker.redis_client.hsetnx(actor_hash, name, pickled_state)

    if is_existed == 0:
        raise ValueError(
            "Error: the actor with name={} already exists".format(name))
