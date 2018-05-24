from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
import ray.cloudpickle as pickle


def _calculate_key(name):
    """Generate a Redis key with the given name.

    Args:
        name: The name of the named actor.

    Returns:
        The key to use for storing a named actor in Redis.
    """
    return b"Actor:" + name.encode("ascii")


def get_actor(name):
    """Get a named actor which was previously created.

    If the actor doesn't exist, an exception will be raised.

    Args:
        name: The name of the named actor.

    Returns:
        The ActorHandle object corresponding to the name.
    """
    worker = ray.worker.get_global_worker()
    actor_hash = _calculate_key(name)
    pickled_state = worker.redis_client.hget(actor_hash, name)
    if pickled_state is None:
        raise ValueError("The actor with name={} doesn't exist".format(name))
    handle = pickle.loads(pickled_state)
    return handle


def register_actor(name, actor_handle):
    """Register a named actor under a string key.

   Args:
       name: The name of the named actor.
       actor_handle: The actor object to be associated with this name
   """
    worker = ray.worker.get_global_worker()
    if not isinstance(name, str):
        raise TypeError("The name argument must be a string.")
    if not isinstance(actor_handle, ray.actor.ActorHandle):
        raise TypeError("The actor_handle argument must be an ActorHandle "
                        "object.")
    actor_hash = _calculate_key(name)
    pickled_state = pickle.dumps(actor_handle)

    # Add the actor to Redis if it does not already exist.
    updated = worker.redis_client.hsetnx(actor_hash, name, pickled_state)
    if updated == 0:
        raise ValueError(
            "Error: the actor with name={} already exists".format(name))
