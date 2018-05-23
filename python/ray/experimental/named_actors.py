from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
import ray.cloudpickle as pickle
"""
This file contains functions intended to implement the named actor
"""


# global_worker = ray.worker.get_global_worker()

def _calculate_key_(name):
    return b"Actor:" + str.encode(name)

def get_actor(name):
    worker = ray.worker.get_global_worker()
    actor_hash = _calculate_key_(name)
    pickled_state = worker.redis_client.hmget(actor_hash, name)
    assert len(pickled_state) == 1, \
        "Error: Multiple actors under this name."
    assert pickled_state[0] is not None, \
        "Error: actor with name {} doesn't exist".format(name)
    handle = pickle.loads(pickled_state[0])
    return handle

def register_actor(name, actor_handle):
    worker = ray.worker.get_global_worker()
    actor_hash = _calculate_key_(name)
    assert type(actor_handle) == ray.actor.ActorHandle, \
        "Error: you could only store named-actors."
    is_existed = worker.redis_client.hexists(actor_hash, name)
    assert not is_existed, \
        "Error: the actor with name={} already exists".format(name)
    pickled_state = pickle.dumps(actor_handle)
    worker.redis_client.hmset(actor_hash, {name: pickled_state})
