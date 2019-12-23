# coding: utf-8
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import logging

import ray
import ray.cluster_utils
import ray.test_utils

logger = logging.getLogger(__name__)


def test_basic_gc(shutdown_only):
    ray.init(object_store_memory=100 * 1024 * 1024)

    @ray.remote
    def shuffle(input):
        return np.random.shuffle(input)

    @ray.remote
    class Actor(object):
        def __init__(self):
            # Hold a long-lived reference to a ray.put object. This should not
            # be garbage collected while the actor is alive.
            self.large_object = ray.put(
                np.zeros(25 * 1024 * 1024, dtype=np.uint8), weakref=True)

        def get_large_object(self):
            return ray.get(self.large_object)

    actor = Actor.remote()

    # Fill up the object store with short-lived objects. These should be
    # evicted before the long-lived object whose reference is held by
    # the actor.
    for batch in range(10):
        intermediate_result = shuffle.remote(
            np.zeros(10 * 1024 * 1024, dtype=np.uint8))
        ray.get(intermediate_result)

    # The ray.get below would fail with only LRU eviction, as the object
    # that was ray.put by the actor would have been evicted.
    ray.get(actor.get_large_object.remote())


def test_pending_task_dependency(shutdown_only):
    ray.init(object_store_memory=100 * 1024 * 1024, use_pickle=True)

    @ray.remote
    def pending(input1, input2):
        return

    @ray.remote
    def slow(dep):
        pass

    # The object that is ray.put here will go out of scope immediately, so if
    # pending task dependencies aren't considered, it will be evicted before
    # the ray.get below due to the subsequent ray.puts that fill up the object
    # store.
    np_array = np.zeros(40 * 1024 * 1024, dtype=np.uint8)
    random_id = ray.ObjectID.from_random()
    oid = pending.remote(ray.put(np_array), slow.remote(random_id))

    for _ in range(2):
        ray.put(np_array)

    ray.worker.global_worker.put_object(None, object_id=random_id)
    ray.get(oid)


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
