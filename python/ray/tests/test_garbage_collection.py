# coding: utf-8
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import logging

import ray
import ray.tests.cluster_utils
import ray.tests.utils

logger = logging.getLogger(__name__)


def test_basic_gc(shutdown_only):
    ray.init(object_store_memory=100 * 1024 * 1024, use_pickle=True)

    @ray.remote
    def shuffle(input):
        return np.random.shuffle(input)

    @ray.remote
    class Actor(object):
        def __init__(self):
            self.large_object = ray.put(
                np.zeros(25 * 1024 * 1024, dtype=np.uint8), weakref=True)

        def get_large_object(self):
            return ray.get(self.large_object)

    actor = Actor.remote()
    ray.get(actor.get_large_object.remote())

    for batch in range(10):
        intermediate_result = shuffle.remote(
            np.zeros(10 * 1024 * 1024, dtype=np.uint8))
        ray.get(intermediate_result)

    # The ray.get below would fail with only LRU eviction, as the object
    # that was ray.put by the actor would have been evicted.
    ray.get(actor.get_large_object.remote())
