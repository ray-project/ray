from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import multiprocessing
import numpy as np
import os
import pytest
import time

import ray
from ray.test.cluster_utils import Cluster


# if (multiprocessing.cpu_count() < 40 or
#         ray.utils.get_system_memory() < 50 * 10**9):
#     raise Exception("This test must be run on large machines.")


@pytest.fixture()
def ray_start_cluster():
    cluster = Cluster()
    #cluster = Cluster(initialize_head=True, connect=True)
    yield cluster

    # The code after the yield will run as teardown code.
    ray.shutdown()
    cluster.shutdown()


def test_object_broadcast(ray_start_cluster):
    cluster = ray_start_cluster
    num_nodes = 10
    for i in range(num_nodes):
        cluster.add_node(resources={str(i): 1}, object_store_memory=10**9)

    ray.init(redis_address=cluster.redis_address)

    @ray.remote
    def f(x):
        return

    x = np.zeros(3 * 10**8, dtype=np.uint8)

    for j in range(5):
        print(j)
        # Broadcast an object to all machines.
        x_id = ray.put(x)
        ray.get([f._submit(args=[x_id], resources={str(i): 1})
                 for i in range(num_nodes)])
