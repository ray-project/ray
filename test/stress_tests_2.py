from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import multiprocessing
import numpy as np
import pytest
import time
import warnings

import ray
from ray.test.cluster_utils import Cluster


if (multiprocessing.cpu_count() < 40 or
        ray.utils.get_system_memory() < 50 * 10**9):
    warnings.warn("This test must be run on large machines.")


def create_cluster(num_nodes):
    cluster = Cluster()
    for i in range(num_nodes):
        cluster.add_node(resources={str(i): 100}, object_store_memory=10**9)

    ray.init(redis_address=cluster.redis_address)
    return cluster


@pytest.fixture()
def ray_start_cluster():
    num_nodes = 5
    cluster = create_cluster(num_nodes)
    yield cluster, num_nodes

    # The code after the yield will run as teardown code.
    ray.shutdown()
    cluster.shutdown()


# This test is here to make sure that when we broadcast an object to a bunch of
# machines, we don't have too many excess object transfers.
@pytest.mark.skip("This test does not work yet.")
def test_object_broadcast(ray_start_cluster):
    cluster, num_nodes = ray_start_cluster

    @ray.remote
    def f(x):
        return

    x = np.zeros(10**8, dtype=np.uint8)

    @ray.remote
    def create_object():
        return np.zeros(10**8, dtype=np.uint8)

    object_ids = []

    for _ in range(3):
        # Broadcast an object to all machines.
        x_id = ray.put(x)
        object_ids.append(x_id)
        ray.get([f._submit(args=[x_id], resources={str(i % num_nodes): 1})
                 for i in range(10 * num_nodes)])

    for _ in range(3):
        # Broadcast an object to all machines.
        x_id = create_object.remote()
        object_ids.append(x_id)
        ray.get([f._submit(args=[x_id], resources={str(i % num_nodes): 1})
                 for i in range(10 * num_nodes)])

    # Wait for profiling information to be pushed to the profile table.
    time.sleep(1)
    transfer_events = ray.global_state.chrome_tracing_object_transfer_dump()

    # Make sure that each object was transferred a reasonable number of times.
    for x_id in object_ids:
        relevant_events = [
            event for event in transfer_events
            if event["cat"] == "transfer_send"
            and event["args"][0] == x_id.hex() and event["args"][2] == 1
        ]
        # Each object must have been broadcast to each remote machine.
        assert len(relevant_events) >= num_nodes - 1
        # If more object transfers than necessary have been done, print a
        # warning.
        if len(relevant_events) > num_nodes - 1:
            warnings.warn("This object was trasnferred {} times, when only {} "
                          "transfers were required."
                          .format(len(relevant_events), num_nodes - 1))
        # Each object should not have been broadcast more than once from every
        # machine to every other machine.
        assert len(relevant_events) <= (num_nodes - 1) * num_nodes / 2


# When submitting an actor method, we try to pre-emptively push its arguments
# to the actor's object manager. However, in the past we did not deduplicate
# the pushes and so the same object could get shipped to the same object
# manager many times. This test checks that that isn't happening.
def test_actor_broadcast(ray_start_cluster):
    cluster, num_nodes = ray_start_cluster

    @ray.remote
    class Actor(object):
        def ready(self):
            pass

        def set_weights(self, x):
            pass

    actors = [Actor._submit(
                  args=[], kwargs={},
                  resources={str(i % num_nodes): 1})
              for i in range(100)]

    # Wait for the actors to start up.
    ray.get([a.ready.remote() for a in actors])

    object_ids = []

    # Broadcast a large object to all actors.
    for _ in range(10):
        x_id = ray.put(np.zeros(10**7, dtype=np.uint8))
        object_ids.append(x_id)
        # Pass the object into a method for every actor.
        ray.get([a.set_weights.remote(x_id) for a in actors])

    # Wait for profiling information to be pushed to the profile table.
    time.sleep(1)
    transfer_events = ray.global_state.chrome_tracing_object_transfer_dump()

    # Make sure that each object was transferred a reasonable number of times.
    for x_id in object_ids:
        relevant_events = [
            event for event in transfer_events
            if event["cat"] == "transfer_send"
            and event["args"][0] == x_id.hex() and event["args"][2] == 1
        ]
        # Each object must have been broadcast to each remote machine.
        assert len(relevant_events) >= num_nodes - 1
        # If more object transfers than necessary have been done, print a
        # warning.
        if len(relevant_events) > num_nodes - 1:
            warnings.warn("This object was trasnferred {} times, when only {} "
                          "transfers were required."
                          .format(len(relevant_events), num_nodes - 1))
        # Each object should not have been broadcast more than once from every
        # machine to every other machine.
        assert len(relevant_events) <= (num_nodes - 1) * num_nodes / 2
