from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import json
import multiprocessing
import numpy as np
import pytest
import time
import warnings

import ray
from ray.test.cluster_utils import Cluster

if (multiprocessing.cpu_count() < 40
        or ray.utils.get_system_memory() < 50 * 10**9):
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


@pytest.fixture()
def ray_start_empty_cluster():
    cluster = Cluster()
    yield cluster

    # The code after the yield will run as teardown code.
    ray.shutdown()
    cluster.shutdown()


# This test is here to make sure that when we broadcast an object to a bunch of
# machines, we don't have too many excess object transfers.
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
        ray.get([
            f._remote(args=[x_id], resources={str(i % num_nodes): 1})
            for i in range(10 * num_nodes)
        ])

    for _ in range(3):
        # Broadcast an object to all machines.
        x_id = create_object.remote()
        object_ids.append(x_id)
        ray.get([
            f._remote(args=[x_id], resources={str(i % num_nodes): 1})
            for i in range(10 * num_nodes)
        ])

    # Wait for profiling information to be pushed to the profile table.
    time.sleep(1)
    transfer_events = ray.global_state.chrome_tracing_object_transfer_dump()

    # Make sure that each object was transferred a reasonable number of times.
    for x_id in object_ids:
        transfer_counts = ray.utils.count_object_transfers(
            x_id, transfer_events=transfer_events)

        total_transfers = sum(transfer_counts.values())

        # Each object must have been broadcast to each remote machine.
        assert total_transfers >= num_nodes - 1
        # If more object transfers than necessary have been done, print a
        # warning.
        if total_transfers > num_nodes - 1:
            warnings.warn("This object was transferred {} times, when only {} "
                          "transfers were required.".format(
                              total_transfers, num_nodes - 1))
        # Each object should not have been broadcast more than once from every
        # machine to every other machine. Also, a pair of machines should not
        # both have sent the object to each other.
        assert total_transfers <= (num_nodes - 1) * num_nodes / 2

        # Make sure that no object was sent multiple times between the same
        # pair of object managers.
        assert all(value == 1 for value in transfer_counts.values())


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

    actors = [
        Actor._remote(args=[], kwargs={}, resources={str(i % num_nodes): 1})
        for i in range(100)
    ]

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
        transfer_counts = ray.utils.count_object_transfers(
            x_id, transfer_events=transfer_events)

        total_transfers = sum(transfer_counts.values())

        # Each object must have been broadcast to each remote machine.
        assert total_transfers >= num_nodes - 1
        # If more object transfers than necessary have been done, print a
        # warning.
        if total_transfers > num_nodes - 1:
            warnings.warn("This object was transferred {} times, when only {} "
                          "transfers were required.".format(
                              total_transfers, num_nodes - 1))
        # Each object should not have been broadcast more than once from every
        # machine to every other machine. Also, a pair of machines should not
        # both have sent the object to each other.
        assert total_transfers <= (num_nodes - 1) * num_nodes / 2

        # Make sure that no object was sent multiple times between the same
        # pair of object managers.
        assert all(value == 1 for value in transfer_counts.values())


# The purpose of this test is to make sure that an object that was already been
# transferred to a node can be transferred again.
def test_object_transfer_retry(ray_start_empty_cluster):
    cluster = ray_start_empty_cluster

    repeated_push_delay = 4

    config = json.dumps({
        "object_manager_repeated_push_delay_ms": repeated_push_delay * 1000
    })
    cluster.add_node(_internal_config=config)
    cluster.add_node(resources={"GPU": 1}, _internal_config=config)
    ray.init(redis_address=cluster.redis_address)

    @ray.remote(num_gpus=1)
    def f(size):
        return np.zeros(size, dtype=np.uint8)

    x_ids = [f.remote(10**i) for i in [1, 2, 3, 4, 5, 6, 7]]
    assert not any(
        ray.worker.global_worker.plasma_client.contains(
            ray.pyarrow.plasma.ObjectID(x_id.id())) for x_id in x_ids)

    start_time = time.time()

    # Get the objects locally to cause them to be transferred.
    xs = ray.get(x_ids)

    # Cause all objects to be flushed.
    del xs
    x = np.zeros(10**7, dtype=np.uint8)
    for _ in range(10):
        ray.put(x)
    assert not any(
        ray.worker.global_worker.plasma_client.contains(
            ray.pyarrow.plasma.ObjectID(x_id.id())) for x_id in x_ids)

    end_time = time.time()

    # Get the objects again and make sure they get transferred.
    xs = ray.get(x_ids)

    end_transfer_time = time.time()

    # Make sure that the object was retransferred before the object manager
    # repeated push delay expired.
    if end_time - start_time <= repeated_push_delay:
        warnings.warn("This test didn't really fail, but the timing is such "
                      "that it is not testing the thing it should be testing.")
    # We should have had to wait for the repeated push delay.
    assert end_transfer_time - start_time >= repeated_push_delay

    # Flush the objects again and wait longer than the repeated push delay and
    # make sure that the objects are transferred again.
    del xs
    for _ in range(10):
        ray.put(x)
    assert not any(
        ray.worker.global_worker.plasma_client.contains(
            ray.pyarrow.plasma.ObjectID(x_id.id())) for x_id in x_ids)

    time.sleep(repeated_push_delay)
    ray.get(x_ids)


# The purpose of this test is to make sure we can transfer many objects. In the
# past, this has caused failures in which object managers create too many open
# files and run out of resources.
def test_many_small_transfers(ray_start_cluster):
    cluster, num_nodes = ray_start_cluster

    @ray.remote
    def f(*args):
        pass

    # This function creates 1000 objects on each machine and then transfers
    # each object to every other machine.
    def do_transfers():
        id_lists = []
        for i in range(num_nodes):
            id_lists.append([
                f._remote(args=[], kwargs={}, resources={str(i): 1})
                for _ in range(1000)
            ])
        ids = []
        for i in range(num_nodes):
            for j in range(num_nodes):
                if i == j:
                    continue
                ids.append(
                    f._remote(
                        args=id_lists[j], kwargs={}, resources={str(i): 1}))

        # Wait for all of the transfers to finish.
        ray.get(ids)

    do_transfers()
    do_transfers()
    do_transfers()
    do_transfers()


# The purpose of this test is to make sure we can broadcast an object to all
# other nodes without incurring too many more transfers than the minimum
# necessary amount.
def test_large_broadcast(ray_start_empty_cluster):
    cluster = ray_start_empty_cluster
    num_nodes = 30
    for i in range(num_nodes):
        cluster.add_node(resources={str(i): 100})
    ray.init(redis_address=cluster.redis_address)

    @ray.remote
    def f(*args):
        pass

    x_ids = [ray.put(np.ones(size, dtype=np.uint8)) for size in [10**6, 10**7]]
    for x_id in x_ids:
        ray.get([
            f._remote(args=[x_id], kwargs={}, resources={str(i): 1})
            for i in range(num_nodes)
        ])

    # Wait for profiling information to be pushed to the profile table.
    time.sleep(1)
    transfer_events = ray.global_state.chrome_tracing_object_transfer_dump()

    for x_id in x_ids:
        transfer_counts = ray.utils.count_object_transfers(
            x_id, transfer_events=transfer_events)

        recipients = {pair[1] for pair in transfer_counts.keys()}
        assert len(recipients) == num_nodes - 1
        num_recipients_with_duplicates = 0
        for recipient in recipients:
            recipient_counts = 0
            for key, val in transfer_counts.items():
                if key[1] == recipient:
                    recipient_counts += val
            assert recipient_counts <= 2, (
                "Some node received the object twice.")
            if recipient_counts > 1:
                num_recipients_with_duplicates += 1

        if num_recipients_with_duplicates > 0:
            warnings.warn("{} nodes received the same object more than once."
                          .format(num_recipients_with_duplicates))
