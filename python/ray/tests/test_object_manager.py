from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from collections import defaultdict
import json
import multiprocessing
import numpy as np
import pytest
import time
import warnings

import ray
from ray.tests.cluster_utils import Cluster

# TODO(yuhguo): This test file requires a lot of CPU/memory, and
# better be put in Jenkins. However, it fails frequently in Jenkins, but
# works well in Travis. We should consider moving it back to Jenkins once
# we figure out the reason.

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
def ray_start_cluster_with_resource():
    num_nodes = 5
    cluster = create_cluster(num_nodes)
    yield cluster, num_nodes

    # The code after the yield will run as teardown code.
    ray.shutdown()
    cluster.shutdown()


# This test is here to make sure that when we broadcast an object to a bunch of
# machines, we don't have too many excess object transfers.
def test_object_broadcast(ray_start_cluster_with_resource):
    cluster, num_nodes = ray_start_cluster_with_resource

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
        relevant_events = [
            event for event in transfer_events
            if event["cat"] == "transfer_send"
            and event["args"][0] == x_id.hex() and event["args"][2] == 1
        ]

        # NOTE: Each event currently appears twice because we duplicate the
        # send and receive boxes to underline them with a box (black if it is a
        # send and gray if it is a receive). So we need to remove these extra
        # boxes here.
        deduplicated_relevant_events = [
            event for event in relevant_events if event["cname"] != "black"
        ]
        assert len(deduplicated_relevant_events) * 2 == len(relevant_events)
        relevant_events = deduplicated_relevant_events

        # Each object must have been broadcast to each remote machine.
        assert len(relevant_events) >= num_nodes - 1
        # If more object transfers than necessary have been done, print a
        # warning.
        if len(relevant_events) > num_nodes - 1:
            warnings.warn("This object was transferred {} times, when only {} "
                          "transfers were required.".format(
                              len(relevant_events), num_nodes - 1))
        # Each object should not have been broadcast more than once from every
        # machine to every other machine. Also, a pair of machines should not
        # both have sent the object to each other.
        assert len(relevant_events) <= (num_nodes - 1) * num_nodes / 2

        # Make sure that no object was sent multiple times between the same
        # pair of object managers.
        send_counts = defaultdict(int)
        for event in relevant_events:
            # The pid identifies the sender and the tid identifies the
            # receiver.
            send_counts[(event["pid"], event["tid"])] += 1
        assert all(value == 1 for value in send_counts.values())


# When submitting an actor method, we try to pre-emptively push its arguments
# to the actor's object manager. However, in the past we did not deduplicate
# the pushes and so the same object could get shipped to the same object
# manager many times. This test checks that that isn't happening.
def test_actor_broadcast(ray_start_cluster_with_resource):
    cluster, num_nodes = ray_start_cluster_with_resource

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
        relevant_events = [
            event for event in transfer_events
            if event["cat"] == "transfer_send"
            and event["args"][0] == x_id.hex() and event["args"][2] == 1
        ]

        # NOTE: Each event currently appears twice because we duplicate the
        # send and receive boxes to underline them with a box (black if it is a
        # send and gray if it is a receive). So we need to remove these extra
        # boxes here.
        deduplicated_relevant_events = [
            event for event in relevant_events if event["cname"] != "black"
        ]
        assert len(deduplicated_relevant_events) * 2 == len(relevant_events)
        relevant_events = deduplicated_relevant_events

        # Each object must have been broadcast to each remote machine.
        assert len(relevant_events) >= num_nodes - 1
        # If more object transfers than necessary have been done, print a
        # warning.
        if len(relevant_events) > num_nodes - 1:
            warnings.warn("This object was transferred {} times, when only {} "
                          "transfers were required.".format(
                              len(relevant_events), num_nodes - 1))
        # Each object should not have been broadcast more than once from every
        # machine to every other machine. Also, a pair of machines should not
        # both have sent the object to each other.
        assert len(relevant_events) <= (num_nodes - 1) * num_nodes / 2

        # Make sure that no object was sent multiple times between the same
        # pair of object managers.
        send_counts = defaultdict(int)
        for event in relevant_events:
            # The pid identifies the sender and the tid identifies the
            # receiver.
            send_counts[(event["pid"], event["tid"])] += 1
        assert all(value == 1 for value in send_counts.values())


# The purpose of this test is to make sure that an object that was already been
# transferred to a node can be transferred again.
def test_object_transfer_retry(ray_start_cluster):
    cluster = ray_start_cluster

    repeated_push_delay = 4

    # Force the sending object manager to allow duplicate pushes again sooner.
    # Also, force the receiving object manager to retry the Pull sooner. We
    # make the chunk size smaller in order to make it easier to test objects
    # with multiple chunks.
    config = json.dumps({
        "object_manager_repeated_push_delay_ms": repeated_push_delay * 1000,
        "object_manager_pull_timeout_ms": repeated_push_delay * 1000 / 4,
        "object_manager_default_chunk_size": 1000
    })
    object_store_memory = 10**8
    cluster.add_node(
        object_store_memory=object_store_memory, _internal_config=config)
    cluster.add_node(
        num_gpus=1,
        object_store_memory=object_store_memory,
        _internal_config=config)
    ray.init(redis_address=cluster.redis_address)

    @ray.remote(num_gpus=1)
    def f(size):
        return np.zeros(size, dtype=np.uint8)

    # Transfer an object to warm up the object manager.
    ray.get(f.remote(10**6))

    x_ids = [f.remote(10**i) for i in [1, 2, 3, 4]]
    assert not any(
        ray.worker.global_worker.plasma_client.contains(
            ray.pyarrow.plasma.ObjectID(x_id.binary())) for x_id in x_ids)

    # Get the objects locally to cause them to be transferred. This is the
    # first time the objects are getting transferred, so it should happen
    # quickly.
    start_time = time.time()
    xs = ray.get(x_ids)
    end_time = time.time()
    if end_time - start_time > repeated_push_delay:
        warnings.warn("The initial transfer took longer than the repeated "
                      "push delay, so this test may not be testing the thing "
                      "it's supposed to test.")

    # Cause all objects to be flushed.
    del xs
    x = np.zeros(object_store_memory // 10, dtype=np.uint8)
    for _ in range(15):
        ray.put(x)
    assert not any(
        ray.worker.global_worker.plasma_client.contains(
            ray.pyarrow.plasma.ObjectID(x_id.binary())) for x_id in x_ids)

    end_time = time.time()
    # Make sure that the first time the objects get transferred, it happens
    # quickly.
    assert end_time - start_time < repeated_push_delay

    # Get the objects again and make sure they get transferred.
    xs = ray.get(x_ids)
    end_transfer_time = time.time()
    # We should have had to wait for the repeated push delay.
    assert end_transfer_time - start_time >= repeated_push_delay

    # Flush the objects again and wait longer than the repeated push delay and
    # make sure that the objects are transferred again.
    del xs
    for _ in range(15):
        ray.put(x)
    assert not any(
        ray.worker.global_worker.plasma_client.contains(
            ray.pyarrow.plasma.ObjectID(x_id.binary())) for x_id in x_ids)

    time.sleep(repeated_push_delay)

    # Get the objects locally to cause them to be transferred. This should
    # happen quickly.
    start_time = time.time()
    ray.get(x_ids)
    end_time = time.time()
    assert end_time - start_time < repeated_push_delay


# The purpose of this test is to make sure we can transfer many objects. In the
# past, this has caused failures in which object managers create too many open
# files and run out of resources.
def test_many_small_transfers(ray_start_cluster_with_resource):
    cluster, num_nodes = ray_start_cluster_with_resource

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
