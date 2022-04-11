# coding: utf-8
import json
import logging
import sys
import time

import os
import numpy as np
import pytest

import ray.cluster_utils

import ray._private.profiling as profiling
from ray._private.test_utils import (
    client_test_enabled,
    RayTestTimeoutException,
)
from ray.exceptions import ReferenceCountingAssertionError

if client_test_enabled():
    from ray.util.client import ray
else:
    import ray

logger = logging.getLogger(__name__)


# issue https://github.com/ray-project/ray/issues/7105
@pytest.mark.skipif(client_test_enabled(), reason="internal api")
def test_internal_free(shutdown_only):
    ray.init(num_cpus=1)

    @ray.remote
    class Sampler:
        def sample(self):
            return [1, 2, 3, 4, 5]

        def sample_big(self):
            return np.zeros(1024 * 1024)

    sampler = Sampler.remote()

    # Free deletes from in-memory store.
    obj_ref = sampler.sample.remote()
    ray.get(obj_ref)
    ray.internal.free(obj_ref)
    with pytest.raises(ReferenceCountingAssertionError):
        ray.get(obj_ref)

    # Free deletes big objects from plasma store.
    big_id = sampler.sample_big.remote()
    ray.get(big_id)
    ray.internal.free(big_id)
    time.sleep(1)  # wait for delete RPC to propagate
    with pytest.raises(ReferenceCountingAssertionError):
        ray.get(big_id)


def test_multiple_waits_and_gets(shutdown_only):
    # It is important to use three workers here, so that the three tasks
    # launched in this experiment can run at the same time.
    ray.init(num_cpus=3)

    @ray.remote
    def f(delay):
        time.sleep(delay)
        return 1

    @ray.remote
    def g(input_list):
        # The argument input_list should be a list containing one object ref.
        ray.wait([input_list[0]])

    @ray.remote
    def h(input_list):
        # The argument input_list should be a list containing one object ref.
        ray.get(input_list[0])

    # Make sure that multiple wait requests involving the same object ref
    # all return.
    x = f.remote(1)
    ray.get([g.remote([x]), g.remote([x])])

    # Make sure that multiple get requests involving the same object ref all
    # return.
    x = f.remote(1)
    ray.get([h.remote([x]), h.remote([x])])


@pytest.mark.skipif(client_test_enabled(), reason="internal api")
def test_caching_functions_to_run(shutdown_only):
    # Test that we export functions to run on all workers before the driver
    # is connected.
    def f(worker_info):
        sys.path.append(1)

    ray.worker.global_worker.run_function_on_all_workers(f)

    def f(worker_info):
        sys.path.append(2)

    ray.worker.global_worker.run_function_on_all_workers(f)

    def g(worker_info):
        sys.path.append(3)

    ray.worker.global_worker.run_function_on_all_workers(g)

    def f(worker_info):
        sys.path.append(4)

    ray.worker.global_worker.run_function_on_all_workers(f)

    ray.init(num_cpus=1)

    @ray.remote
    def get_state():
        time.sleep(1)
        return sys.path[-4], sys.path[-3], sys.path[-2], sys.path[-1]

    res1 = get_state.remote()
    res2 = get_state.remote()
    assert ray.get(res1) == (1, 2, 3, 4)
    assert ray.get(res2) == (1, 2, 3, 4)

    # Clean up the path on the workers.
    def f(worker_info):
        sys.path.pop()
        sys.path.pop()
        sys.path.pop()
        sys.path.pop()

    ray.worker.global_worker.run_function_on_all_workers(f)


@pytest.mark.skipif(client_test_enabled(), reason="internal api")
def test_running_function_on_all_workers(ray_start_regular):
    def f(worker_info):
        sys.path.append("fake_directory")

    ray.worker.global_worker.run_function_on_all_workers(f)

    @ray.remote
    def get_path1():
        return sys.path

    assert "fake_directory" == ray.get(get_path1.remote())[-1]

    # the function should only run on the current driver once.
    assert sys.path[-1] == "fake_directory"
    if len(sys.path) > 1:
        assert sys.path[-2] != "fake_directory"

    def f(worker_info):
        sys.path.pop(-1)

    ray.worker.global_worker.run_function_on_all_workers(f)

    # Create a second remote function to guarantee that when we call
    # get_path2.remote(), the second function to run will have been run on
    # the worker.
    @ray.remote
    def get_path2():
        return sys.path

    assert "fake_directory" not in ray.get(get_path2.remote())


@pytest.mark.skipif(
    "RAY_PROFILING" not in os.environ, reason="Only tested in client/profiling build."
)
@pytest.mark.skipif(
    client_test_enabled(),
    reason=(
        "wait_for_function will miss in this mode. To be fixed after using"
        " gcs to bootstrap all component."
    ),
)
def test_profiling_api(ray_start_2_cpus):
    @ray.remote
    def f(delay):
        with profiling.profile("custom_event", extra_data={"name": "custom name"}):
            time.sleep(delay)
            pass

    @ray.remote
    def g(input_list):
        # The argument input_list should be a list containing one object ref.
        ray.wait([input_list[0]])

    ray.put(1)
    x = f.remote(1)
    ray.get([g.remote([x]), g.remote([x])])

    # Wait until all of the profiling information appears in the profile
    # table.
    timeout_seconds = 20
    start_time = time.time()
    while True:
        profile_data = ray.timeline()
        event_types = {event["cat"] for event in profile_data}
        expected_types = [
            "task",
            "task:deserialize_arguments",
            "task:execute",
            "task:store_outputs",
            "wait_for_function",
            "ray.get",
            "ray.put",
            "ray.wait",
            "submit_task",
            "fetch_and_run_function",
            # TODO (Alex) :https://github.com/ray-project/ray/pull/9346
            # "register_remote_function",
            "custom_event",  # This is the custom one from ray.profile.
        ]

        if all(expected_type in event_types for expected_type in expected_types):
            break

        if time.time() - start_time > timeout_seconds:
            raise RayTestTimeoutException(
                "Timed out while waiting for information in "
                "profile table. Missing events: {}.".format(
                    set(expected_types) - set(event_types)
                )
            )

        # The profiling information only flushes once every second.
        time.sleep(1.1)


def test_wait_cluster(ray_start_cluster_enabled):
    cluster = ray_start_cluster_enabled
    cluster.add_node(num_cpus=1, resources={"RemoteResource": 1})
    cluster.add_node(num_cpus=1, resources={"RemoteResource": 1})
    ray.init(address=cluster.address)

    @ray.remote(resources={"RemoteResource": 1})
    def f():
        return

    # Make sure we have enough workers on the remote nodes to execute some
    # tasks.
    tasks = [f.remote() for _ in range(10)]
    start = time.time()
    ray.get(tasks)
    end = time.time()

    # Submit some more tasks that can only be executed on the remote nodes.
    tasks = [f.remote() for _ in range(10)]
    # Sleep for a bit to let the tasks finish.
    time.sleep((end - start) * 2)
    _, unready = ray.wait(tasks, num_returns=len(tasks), timeout=0)
    # All remote tasks should have finished.
    assert len(unready) == 0


@pytest.mark.skip(reason="TODO(ekl)")
def test_object_transfer_dump(ray_start_cluster_enabled):
    cluster = ray_start_cluster_enabled

    num_nodes = 3
    for i in range(num_nodes):
        cluster.add_node(resources={str(i): 1}, object_store_memory=10 ** 9)
    ray.init(address=cluster.address)

    @ray.remote
    def f(x):
        return

    # These objects will live on different nodes.
    object_refs = [f._remote(args=[1], resources={str(i): 1}) for i in range(num_nodes)]

    # Broadcast each object from each machine to each other machine.
    for object_ref in object_refs:
        ray.get(
            [
                f._remote(args=[object_ref], resources={str(i): 1})
                for i in range(num_nodes)
            ]
        )

    # The profiling information only flushes once every second.
    time.sleep(1.1)

    transfer_dump = ray.state.object_transfer_timeline()
    # Make sure the transfer dump can be serialized with JSON.
    json.loads(json.dumps(transfer_dump))
    assert len(transfer_dump) >= num_nodes ** 2
    assert (
        len(
            {
                event["pid"]
                for event in transfer_dump
                if event["name"] == "transfer_receive"
            }
        )
        == num_nodes
    )
    assert (
        len(
            {
                event["pid"]
                for event in transfer_dump
                if event["name"] == "transfer_send"
            }
        )
        == num_nodes
    )


def test_identical_function_names(ray_start_regular):
    # Define a bunch of remote functions and make sure that we don't
    # accidentally call an older version.

    num_calls = 200

    @ray.remote
    def f():
        return 1

    results1 = [f.remote() for _ in range(num_calls)]

    @ray.remote
    def f():
        return 2

    results2 = [f.remote() for _ in range(num_calls)]

    @ray.remote
    def f():
        return 3

    results3 = [f.remote() for _ in range(num_calls)]

    @ray.remote
    def f():
        return 4

    results4 = [f.remote() for _ in range(num_calls)]

    @ray.remote
    def f():
        return 5

    results5 = [f.remote() for _ in range(num_calls)]

    assert ray.get(results1) == num_calls * [1]
    assert ray.get(results2) == num_calls * [2]
    assert ray.get(results3) == num_calls * [3]
    assert ray.get(results4) == num_calls * [4]
    assert ray.get(results5) == num_calls * [5]

    @ray.remote
    def g():
        return 1

    @ray.remote  # noqa: F811
    def g():  # noqa: F811
        return 2

    @ray.remote  # noqa: F811
    def g():  # noqa: F811
        return 3

    @ray.remote  # noqa: F811
    def g():  # noqa: F811
        return 4

    @ray.remote  # noqa: F811
    def g():  # noqa: F811
        return 5

    result_values = ray.get([g.remote() for _ in range(num_calls)])
    assert result_values == num_calls * [5]


def test_illegal_api_calls(ray_start_regular):

    # Verify that we cannot call put on an ObjectRef.
    x = ray.put(1)
    with pytest.raises(Exception):
        ray.put(x)
    # Verify that we cannot call get on a regular value.
    with pytest.raises(Exception):
        ray.get(3)


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
