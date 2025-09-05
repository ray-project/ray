# coding: utf-8
import json
import logging
import os
import sys
import time

import numpy as np
import pytest

import ray._private.profiling as profiling
import ray.cluster_utils
from ray._common.test_utils import wait_for_condition
from ray._private.internal_api import (
    get_local_ongoing_lineage_reconstruction_tasks,
    memory_summary,
)
from ray._private.test_utils import (
    client_test_enabled,
)
from ray.core.generated import common_pb2
from ray.exceptions import ObjectFreedError

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
    ray._private.internal_api.free(obj_ref)
    with pytest.raises(ObjectFreedError):
        ray.get(obj_ref)

    # Free deletes big objects from plasma store.
    big_id = sampler.sample_big.remote()
    ray.get(big_id)
    ray._private.internal_api.free(big_id)
    with pytest.raises(ObjectFreedError):
        ray.get(big_id)


@pytest.mark.skipif(client_test_enabled(), reason="internal api")
def test_internal_free_non_owned(shutdown_only):
    info = ray.init(num_cpus=1)

    @ray.remote
    def gen_data():
        return ray.put(np.zeros(1024 * 1024))

    @ray.remote
    def do_free(ref_list):
        ray._private.internal_api.free(ref_list, local_only=False)
        for ref in ref_list:
            with pytest.raises(ObjectFreedError):
                ray.get(ref)

    # Can free locally owned objects from remote worker.
    ref_1 = ray.put(np.zeros(1024 * 1024))
    ref_2 = ray.put(np.zeros(1024 * 1024))
    ray.get(do_free.remote([ref_1, ref_2]))

    # Can free remotely owned objects from local worker.
    ref_3 = ray.get(gen_data.remote())
    ref_4 = ray.get(gen_data.remote())
    ray._private.internal_api.free([ref_3, ref_4], local_only=False)
    for ref in [ref_3, ref_4]:
        with pytest.raises(ObjectFreedError):
            ray.get(ref)

    # Memory was really freed.
    info = memory_summary(info.address_info["address"])
    assert "Plasma memory usage 0 MiB, 0 objects" in info, info


@pytest.mark.skipif(client_test_enabled(), reason="internal api")
def test_internal_free_edge_case(shutdown_only):
    ray.init(
        num_cpus=1,
        _system_config={
            "fetch_fail_timeout_milliseconds": 200,
        },
    )

    @ray.remote
    def gen():
        return ray.put(np.ones(1024 * 1024 * 100))

    @ray.remote
    def free(x):
        ray._private.internal_api.free(x[0], local_only=False)

    x = ray.get(gen.remote())
    ray.get(x)
    ray.get(free.remote([x]))

    # This currently hangs, since as a borrower we never subscribe for
    # object deletion events. Check that we at least hit the fetch timeout.
    with pytest.raises(ray.exceptions.ObjectFetchTimedOutError):
        ray.get(x)


@pytest.mark.skipif(client_test_enabled(), reason="internal api")
def test_internal_get_local_ongoing_lineage_reconstruction_tasks(
    ray_start_cluster_enabled,
):
    cluster = ray_start_cluster_enabled
    cluster.add_node(resources={"head": 2})
    ray.init(address=cluster.address)
    worker1 = cluster.add_node(resources={"worker": 2})

    @ray.remote(num_cpus=0, resources={"head": 1})
    class Counter:
        def __init__(self):
            self.count = 0

        def inc(self):
            self.count = self.count + 1
            return self.count

    @ray.remote(
        max_retries=-1, num_cpus=0, resources={"worker": 1}, _labels={"key1": "value1"}
    )
    def task(counter):
        count = ray.get(counter.inc.remote())
        if count > 1:
            # lineage reconstruction
            time.sleep(100000)
        return [1] * 1024 * 1024

    @ray.remote(
        max_restarts=-1,
        max_task_retries=-1,
        num_cpus=0,
        resources={"worker": 1},
        _labels={"key2": "value2"},
    )
    class Actor:
        def run(self, counter):
            count = ray.get(counter.inc.remote())
            if count > 1:
                # lineage reconstruction
                time.sleep(100000)
            return [1] * 1024 * 1024

    counter1 = Counter.remote()
    obj1 = task.remote(counter1)
    # Wait for task to finish
    ray.wait([obj1], fetch_local=False)

    counter2 = Counter.remote()
    actor = Actor.remote()
    obj2 = actor.run.remote(counter2)
    # Wait for actor task to finish
    ray.wait([obj2], fetch_local=False)

    assert len(get_local_ongoing_lineage_reconstruction_tasks()) == 0

    # Trigger lineage reconstruction of obj
    cluster.remove_node(worker1)

    def verify(expected_task_status):
        lineage_reconstruction_tasks = get_local_ongoing_lineage_reconstruction_tasks()
        lineage_reconstruction_tasks.sort(key=lambda task: task[0].name)
        assert len(lineage_reconstruction_tasks) == 2
        assert [
            lineage_reconstruction_tasks[0][0].name,
            lineage_reconstruction_tasks[1][0].name,
        ] == ["Actor.run", "task"]
        assert (
            lineage_reconstruction_tasks[0][0].labels == {"key2": "value2"}
            and lineage_reconstruction_tasks[0][0].status == expected_task_status
            and lineage_reconstruction_tasks[0][1] == 1
        )
        assert (
            lineage_reconstruction_tasks[1][0].labels == {"key1": "value1"}
            and lineage_reconstruction_tasks[1][0].status == expected_task_status
            and lineage_reconstruction_tasks[1][1] == 1
        )

        return True

    wait_for_condition(lambda: verify(common_pb2.TaskStatus.PENDING_NODE_ASSIGNMENT))
    cluster.add_node(resources={"worker": 2})
    wait_for_condition(lambda: verify(common_pb2.TaskStatus.SUBMITTED_TO_WORKER))


def test_multiple_waits_and_gets(shutdown_only):
    # It is important to use three workers here, so that the three tasks
    # launched in this experiment can run at the same time.
    ray.init(num_cpus=3)

    @ray.remote
    def f():
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
    x = f.remote()
    ray.get([g.remote([x]), g.remote([x])])

    # Make sure that multiple get requests involving the same object ref all
    # return.
    x = f.remote()
    ray.get([h.remote([x]), h.remote([x])])


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
def test_profiling_api(shutdown_only):

    ray.init(
        num_cpus=2,
        _system_config={
            "task_events_report_interval_ms": 200,
            "enable_timeline": True,
        },
    )

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

    def verify():
        profile_data = ray.timeline()
        actual_types = {event["cat"] for event in profile_data}
        expected_types = {
            "task::f",  # for f
            "task::g",  # for g
            "task:deserialize_arguments",
            "task:execute",
            "task:store_outputs",
            "wait_for_function",
            "ray.get",
            "ray.put",
            "ray.wait",
            "submit_task",
            "fetch_and_run_function",
            "custom_event",  # This is the custom one from ray.profile.
        }
        assert expected_types == actual_types
        return True

    wait_for_condition(verify, timeout=20, retry_interval_ms=1000)

    # Test for content of the profiling events.
    @ray.remote
    def k():
        exec_time_us = time.time() * (10**6)
        worker_id = ray._private.worker.global_worker.core_worker.get_worker_id().hex()
        return worker_id, exec_time_us

    k_worker_id, k_exec_time_us = ray.get(k.remote())

    def verify():
        profile_data = ray.timeline()
        k_events = [
            event for event in profile_data if event["tid"] == f"worker:{k_worker_id}"
        ]
        assert len(k_events) > 0
        for event in k_events:
            if event["name"] == "task:execute":
                reported_exec_time = event["ts"]
                # diff smaller than 3 secs, a fine-tuned threshold from running locally.
                assert abs(reported_exec_time - k_exec_time_us) < 3 * (10**6)

        return True

    wait_for_condition(verify, timeout=20, retry_interval_ms=1000)


def test_wait_cluster(ray_start_cluster_enabled):
    cluster = ray_start_cluster_enabled
    cluster.add_node(num_cpus=1, resources={"RemoteResource": 1})
    cluster.add_node(num_cpus=1, resources={"RemoteResource": 1})
    ray.init(address=cluster.address)

    @ray.remote(resources={"RemoteResource": 1})
    def f():
        return

    # Submit some more tasks that can only be executed on the remote nodes.
    tasks = [f.remote() for _ in range(10)]
    # Wait for all tasks to finish.
    _, _ = ray.wait(tasks, num_returns=len(tasks), fetch_local=False)
    # Make sure a wait with 0 timeout works.
    _, unready = ray.wait(tasks, num_returns=len(tasks), timeout=0)
    # All remote tasks should have finished.
    assert len(unready) == 0


@pytest.mark.skip(reason="TODO(ekl)")
def test_object_transfer_dump(ray_start_cluster_enabled):
    cluster = ray_start_cluster_enabled

    num_nodes = 3
    for i in range(num_nodes):
        cluster.add_node(resources={str(i): 1}, object_store_memory=10**9)
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

    transfer_dump = ray._private.state.object_transfer_timeline()
    # Make sure the transfer dump can be serialized with JSON.
    json.loads(json.dumps(transfer_dump))
    assert len(transfer_dump) >= num_nodes**2
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
    sys.exit(pytest.main(["-sv", __file__]))
