import os
import time

import numpy as np
import pytest

import ray
from ray._private.internal_api import memory_summary
from ray._private.test_utils import Semaphore, wait_for_condition
from ray.cluster_utils import Cluster, cluster_not_supported

# RayConfig to enable recording call sites during ObjectRej creations.
ray_config = {"record_ref_creation_sites": True}

# Unique strings.
DRIVER_PID = "Driver"
WORKER_PID = "Worker"
UNKNOWN_SIZE = " ? "

# Reference status.
PINNED_IN_MEMORY = "PINNED_IN_MEMORY"
LOCAL_REF = "LOCAL_REFERENCE"
USED_BY_PENDING_TASK = "USED_BY_PENDING_TASK"
CAPTURED_IN_OBJECT = "CAPTURED_IN_OBJECT"
ACTOR_HANDLE = "ACTOR_HANDLE"

# Call sites.
PUT_OBJ = "(put object)"
TASK_CALL_OBJ = "(task call)"
ACTOR_TASK_CALL_OBJ = "(actor call)"
DESER_TASK_ARG = "(deserialize task arg)"
# Only 22 characters can be matched because longer strings are wrapped around.
DESER_ACTOR_TASK_ARG = "(deserialize actor tas"

# Group by and sort by parameters.
NODE_ADDRESS = "node address"
STACK_TRACE = "stack trace"
PID = "pid"
OBJECT_SIZE = "object size"
REFERENCE_TYPE = "reference type"

# Task status.
WAITING_FOR_DEPENDENCIES = "WAITING_FOR_DEPENDENCIES"
SCHEDULED = "SCHEDULED"
FINISHED = "FINISHED"
WAITING_FOR_EXECUTION = "WAITING_FOR_EXECUTION"


def data_lines(memory_str):
    for line in memory_str.split("\n"):
        if (
            PINNED_IN_MEMORY in line
            or LOCAL_REF in line
            or USED_BY_PENDING_TASK in line
            or CAPTURED_IN_OBJECT in line
            or ACTOR_HANDLE in line
        ):
            yield line
        else:
            continue


def num_objects(memory_str):
    n = 0
    for line in data_lines(memory_str):
        n += 1
    return n


def count(memory_str, substr):
    substr = substr[:42]
    n = 0
    for line in memory_str.split("\n"):
        if substr in line:
            n += 1
    return n


@pytest.mark.parametrize(
    "ray_start_regular", [{"_system_config": ray_config}], indirect=True
)
def test_driver_put_ref(ray_start_regular):
    address = ray_start_regular["address"]
    info = memory_summary(address)
    assert num_objects(info) == 0, info
    x_id = ray.put("HI")
    info = memory_summary(address)
    print(info)
    assert num_objects(info) == 1, info
    assert count(info, DRIVER_PID) == 1, info
    assert count(info, WORKER_PID) == 0, info
    del x_id
    info = memory_summary(address)
    assert num_objects(info) == 0, info


@pytest.mark.parametrize(
    "ray_start_regular", [{"_system_config": ray_config}], indirect=True
)
def test_worker_task_refs(ray_start_regular):
    address = ray_start_regular["address"]

    @ray.remote
    def f(y):
        from ray._private.internal_api import memory_summary

        x_id = ray.put("HI")
        info = memory_summary(address)
        del x_id
        return info

    x_id = f.remote(np.zeros(100000))
    info = ray.get(x_id)
    print(info)
    assert num_objects(info) == 4, info
    # Task argument plus task return ids.
    assert count(info, TASK_CALL_OBJ) == 2, info
    assert count(info, DRIVER_PID) == 2, info
    assert count(info, WORKER_PID) == 2, info
    assert count(info, LOCAL_REF) == 2, info
    assert count(info, PINNED_IN_MEMORY) == 1, info
    assert count(info, PUT_OBJ) == 1, info
    assert count(info, DESER_TASK_ARG) == 1, info
    assert count(info, UNKNOWN_SIZE) == 1, info

    print(ray_start_regular)
    info = memory_summary(address)
    print(info)
    assert num_objects(info) == 1, info
    assert count(info, DRIVER_PID) == 1, info
    assert count(info, TASK_CALL_OBJ) == 1, info
    assert count(info, UNKNOWN_SIZE) == 0, info
    assert count(info, x_id.hex()) == 1, info

    del x_id
    info = memory_summary(address)
    assert num_objects(info) == 0, info


@pytest.mark.parametrize(
    "ray_start_regular", [{"_system_config": ray_config}], indirect=True
)
def test_actor_task_refs(ray_start_regular):
    address = ray_start_regular["address"]

    @ray.remote
    class Actor:
        def __init__(self):
            self.refs = []

        def f(self, x):
            from ray._private.internal_api import memory_summary

            self.refs.append(x)
            return memory_summary(address)

    def make_actor():
        return Actor.remote()

    actor = make_actor()
    x_id = actor.f.remote(np.zeros(100000))
    info = ray.get(x_id)
    print(info)
    # Note, the actor will always hold a handle to the actor itself.
    assert num_objects(info) == 5, info
    # Actor handle, task argument id, task return id.
    assert count(info, ACTOR_TASK_CALL_OBJ) == 3, info
    assert count(info, DRIVER_PID) == 3, info
    assert count(info, WORKER_PID) == 2, info
    assert count(info, LOCAL_REF) == 1, info
    assert count(info, PINNED_IN_MEMORY) == 1, info
    assert count(info, USED_BY_PENDING_TASK) == 1, info
    assert count(info, ACTOR_HANDLE) == 2, info
    assert count(info, DESER_ACTOR_TASK_ARG) == 1, info
    del x_id

    # These should accumulate in the actor.
    for _ in range(5):
        ray.get(actor.f.remote([ray.put(np.zeros(100000))]))
    info = memory_summary(address)
    print(info)
    assert count(info, DESER_ACTOR_TASK_ARG) == 5, info
    assert count(info, ACTOR_TASK_CALL_OBJ) == 1, info

    # Cleanup.
    del actor
    time.sleep(1)
    info = memory_summary(address)
    assert num_objects(info) == 0, info


@pytest.mark.parametrize(
    "ray_start_regular", [{"_system_config": ray_config}], indirect=True
)
def test_nested_object_refs(ray_start_regular):
    address = ray_start_regular["address"]
    x_id = ray.put(np.zeros(100000))
    y_id = ray.put([x_id])
    z_id = ray.put([y_id])
    del x_id, y_id
    info = memory_summary(address)
    print(info)
    assert num_objects(info) == 3, info
    assert count(info, LOCAL_REF) == 1, info
    assert count(info, CAPTURED_IN_OBJECT) == 2, info
    del z_id


@pytest.mark.parametrize(
    "ray_start_regular", [{"_system_config": ray_config}], indirect=True
)
def test_pinned_object_call_site(ray_start_regular):
    address = ray_start_regular["address"]
    # Local ref only.
    x_id = ray.put(np.zeros(100000))
    info = memory_summary(address)
    print(info)
    assert num_objects(info) == 1, info
    assert count(info, LOCAL_REF) == 1, info
    assert count(info, PINNED_IN_MEMORY) == 0, info

    # Local ref + pinned buffer.
    buf = ray.get(x_id)
    info = memory_summary(address)
    print(info)
    assert num_objects(info) == 1, info
    assert count(info, LOCAL_REF) == 0, info
    assert count(info, PINNED_IN_MEMORY) == 1, info

    # Just pinned buffer.
    del x_id
    info = memory_summary(address)
    print(info)
    assert num_objects(info) == 1, info
    assert count(info, LOCAL_REF) == 0, info
    assert count(info, PINNED_IN_MEMORY) == 1, info

    # Nothing.
    del buf
    info = memory_summary(address)
    print(info)
    assert num_objects(info) == 0, info


@pytest.mark.xfail(cluster_not_supported, reason="cluster not supported")
def test_multi_node_stats(shutdown_only):
    # NOTE(mwtian): using env var only enables the feature on workers, while
    # using head_node_args={"_system_config": ray_config} only enables the
    # feature on the driver.
    os.environ["RAY_record_ref_creation_sites"] = "1"
    cluster = Cluster()
    for _ in range(2):
        cluster.add_node(num_cpus=1)

    ray.init(address=cluster.address)

    @ray.remote(num_cpus=1)
    class Actor:
        def __init__(self):
            self.ref = ray.put(np.zeros(100000))

        def ping(self):
            pass

    # Each actor will be on a different node.
    a = Actor.remote()
    b = Actor.remote()
    ray.get(a.ping.remote())
    ray.get(b.ping.remote())

    # Verify we have collected stats across the nodes.
    info = memory_summary(cluster.address)
    print(info)
    assert count(info, PUT_OBJ) == 2, info


@pytest.mark.parametrize(
    "ray_start_regular", [{"_system_config": ray_config}], indirect=True
)
def test_group_by_sort_by(ray_start_regular):
    address = ray_start_regular["address"]

    @ray.remote
    def f(y):
        x_id = ray.put("HI")
        info_a = memory_summary(
            address, group_by="STACK_TRACE", sort_by="REFERENCE_TYPE"
        )
        info_b = memory_summary(address, group_by="NODE_ADDRESS", sort_by="OBJECT_SIZE")
        info_c = memory_summary(address, group_by="NODE_ADDRESS", sort_by="PID")
        del x_id
        return info_a, info_b, info_c

    x_id = f.remote(np.zeros(100000))
    info_a, info_b, info_c = ray.get(x_id)
    print(info_c)
    assert count(info_a, STACK_TRACE) == 7, info_a
    assert count(info_a, REFERENCE_TYPE) == 1, info_a
    assert count(info_b, NODE_ADDRESS) == 3, info_b
    assert count(info_b, OBJECT_SIZE) == 1, info_b
    assert count(info_c, NODE_ADDRESS) == 3, info_c
    assert count(info_c, PID) == 1, info_c


@pytest.mark.parametrize(
    "ray_start_regular", [{"_system_config": ray_config}], indirect=True
)
def test_memory_used_output(ray_start_regular):
    address = ray_start_regular["address"]
    import numpy as np

    _ = ray.put(np.ones(8 * 1024 * 1024, dtype=np.int8))

    info = memory_summary(address)
    print(info)
    assert count(info, "Plasma memory usage 8 MiB") == 1, info
    assert count(info, "8388861.0 B") == 2, info


def test_task_status(ray_start_regular):
    address = ray_start_regular["address"]

    @ray.remote
    def dep(sema, x=None):
        ray.get(sema.acquire.remote())
        return

    @ray.remote(num_gpus=1)
    def impossible():
        pass

    # Filter out actor handle refs.
    def filtered_summary():
        data = "\n".join(
            [
                line
                for line in memory_summary(address, line_wrap=False).split("\n")
                if "ACTOR_HANDLE" not in line
            ]
        )
        print(data)
        return data

    sema = Semaphore.remote(value=0)
    x = dep.remote(sema)
    y = dep.remote(sema, x=x)
    im = impossible.remote()  # noqa
    # x and its semaphore task are scheduled. im cannot
    # be scheduled, so it is pending forever.
    wait_for_condition(lambda: count(filtered_summary(), WAITING_FOR_EXECUTION) == 2)
    wait_for_condition(lambda: count(filtered_summary(), WAITING_FOR_DEPENDENCIES) == 1)
    wait_for_condition(lambda: count(filtered_summary(), SCHEDULED) == 1)

    z = dep.remote(sema, x=x)
    wait_for_condition(lambda: count(filtered_summary(), WAITING_FOR_DEPENDENCIES) == 2)
    wait_for_condition(lambda: count(filtered_summary(), WAITING_FOR_EXECUTION) == 2)
    wait_for_condition(lambda: count(filtered_summary(), FINISHED) == 0)

    sema.release.remote()
    time.sleep(2)
    wait_for_condition(lambda: count(filtered_summary(), FINISHED) == 1)
    wait_for_condition(lambda: count(filtered_summary(), WAITING_FOR_DEPENDENCIES) == 0)
    # y, z, and two semaphore tasks are scheduled.
    wait_for_condition(lambda: count(filtered_summary(), WAITING_FOR_EXECUTION) == 4)

    sema.release.remote()
    wait_for_condition(lambda: count(filtered_summary(), FINISHED) == 2)
    wait_for_condition(lambda: count(filtered_summary(), WAITING_FOR_DEPENDENCIES) == 0)
    wait_for_condition(lambda: count(filtered_summary(), WAITING_FOR_EXECUTION) == 2)

    sema.release.remote()
    ray.get(y)
    ray.get(z)
    wait_for_condition(lambda: count(filtered_summary(), FINISHED) == 3)
    wait_for_condition(lambda: count(filtered_summary(), WAITING_FOR_DEPENDENCIES) == 0)
    wait_for_condition(lambda: count(filtered_summary(), WAITING_FOR_EXECUTION) == 0)
    wait_for_condition(lambda: count(filtered_summary(), SCHEDULED) == 1)


if __name__ == "__main__":
    import sys

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
