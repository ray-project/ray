import os
import sys
import time

import numpy as np
import pytest

import ray
import ray._private.ray_constants as ray_constants
from ray._private.internal_api import memory_summary
from ray._private.test_utils import Semaphore, SignalActor, wait_for_condition

# Task status.
WAITING_FOR_DEPENDENCIES = "PENDING_ARGS_AVAIL"
SCHEDULED = "PENDING_NODE_ASSIGNMENT"
FINISHED = "FINISHED"
WAITING_FOR_EXECUTION = "SUBMITTED_TO_WORKER"


@pytest.fixture(params=[True, False])
def config(request):
    pull_based = request.param
    if pull_based:
        config = {
            "health_check_initial_delay_ms": 0,
            "health_check_period_ms": 100,
            "health_check_failure_threshold": 10,
            "object_timeout_milliseconds": 200,
            "pull_based_healthcheck": True,
        }
    else:
        config = {
            "num_heartbeats_timeout": 10,
            "raylet_heartbeat_period_milliseconds": 100,
            "pull_based_healthcheck": False,
            "object_timeout_milliseconds": 200,
        }
    yield config


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
@pytest.mark.parametrize("reconstruction_enabled", [False, True])
def test_nondeterministic_output(config, ray_start_cluster, reconstruction_enabled):
    config["max_direct_call_object_size"] = 100
    config["task_retry_delay_ms"] = 100
    config["object_timeout_milliseconds"] = 200

    cluster = ray_start_cluster
    # Head node with no resources.
    cluster.add_node(
        num_cpus=0, _system_config=config, enable_object_reconstruction=True
    )
    ray.init(address=cluster.address)
    # Node to place the initial object.
    node_to_kill = cluster.add_node(
        num_cpus=1, resources={"node1": 1}, object_store_memory=10**8
    )
    cluster.add_node(num_cpus=1, object_store_memory=10**8)
    cluster.wait_for_nodes()

    @ray.remote
    def nondeterministic_object():
        if np.random.rand() < 0.5:
            return np.zeros(10**5, dtype=np.uint8)
        else:
            return 0

    @ray.remote
    def dependent_task(x):
        return

    for _ in range(10):
        obj = nondeterministic_object.options(resources={"node1": 1}).remote()
        for _ in range(3):
            ray.get(dependent_task.remote(obj))
            x = dependent_task.remote(obj)
            cluster.remove_node(node_to_kill, allow_graceful=False)
            node_to_kill = cluster.add_node(
                num_cpus=1, resources={"node1": 1}, object_store_memory=10**8
            )
            ray.get(x)


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
def test_reconstruction_hangs(config, ray_start_cluster):
    config["max_direct_call_object_size"] = 100
    config["task_retry_delay_ms"] = 100
    config["object_timeout_milliseconds"] = 200
    config["fetch_warn_timeout_milliseconds"] = 1000

    cluster = ray_start_cluster
    # Head node with no resources.
    cluster.add_node(
        num_cpus=0, _system_config=config, enable_object_reconstruction=True
    )
    ray.init(address=cluster.address)
    # Node to place the initial object.
    node_to_kill = cluster.add_node(
        num_cpus=1, resources={"node1": 1}, object_store_memory=10**8
    )
    cluster.add_node(num_cpus=1, object_store_memory=10**8)
    cluster.wait_for_nodes()

    @ray.remote
    def sleep():
        # Task takes longer than the reconstruction timeout.
        time.sleep(3)
        return np.zeros(10**5, dtype=np.uint8)

    @ray.remote
    def dependent_task(x):
        return

    obj = sleep.options(resources={"node1": 1}).remote()
    for _ in range(3):
        ray.get(dependent_task.remote(obj))
        x = dependent_task.remote(obj)
        cluster.remove_node(node_to_kill, allow_graceful=False)
        node_to_kill = cluster.add_node(
            num_cpus=1, resources={"node1": 1}, object_store_memory=10**8
        )
        ray.get(x)


def test_lineage_evicted(config, ray_start_cluster):
    config["max_lineage_bytes"] = 10_000

    cluster = ray_start_cluster
    # Head node with no resources.
    cluster.add_node(
        num_cpus=0,
        _system_config=config,
        object_store_memory=10**8,
        enable_object_reconstruction=True,
    )
    ray.init(address=cluster.address)
    node_to_kill = cluster.add_node(num_cpus=1, object_store_memory=10**8)
    cluster.wait_for_nodes()

    @ray.remote
    def large_object():
        return np.zeros(10**7, dtype=np.uint8)

    @ray.remote
    def chain(x):
        return x

    @ray.remote
    def dependent_task(x):
        return x

    obj = large_object.remote()
    for _ in range(5):
        obj = chain.remote(obj)
    ray.get(dependent_task.remote(obj))

    cluster.remove_node(node_to_kill, allow_graceful=False)
    node_to_kill = cluster.add_node(num_cpus=1, object_store_memory=10**8)
    ray.get(dependent_task.remote(obj))

    # Lineage now exceeds the eviction factor.
    for _ in range(100):
        obj = chain.remote(obj)
    ray.get(dependent_task.remote(obj))

    cluster.remove_node(node_to_kill, allow_graceful=False)
    cluster.add_node(num_cpus=1, object_store_memory=10**8)
    try:
        ray.get(dependent_task.remote(obj))
        assert False
    except ray.exceptions.RayTaskError as e:
        assert "ObjectReconstructionFailedLineageEvictedError" in str(e)


@pytest.mark.parametrize("reconstruction_enabled", [False, True])
def test_multiple_returns(config, ray_start_cluster, reconstruction_enabled):
    # Workaround to reset the config to the default value.
    if not reconstruction_enabled:
        config["lineage_pinning_enabled"] = False

    cluster = ray_start_cluster
    # Head node with no resources.
    cluster.add_node(
        num_cpus=0,
        _system_config=config,
        enable_object_reconstruction=reconstruction_enabled,
    )
    ray.init(address=cluster.address)
    # Node to place the initial object.
    node_to_kill = cluster.add_node(num_cpus=1, object_store_memory=10**8)
    cluster.wait_for_nodes()

    @ray.remote(num_returns=2)
    def two_large_objects():
        return (np.zeros(10**7, dtype=np.uint8), np.zeros(10**7, dtype=np.uint8))

    @ray.remote
    def dependent_task(x):
        return

    obj1, obj2 = two_large_objects.remote()
    ray.get(dependent_task.remote(obj1))
    cluster.add_node(num_cpus=1, resources={"node": 1}, object_store_memory=10**8)
    ray.get(dependent_task.options(resources={"node": 1}).remote(obj1))

    cluster.remove_node(node_to_kill, allow_graceful=False)
    wait_for_condition(
        lambda: not all(node["Alive"] for node in ray.nodes()), timeout=10
    )

    if reconstruction_enabled:
        ray.get(dependent_task.remote(obj1))
        ray.get(dependent_task.remote(obj2))
    else:
        with pytest.raises(ray.exceptions.RayTaskError):
            ray.get(dependent_task.remote(obj1))
            ray.get(dependent_task.remote(obj2))
        with pytest.raises(ray.exceptions.ObjectLostError):
            ray.get(obj2)


@pytest.mark.parametrize("reconstruction_enabled", [False, True])
def test_nested(config, ray_start_cluster, reconstruction_enabled):
    config["fetch_fail_timeout_milliseconds"] = 10_000
    # Workaround to reset the config to the default value.
    if not reconstruction_enabled:
        config["lineage_pinning_enabled"] = False

    cluster = ray_start_cluster
    # Head node with no resources.
    cluster.add_node(
        num_cpus=0,
        _system_config=config,
        enable_object_reconstruction=reconstruction_enabled,
    )
    ray.init(address=cluster.address)
    done_signal = SignalActor.remote()
    exit_signal = SignalActor.remote()
    ray.get(done_signal.wait.remote(should_wait=False))
    ray.get(exit_signal.wait.remote(should_wait=False))

    # Node to place the initial object.
    node_to_kill = cluster.add_node(num_cpus=1, object_store_memory=10**8)
    cluster.wait_for_nodes()

    @ray.remote
    def dependent_task(x):
        return

    @ray.remote
    def large_object():
        return np.zeros(10**7, dtype=np.uint8)

    @ray.remote
    def nested(done_signal, exit_signal):
        ref = ray.put(np.zeros(10**7, dtype=np.uint8))
        # Flush object store.
        for _ in range(20):
            ray.put(np.zeros(10**7, dtype=np.uint8))
        dep = dependent_task.options(resources={"node": 1}).remote(ref)
        ray.get(done_signal.send.remote(clear=True))
        ray.get(dep)
        return ray.get(ref)

    ref = nested.remote(done_signal, exit_signal)
    # Wait for task to get scheduled on the node to kill.
    ray.get(done_signal.wait.remote())
    # Wait for ray.put object to get transferred to the other node.
    cluster.add_node(num_cpus=2, resources={"node": 10}, object_store_memory=10**8)
    ray.get(dependent_task.remote(ref))

    # Destroy the task's output.
    cluster.remove_node(node_to_kill, allow_graceful=False)
    wait_for_condition(
        lambda: not all(node["Alive"] for node in ray.nodes()), timeout=10
    )

    if reconstruction_enabled:
        ray.get(ref, timeout=60)
    else:
        with pytest.raises(ray.exceptions.ObjectLostError):
            ray.get(ref, timeout=60)


@pytest.mark.parametrize("reconstruction_enabled", [False, True])
def test_spilled(config, ray_start_cluster, reconstruction_enabled):
    # Workaround to reset the config to the default value.
    if not reconstruction_enabled:
        config["lineage_pinning_enabled"] = False

    cluster = ray_start_cluster
    # Head node with no resources.
    cluster.add_node(
        num_cpus=0,
        _system_config=config,
        enable_object_reconstruction=reconstruction_enabled,
    )
    ray.init(address=cluster.address)
    # Node to place the initial object.
    node_to_kill = cluster.add_node(
        num_cpus=1, resources={"node1": 1}, object_store_memory=10**8
    )
    cluster.wait_for_nodes()

    @ray.remote(max_retries=1 if reconstruction_enabled else 0)
    def large_object():
        return np.zeros(10**7, dtype=np.uint8)

    @ray.remote
    def dependent_task(x):
        return

    obj = large_object.options(resources={"node1": 1}).remote()
    ray.get(dependent_task.options(resources={"node1": 1}).remote(obj))
    # Force spilling.
    objs = [large_object.options(resources={"node1": 1}).remote() for _ in range(20)]
    for o in objs:
        ray.get(o)

    cluster.remove_node(node_to_kill, allow_graceful=False)
    node_to_kill = cluster.add_node(
        num_cpus=1, resources={"node1": 1}, object_store_memory=10**8
    )

    if reconstruction_enabled:
        ray.get(dependent_task.remote(obj), timeout=60)
    else:
        with pytest.raises(ray.exceptions.RayTaskError):
            ray.get(dependent_task.remote(obj), timeout=60)
        with pytest.raises(ray.exceptions.ObjectLostError):
            ray.get(obj, timeout=60)


def test_memory_util(config, ray_start_cluster):
    cluster = ray_start_cluster
    # Head node with no resources.
    cluster.add_node(
        num_cpus=0,
        resources={"head": 1},
        _system_config=config,
        enable_object_reconstruction=True,
    )
    ray.init(address=cluster.address)
    # Node to place the initial object.
    node_to_kill = cluster.add_node(
        num_cpus=1, resources={"node1": 1}, object_store_memory=10**8
    )
    cluster.wait_for_nodes()

    @ray.remote
    def large_object(sema=None):
        if sema is not None:
            ray.get(sema.acquire.remote())
        return np.zeros(10**7, dtype=np.uint8)

    @ray.remote
    def dependent_task(x, sema):
        ray.get(sema.acquire.remote())
        return x

    def stats():
        info = memory_summary(cluster.address, line_wrap=False)
        print(info)
        info = info.split("\n")
        reconstructing_waiting = [
            line
            for line in info
            if "Attempt #2" in line and WAITING_FOR_DEPENDENCIES in line
        ]
        reconstructing_scheduled = [
            line
            for line in info
            if "Attempt #2" in line and WAITING_FOR_EXECUTION in line
        ]
        reconstructing_finished = [
            line for line in info if "Attempt #2" in line and FINISHED in line
        ]
        return (
            len(reconstructing_waiting),
            len(reconstructing_scheduled),
            len(reconstructing_finished),
        )

    sema = Semaphore.options(resources={"head": 1}).remote(value=0)
    obj = large_object.options(resources={"node1": 1}).remote(sema)
    x = dependent_task.options(resources={"node1": 1}).remote(obj, sema)
    ref = dependent_task.options(resources={"node1": 1}).remote(x, sema)
    ray.get(sema.release.remote())
    ray.get(sema.release.remote())
    ray.get(sema.release.remote())
    ray.get(ref)
    wait_for_condition(lambda: stats() == (0, 0, 0))
    del ref

    cluster.remove_node(node_to_kill, allow_graceful=False)
    node_to_kill = cluster.add_node(
        num_cpus=1, resources={"node1": 1}, object_store_memory=10**8
    )

    ref = dependent_task.remote(x, sema)
    wait_for_condition(lambda: stats() == (1, 1, 0))
    ray.get(sema.release.remote())
    wait_for_condition(lambda: stats() == (0, 1, 1))
    ray.get(sema.release.remote())
    ray.get(sema.release.remote())
    ray.get(ref)
    wait_for_condition(lambda: stats() == (0, 0, 2))


@pytest.mark.parametrize("override_max_retries", [False, True])
def test_override_max_retries(ray_start_cluster, override_max_retries):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=1)
    max_retries = ray_constants.DEFAULT_TASK_MAX_RETRIES
    runtime_env = {}
    if override_max_retries:
        max_retries = 1
        runtime_env["env_vars"] = {"RAY_TASK_MAX_RETRIES": str(max_retries)}
        os.environ["RAY_TASK_MAX_RETRIES"] = str(max_retries)
    # Since we're setting the OS environment variable after the driver process
    # is already started, we need to set it a second time for the workers with
    # runtime_env.
    ray.init(cluster.address, runtime_env=runtime_env)

    try:

        @ray.remote
        class ExecutionCounter:
            def __init__(self):
                self.count = 0

            def inc(self):
                self.count += 1

            def pop(self):
                count = self.count
                self.count = 0
                return count

        @ray.remote
        def f(counter):
            ray.get(counter.inc.remote())
            sys.exit(-1)

        counter = ExecutionCounter.remote()
        with pytest.raises(ray.exceptions.WorkerCrashedError):
            ray.get(f.remote(counter))
        assert ray.get(counter.pop.remote()) == max_retries + 1

        # Check max_retries override still works.
        with pytest.raises(ray.exceptions.WorkerCrashedError):
            ray.get(f.options(max_retries=0).remote(counter))
        assert ray.get(counter.pop.remote()) == 1

        @ray.remote
        def nested(counter):
            ray.get(f.remote(counter))

        # Check override works through nested tasks.
        with pytest.raises(ray.exceptions.RayTaskError):
            ray.get(nested.remote(counter))
        assert ray.get(counter.pop.remote()) == max_retries + 1
    finally:
        if override_max_retries:
            del os.environ["RAY_TASK_MAX_RETRIES"]


@pytest.mark.parametrize("reconstruction_enabled", [False, True])
def test_reconstruct_freed_object(config, ray_start_cluster, reconstruction_enabled):
    # Workaround to reset the config to the default value.
    if not reconstruction_enabled:
        config["lineage_pinning_enabled"] = False

    cluster = ray_start_cluster
    # Head node with no resources.
    cluster.add_node(
        num_cpus=0,
        _system_config=config,
        enable_object_reconstruction=reconstruction_enabled,
    )
    ray.init(address=cluster.address)

    node_to_kill = cluster.add_node(num_cpus=1, object_store_memory=10**8)
    cluster.wait_for_nodes()

    @ray.remote
    def large_object():
        return np.zeros(10**7, dtype=np.uint8)

    @ray.remote
    def dependent_task(x):
        return np.zeros(10**7, dtype=np.uint8)

    obj = large_object.remote()
    x = dependent_task.remote(obj)
    ray.get(dependent_task.remote(x))

    ray.internal.free(obj)
    cluster.remove_node(node_to_kill, allow_graceful=False)
    cluster.add_node(num_cpus=1, object_store_memory=10**8)

    if reconstruction_enabled:
        ray.get(x)
    else:
        with pytest.raises(ray.exceptions.ObjectLostError):
            ray.get(x)
        with pytest.raises(ray.exceptions.ObjectFreedError):
            ray.get(obj)


if __name__ == "__main__":
    import pytest

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
