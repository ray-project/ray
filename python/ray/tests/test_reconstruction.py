import os
import signal
import sys
import time

import numpy as np
import pytest

import ray
from ray._private.test_utils import (
    wait_for_condition,
    wait_for_pid_to_exit,
)

SIGKILL = signal.SIGKILL if sys.platform != "win32" else signal.SIGTERM


@pytest.fixture
def config(request):
    config = {
        "health_check_initial_delay_ms": 0,
        "health_check_period_ms": 100,
        "health_check_failure_threshold": 10,
        "object_timeout_milliseconds": 200,
    }

    yield config


def test_cached_object(config, ray_start_cluster):
    cluster = ray_start_cluster
    # Head node with no resources.
    cluster.add_node(num_cpus=0, _system_config=config)
    ray.init(address=cluster.address)
    # Node to place the initial object.
    node_to_kill = cluster.add_node(
        num_cpus=1, resources={"node1": 1}, object_store_memory=10**8
    )
    cluster.add_node(num_cpus=1, resources={"node2": 1}, object_store_memory=10**8)
    cluster.wait_for_nodes()

    @ray.remote
    def large_object():
        return np.zeros(10**7, dtype=np.uint8)

    @ray.remote
    def dependent_task(x):
        return

    obj = large_object.options(resources={"node1": 1}).remote()
    ray.get(dependent_task.options(resources={"node2": 1}).remote(obj))

    cluster.remove_node(node_to_kill, allow_graceful=False)
    cluster.add_node(num_cpus=1, resources={"node1": 1}, object_store_memory=10**8)
    wait_for_condition(
        lambda: not all(node["Alive"] for node in ray.nodes()), timeout=10
    )

    for _ in range(20):
        large_object.options(resources={"node2": 1}).remote()

    ray.get(dependent_task.remote(obj))


@pytest.mark.parametrize("reconstruction_enabled", [False, True])
def test_reconstruction_cached_dependency(
    config, ray_start_cluster, reconstruction_enabled
):
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
    cluster.add_node(num_cpus=1, resources={"node2": 1}, object_store_memory=10**8)
    cluster.wait_for_nodes()

    @ray.remote(max_retries=0)
    def large_object():
        return np.zeros(10**7, dtype=np.uint8)

    @ray.remote
    def chain(x):
        return x

    @ray.remote
    def dependent_task(x):
        return

    obj = large_object.options(resources={"node2": 1}).remote()
    obj = chain.options(resources={"node1": 1}).remote(obj)
    ray.get(dependent_task.options(resources={"node1": 1}).remote(obj))

    cluster.remove_node(node_to_kill, allow_graceful=False)
    cluster.add_node(num_cpus=1, resources={"node1": 1}, object_store_memory=10**8)
    wait_for_condition(
        lambda: not all(node["Alive"] for node in ray.nodes()), timeout=10
    )

    for _ in range(20):
        large_object.options(resources={"node2": 1}).remote()

    if reconstruction_enabled:
        ray.get(dependent_task.remote(obj))
    else:
        with pytest.raises(ray.exceptions.RayTaskError):
            ray.get(dependent_task.remote(obj))
        with pytest.raises(ray.exceptions.ObjectLostError):
            ray.get(obj)


@pytest.mark.skipif(
    sys.platform == "win32", reason="Very flaky on Windows due to memory usage."
)
@pytest.mark.parametrize("reconstruction_enabled", [False, True])
def test_basic_reconstruction(config, ray_start_cluster, reconstruction_enabled):
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

    cluster.remove_node(node_to_kill, allow_graceful=False)
    node_to_kill = cluster.add_node(
        num_cpus=1, resources={"node1": 1}, object_store_memory=10**8
    )

    if reconstruction_enabled:
        ray.get(dependent_task.remote(obj))
    else:
        with pytest.raises(ray.exceptions.RayTaskError):
            ray.get(dependent_task.remote(obj))
        with pytest.raises(ray.exceptions.ObjectLostError):
            ray.get(obj)

    # Losing the object a second time will cause reconstruction to fail because
    # we have reached the max task retries.
    cluster.remove_node(node_to_kill, allow_graceful=False)
    cluster.add_node(num_cpus=1, resources={"node1": 1}, object_store_memory=10**8)

    if reconstruction_enabled:
        with pytest.raises(
            ray.exceptions.ObjectReconstructionFailedMaxAttemptsExceededError
        ):
            ray.get(obj)
    else:
        with pytest.raises(ray.exceptions.ObjectLostError):
            ray.get(obj)


# TODO(swang): Add a test to check for ObjectReconstructionFailedError if we
# fail to reconstruct a ray.put object.
@pytest.mark.skipif(sys.platform == "win32", reason="Very flaky on Windows.")
@pytest.mark.parametrize("reconstruction_enabled", [False, True])
def test_basic_reconstruction_put(config, ray_start_cluster, reconstruction_enabled):
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
    cluster.add_node(num_cpus=1, resources={"node2": 1}, object_store_memory=10**8)
    cluster.wait_for_nodes()

    @ray.remote(max_retries=1 if reconstruction_enabled else 0)
    def large_object():
        return np.zeros(10**7, dtype=np.uint8)

    @ray.remote
    def dependent_task(x):
        return x

    obj = ray.put(np.zeros(10**7, dtype=np.uint8))
    result = dependent_task.options(resources={"node1": 1}).remote(obj)
    ray.get(result)
    del obj

    cluster.remove_node(node_to_kill, allow_graceful=False)
    cluster.add_node(num_cpus=1, resources={"node1": 1}, object_store_memory=10**8)

    for _ in range(20):
        ray.put(np.zeros(10**7, dtype=np.uint8))

    if reconstruction_enabled:
        ray.get(result)
    else:
        # The copy that we fetched earlier may still be local or it may have
        # been evicted.
        try:
            ray.get(result)
        except ray.exceptions.ObjectLostError:
            pass


@pytest.mark.skipif(sys.platform == "win32", reason="Very flaky on Windows.")
@pytest.mark.parametrize("reconstruction_enabled", [False, True])
def test_basic_reconstruction_actor_task(
    config, ray_start_cluster, reconstruction_enabled
):
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
        num_cpus=1, resources={"node1": 2}, object_store_memory=10**8
    )
    cluster.add_node(num_cpus=1, resources={"node2": 1}, object_store_memory=10**8)
    cluster.wait_for_nodes()

    # Always set max retries to -1 because Ray fails actor tasks if the actor
    # is restarting when the task is submitted.
    # See #22818 for details.
    @ray.remote(
        max_restarts=-1,
        max_task_retries=-1,
        resources={"node1": 1},
        num_cpus=0,
    )
    class Actor:
        def __init__(self):
            pass

        def large_object(self):
            return np.zeros(10**7, dtype=np.uint8)

        def pid(self):
            return os.getpid()

    @ray.remote
    def dependent_task(x):
        return

    a = Actor.remote()
    pid = ray.get(a.pid.remote())
    obj = a.large_object.remote()
    ray.get(dependent_task.options(resources={"node1": 1}).remote(obj))

    for i in range(10):
        # Workaround to kill the actor process too since there is a bug where the
        # actor's plasma client hangs after the plasma store has exited.
        os.kill(pid, SIGKILL)

        cluster.remove_node(node_to_kill, allow_graceful=False)
        node_to_kill = cluster.add_node(
            num_cpus=1, resources={"node1": 2}, object_store_memory=10**8
        )

        wait_for_pid_to_exit(pid)

        if reconstruction_enabled:
            ray.get(dependent_task.remote(obj))
        else:
            with pytest.raises(ray.exceptions.RayTaskError):
                ray.get(dependent_task.remote(obj))
            with pytest.raises(ray.exceptions.ObjectLostError):
                ray.get(obj)

        # Make sure the actor handle is still usable.
        pid_ref = a.pid.remote()
        print(i, "pid", pid_ref)
        pid = ray.get(pid_ref)


@pytest.mark.skipif(sys.platform == "win32", reason="Very flaky on Windows.")
@pytest.mark.parametrize("reconstruction_enabled", [False, True])
def test_basic_reconstruction_actor_lineage_disabled(
    config, ray_start_cluster, reconstruction_enabled
):
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
        num_cpus=1, resources={"node1": 2}, object_store_memory=10**8
    )
    cluster.add_node(num_cpus=1, resources={"node2": 1}, object_store_memory=10**8)
    cluster.wait_for_nodes()

    # Actor can be restarted but its outputs cannot be reconstructed.
    @ray.remote(max_restarts=-1, resources={"node1": 1}, num_cpus=0)
    class Actor:
        def __init__(self):
            pass

        def large_object(self):
            return np.zeros(10**7, dtype=np.uint8)

        def pid(self):
            return os.getpid()

    @ray.remote
    def dependent_task(x):
        return

    a = Actor.remote()
    pid = ray.get(a.pid.remote())
    obj = a.large_object.remote()
    ray.get(dependent_task.options(resources={"node1": 1}).remote(obj))

    # Workaround to kill the actor process too since there is a bug where the
    # actor's plasma client hangs after the plasma store has exited.
    os.kill(pid, SIGKILL)

    cluster.remove_node(node_to_kill, allow_graceful=False)
    cluster.add_node(num_cpus=1, resources={"node1": 2}, object_store_memory=10**8)

    wait_for_pid_to_exit(pid)

    with pytest.raises(ray.exceptions.ObjectLostError):
        ray.get(obj)

    while True:
        time.sleep(1)
        try:
            ray.get(a.pid.remote())
            break
        except ray.exceptions.RayActorError:
            pass

    # Make sure the actor handle is still usable.
    pid = ray.get(a.pid.remote())


@pytest.mark.skipif(sys.platform == "win32", reason="Test failing on Windows.")
@pytest.mark.parametrize("reconstruction_enabled", [False, True])
def test_basic_reconstruction_actor_constructor(
    config, ray_start_cluster, reconstruction_enabled
):
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
    cluster.add_node(num_cpus=1, resources={"node2": 1}, object_store_memory=10**8)
    cluster.wait_for_nodes()

    @ray.remote(max_retries=1 if reconstruction_enabled else 0)
    def large_object():
        return np.zeros(10**7, dtype=np.uint8)

    # Both the constructor and a method depend on the large object.
    @ray.remote(max_restarts=-1)
    class Actor:
        def __init__(self, x):
            pass

        def dependent_task(self, x):
            return

        def pid(self):
            return os.getpid()

    obj = large_object.options(resources={"node1": 1}).remote()
    a = Actor.options(resources={"node1": 1}).remote(obj)
    ray.get(a.dependent_task.remote(obj))
    pid = ray.get(a.pid.remote())

    # Workaround to kill the actor process too since there is a bug where the
    # actor's plasma client hangs after the plasma store has exited.
    os.kill(pid, SIGKILL)

    cluster.remove_node(node_to_kill, allow_graceful=False)
    cluster.add_node(num_cpus=1, resources={"node1": 1}, object_store_memory=10**8)

    wait_for_pid_to_exit(pid)

    # Wait for the actor to restart.
    def probe():
        try:
            ray.get(a.dependent_task.remote(obj))
            return True
        except ray.exceptions.RayActorError as e:
            return e.actor_init_failed
        except (ray.exceptions.RayTaskError, ray.exceptions.ObjectLostError):
            return True

    wait_for_condition(probe)

    if reconstruction_enabled:
        ray.get(a.dependent_task.remote(obj))
    else:
        with pytest.raises(ray.exceptions.RayActorError) as exc_info:
            x = a.dependent_task.remote(obj)
            print(x)
            ray.get(x)
        exc = str(exc_info.value)
        assert "arguments" in exc
        assert "ObjectLostError" in exc


@pytest.mark.parametrize("reconstruction_enabled", [False, True])
def test_multiple_downstream_tasks(config, ray_start_cluster, reconstruction_enabled):
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
    cluster.add_node(num_cpus=1, resources={"node2": 1}, object_store_memory=10**8)
    cluster.wait_for_nodes()

    @ray.remote
    def large_object():
        return np.zeros(10**7, dtype=np.uint8)

    @ray.remote
    def chain(x):
        return x

    @ray.remote
    def dependent_task(x):
        return

    obj = large_object.options(resources={"node2": 1}).remote()
    downstream = [chain.options(resources={"node1": 1}).remote(obj) for _ in range(4)]
    for obj in downstream:
        ray.get(dependent_task.options(resources={"node1": 1}).remote(obj))

    cluster.remove_node(node_to_kill, allow_graceful=False)
    node_to_kill = cluster.add_node(
        num_cpus=1, resources={"node1": 1}, object_store_memory=10**8
    )

    if reconstruction_enabled:
        for obj in downstream:
            ray.get(dependent_task.options(resources={"node1": 1}).remote(obj))
    else:
        with pytest.raises(ray.exceptions.RayTaskError):
            for obj in downstream:
                ray.get(dependent_task.options(resources={"node1": 1}).remote(obj))
        with pytest.raises(ray.exceptions.ObjectLostError):
            ray.get(obj)

    cluster.remove_node(node_to_kill, allow_graceful=False)
    cluster.add_node(num_cpus=1, resources={"node1": 1}, object_store_memory=10**8)

    if reconstruction_enabled:
        for obj in downstream:
            ray.get(dependent_task.options(resources={"node1": 1}).remote(obj))
    else:
        for obj in downstream:
            with pytest.raises(ray.exceptions.ObjectLostError):
                ray.get(obj)


@pytest.mark.parametrize("reconstruction_enabled", [False, True])
def test_reconstruction_chain(config, ray_start_cluster, reconstruction_enabled):
    # Workaround to reset the config to the default value.
    if not reconstruction_enabled:
        config["lineage_pinning_enabled"] = False

    cluster = ray_start_cluster
    # Head node with no resources.
    cluster.add_node(
        num_cpus=0,
        _system_config=config,
        object_store_memory=10**8,
        enable_object_reconstruction=reconstruction_enabled,
    )
    ray.init(address=cluster.address)
    node_to_kill = cluster.add_node(num_cpus=1, object_store_memory=10**8)
    cluster.wait_for_nodes()

    @ray.remote(max_retries=1 if reconstruction_enabled else 0)
    def large_object():
        return np.zeros(10**7, dtype=np.uint8)

    @ray.remote
    def chain(x):
        return x

    @ray.remote
    def dependent_task(x):
        return x

    obj = large_object.remote()
    for _ in range(20):
        obj = chain.remote(obj)
    ray.get(dependent_task.remote(obj))

    cluster.remove_node(node_to_kill, allow_graceful=False)
    cluster.add_node(num_cpus=1, object_store_memory=10**8)

    if reconstruction_enabled:
        ray.get(dependent_task.remote(obj))
    else:
        with pytest.raises(ray.exceptions.RayTaskError):
            ray.get(dependent_task.remote(obj))
        with pytest.raises(ray.exceptions.ObjectLostError):
            ray.get(obj)


if __name__ == "__main__":
    import pytest

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
