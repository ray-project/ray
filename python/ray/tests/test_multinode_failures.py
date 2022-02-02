import os
import signal
import sys
import time

import pytest

import ray
import ray.ray_constants as ray_constants
from ray.cluster_utils import Cluster, cluster_not_supported
from ray._private.test_utils import get_other_nodes, Semaphore

SIGKILL = signal.SIGKILL if sys.platform != "win32" else signal.SIGTERM


@pytest.mark.xfail(cluster_not_supported, reason="cluster not supported")
@pytest.fixture(params=[(1, 4), (4, 4)])
def ray_start_workers_separate_multinode(request):
    num_nodes = request.param[0]
    num_initial_workers = request.param[1]
    # Start the Ray processes.
    cluster = Cluster()
    for _ in range(num_nodes):
        cluster.add_node(
            num_cpus=num_initial_workers, resources={"custom": num_initial_workers}
        )
    ray.init(address=cluster.address)

    yield num_nodes, num_initial_workers
    # The code after the yield will run as teardown code.
    ray.shutdown()
    cluster.shutdown()


def test_worker_failed(ray_start_workers_separate_multinode):
    num_nodes, num_initial_workers = ray_start_workers_separate_multinode

    block_worker = Semaphore.remote(0)
    block_driver = Semaphore.remote(0)
    ray.get([block_worker.locked.remote(), block_driver.locked.remote()])

    # Acquire a custom resource that isn't released on `ray.get` to make sure
    # this task gets spread across all the nodes.
    @ray.remote(num_cpus=1, resources={"custom": 1})
    def get_pids():
        ray.get(block_driver.release.remote())
        ray.get(block_worker.acquire.remote())
        return os.getpid()

    total_num_workers = num_nodes * num_initial_workers
    pid_refs = [get_pids.remote() for _ in range(total_num_workers)]
    ray.get([block_driver.acquire.remote() for _ in range(total_num_workers)])
    ray.get([block_worker.release.remote() for _ in range(total_num_workers)])

    pids = set(ray.get(pid_refs))

    @ray.remote
    def f(x):
        time.sleep(0.5)
        return x

    # Submit more tasks than there are workers so that all workers and
    # cores are utilized.
    object_refs = [f.remote(i) for i in range(num_initial_workers * num_nodes)]
    object_refs += [f.remote(object_ref) for object_ref in object_refs]
    # Allow the tasks some time to begin executing.
    time.sleep(0.1)
    # Kill the workers as the tasks execute.
    for pid in pids:
        try:
            os.kill(pid, SIGKILL)
        except OSError:
            # The process may have already exited due to worker capping.
            pass
        time.sleep(0.1)
    # Make sure that we either get the object or we get an appropriate
    # exception.
    for object_ref in object_refs:
        try:
            ray.get(object_ref)
        except (ray.exceptions.RayTaskError, ray.exceptions.WorkerCrashedError):
            pass


def _test_component_failed(cluster, component_type):
    """Kill a component on all worker nodes and check workload succeeds."""
    # Submit many tasks with many dependencies.
    @ray.remote
    def f(x):
        # Sleep to make sure that tasks actually fail mid-execution.
        time.sleep(0.01)
        return x

    @ray.remote
    def g(*xs):
        # Sleep to make sure that tasks actually fail mid-execution. We
        # only use it for direct calls because the test already takes a
        # long time to run with the raylet codepath.
        time.sleep(0.01)
        return 1

    # Kill the component on all nodes except the head node as the tasks
    # execute. Do this in a loop while submitting tasks between each
    # component failure.
    time.sleep(0.1)
    worker_nodes = get_other_nodes(cluster)
    assert len(worker_nodes) > 0
    for node in worker_nodes:
        process = node.all_processes[component_type][0].process
        # Submit a round of tasks with many dependencies.
        x = 1
        for _ in range(1000):
            x = f.remote(x)

        xs = [g.remote(1)]
        for _ in range(100):
            xs.append(g.remote(*xs))
            xs.append(g.remote(1))

        # Kill a component on one of the nodes.
        process.terminate()
        time.sleep(1)
        process.kill()
        process.wait()
        assert not process.poll() is None

        # Make sure that we can still get the objects after the
        # executing tasks died.
        ray.get(x)
        ray.get(xs)


def check_components_alive(cluster, component_type, check_component_alive):
    """Check that a given component type is alive on all worker nodes."""
    worker_nodes = get_other_nodes(cluster)
    assert len(worker_nodes) > 0
    for node in worker_nodes:
        process = node.all_processes[component_type][0].process
        if check_component_alive:
            assert process.poll() is None
        else:
            print(
                "waiting for "
                + component_type
                + " with PID "
                + str(process.pid)
                + "to terminate"
            )
            process.wait()
            print(
                "done waiting for "
                + component_type
                + " with PID "
                + str(process.pid)
                + "to terminate"
            )
            assert not process.poll() is None


@pytest.mark.parametrize(
    "ray_start_cluster",
    [
        {
            "num_cpus": 8,
            "num_nodes": 4,
            "_system_config": {
                # Raylet codepath is not stable with a shorter timeout.
                "num_heartbeats_timeout": 10
            },
        }
    ],
    indirect=True,
)
def test_raylet_failed(ray_start_cluster):
    cluster = ray_start_cluster
    # Kill all raylets on worker nodes.
    _test_component_failed(cluster, ray_constants.PROCESS_TYPE_RAYLET)


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
