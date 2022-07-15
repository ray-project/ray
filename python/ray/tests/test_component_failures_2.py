import signal
import sys
import time

import pytest

import ray
import ray._private.ray_constants as ray_constants
from ray._private.test_utils import get_other_nodes, wait_for_condition
from ray.cluster_utils import Cluster, cluster_not_supported

SIGKILL = signal.SIGKILL if sys.platform != "win32" else signal.SIGTERM


@pytest.mark.xfail(cluster_not_supported, reason="cluster not supported")
@pytest.fixture(params=[(1, 4), (4, 4)])
def ray_start_workers_separate_multinode(request):
    num_nodes = request.param[0]
    num_initial_workers = request.param[1]
    # Start the Ray processes.
    cluster = Cluster()
    for _ in range(num_nodes):
        cluster.add_node(num_cpus=num_initial_workers)
    ray.init(address=cluster.address)

    yield num_nodes, num_initial_workers
    # The code after the yield will run as teardown code.
    ray.shutdown()
    cluster.shutdown()


def _test_component_failed(cluster, component_type):
    """Kill a component on all worker nodes and check workload succeeds."""
    # Submit many tasks with many dependencies.
    @ray.remote
    def f(x):
        return x

    @ray.remote
    def g(*xs):
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
            "_system_config": {"num_heartbeats_timeout": 10},
        }
    ],
    indirect=True,
)
def test_raylet_failed(ray_start_cluster):
    cluster = ray_start_cluster
    # Kill all raylets on worker nodes.
    _test_component_failed(cluster, ray_constants.PROCESS_TYPE_RAYLET)


def test_get_node_info_after_raylet_died(ray_start_cluster_head):
    cluster = ray_start_cluster_head

    def get_node_info():
        return ray._private.services.get_node_to_connect_for_driver(
            cluster.redis_address,
            cluster.gcs_address,
            cluster.head_node.node_ip_address,
            redis_password=cluster.redis_password,
        )

    assert get_node_info().raylet_socket_name == cluster.head_node.raylet_socket_name

    cluster.head_node.kill_raylet()
    wait_for_condition(
        lambda: not cluster.global_state.node_table()[0]["Alive"], timeout=30
    )
    with pytest.raises(RuntimeError):
        get_node_info()

    node2 = cluster.add_node()
    assert get_node_info().raylet_socket_name == node2.raylet_socket_name


if __name__ == "__main__":
    import os
    import pytest

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
