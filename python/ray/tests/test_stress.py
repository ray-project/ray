import numpy as np
import pytest
import time

import ray
from ray.cluster_utils import Cluster, cluster_not_supported


@pytest.mark.xfail(cluster_not_supported, reason="cluster not supported")
@pytest.fixture(params=[(1, 4), (4, 4)])
def ray_start_combination(request):
    num_nodes = request.param[0]
    num_workers_per_scheduler = request.param[1]
    # Start the Ray processes.
    cluster = Cluster(
        initialize_head=True,
        head_node_args={"num_cpus": 10, "redis_max_memory": 10 ** 8},
    )
    for i in range(num_nodes - 1):
        cluster.add_node(num_cpus=10)
    ray.init(address=cluster.address)

    yield num_nodes, num_workers_per_scheduler, cluster
    # The code after the yield will run as teardown code.
    ray.shutdown()
    cluster.shutdown()


def test_submitting_tasks(ray_start_combination):
    _, _, cluster = ray_start_combination

    @ray.remote
    def f(x):
        return x

    for _ in range(1):
        ray.get([f.remote(1) for _ in range(1000)])

    for _ in range(10):
        ray.get([f.remote(1) for _ in range(100)])

    for _ in range(100):
        ray.get([f.remote(1) for _ in range(10)])

    for _ in range(1000):
        ray.get([f.remote(1) for _ in range(1)])

    assert cluster.remaining_processes_alive()


def test_dependencies(ray_start_combination):
    _, _, cluster = ray_start_combination

    @ray.remote
    def f(x):
        return x

    x = 1
    for _ in range(1000):
        x = f.remote(x)
    ray.get(x)

    @ray.remote
    def g(*xs):
        return 1

    xs = [g.remote(1)]
    for _ in range(100):
        xs.append(g.remote(*xs))
        xs.append(g.remote(1))
    ray.get(xs)

    assert cluster.remaining_processes_alive()


def test_wait(ray_start_combination):
    num_nodes, num_workers_per_scheduler, cluster = ray_start_combination
    num_workers = num_nodes * num_workers_per_scheduler

    @ray.remote
    def f(x):
        return x

    x_ids = [f.remote(i) for i in range(100)]
    for i in range(len(x_ids)):
        ray.wait([x_ids[i]])
    for i in range(len(x_ids) - 1):
        ray.wait(x_ids[i:])

    @ray.remote
    def g(x):
        time.sleep(x)

    for i in range(1, 5):
        x_ids = [g.remote(np.random.uniform(0, i)) for _ in range(2 * num_workers)]
        ray.wait(x_ids, num_returns=len(x_ids))

    assert cluster.remaining_processes_alive()


if __name__ == "__main__":
    import pytest
    import os
    import sys

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
