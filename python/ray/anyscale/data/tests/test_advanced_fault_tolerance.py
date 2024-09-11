import asyncio
import threading
import time

import pytest

import ray
from ray.tests.conftest import *  # noqa


def test_removed_nodes_not_added_back(ray_start_cluster):
    """Test that a dataset with actor pools can finish, when some
    nodes in the cluster are removed and not added back."""
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=0)
    ray.init()

    num_nodes = 4
    nodes = []
    for _ in range(num_nodes):
        nodes.append(cluster.add_node(num_cpus=10, num_gpus=1))
    cluster.wait_for_nodes()

    num_items = 100

    @ray.remote(num_cpus=0)
    class Signal:
        def __init__(self):
            self._counter = 0

        async def incr(self):
            self._counter += 1

        async def wait(self, value):
            while self._counter != value:
                await asyncio.sleep(0.1)

    signal_actor = Signal.remote()

    class MyUDF:
        def __init__(self, signal_actor):
            self._signal_actor = signal_actor
            self._signal_sent = False

        def __call__(self, batch):
            if not self._signal_sent:
                self._signal_actor.incr.remote()
                self._signal_sent = True
            time.sleep(0.1)
            return batch

    res = []

    def run_dataset():
        nonlocal res

        ds = ray.data.range(num_items, override_num_blocks=num_items)
        ds = ds.map_batches(
            MyUDF,
            fn_constructor_args=[signal_actor],
            concurrency=num_nodes,
            batch_size=1,
            num_gpus=1,
        )
        res = ds.take_all()

    thread = threading.Thread(target=run_dataset)
    thread.start()

    # Wait for all actors to start, then remove some nodes.
    ray.get(signal_actor.wait.remote(num_nodes))
    print("Removing nodes")
    nodes_to_remove = nodes[-num_nodes // 2 :]
    for node in nodes_to_remove:
        cluster.remove_node(node)

    thread.join()
    assert sorted(res, key=lambda x: x["id"]) == [{"id": i} for i in range(num_items)]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
