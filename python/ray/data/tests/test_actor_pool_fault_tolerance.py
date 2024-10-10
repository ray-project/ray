import asyncio
import threading

import pytest

import ray
from ray.tests.conftest import *  # noqa


def test_removed_nodes_and_added_back(ray_start_cluster):
    """Test that a dataset with actor pools can finish, when
    all nodes in the cluster are removed and added back."""
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=0)
    ray.init()

    @ray.remote(num_cpus=0)
    class Signal:
        def __init__(self):
            self._node_id = ray.get_runtime_context().get_node_id()
            self._num_alive_actors = 0
            self._all_nodes_removed = False
            self._all_nodes_restarted = False

        async def notify_actor_alive(self):
            self._num_alive_actors += 1

        async def wait_for_actors_alive(self, value):
            while self._num_alive_actors != value:
                await asyncio.sleep(0.01)

        async def notify_nodes_removed(self):
            self._all_nodes_removed = True

        async def notify_nodes_restarted(self):
            self._all_nodes_restarted = True

        async def wait_for_nodes_removed(self):
            while not self._all_nodes_removed:
                await asyncio.sleep(0.01)

        async def wait_for_nodes_restarted(self):
            while not self._all_nodes_restarted:
                await asyncio.sleep(0.01)

    # Create the signal actor on the head node.
    signal_actor = Signal.remote()

    # Spin up nodes
    num_nodes = 5
    nodes = []
    for _ in range(num_nodes):
        nodes.append(cluster.add_node(num_cpus=10, num_gpus=1))
    cluster.wait_for_nodes()

    class MyUDF:
        def __init__(self, signal_actor):
            self._node_id = ray.get_runtime_context().get_node_id()
            self._signal_actor = signal_actor
            self._signal_sent = False

        def __call__(self, batch):
            if not self._signal_sent:
                # Notify the Actor is alive
                self._signal_actor.notify_actor_alive.remote()

                # Wait for the driver to remove nodes. This makes sure all
                # actors are running tasks when removing nodes.
                ray.get(self._signal_actor.wait_for_nodes_removed.remote())

                # Wait for the driver to add nodes.
                ray.get(self._signal_actor.wait_for_nodes_restarted.remote())

                self._signal_sent = True

            return batch

    res = []
    num_items = 100

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

    # Kick off Actors
    thread = threading.Thread(target=run_dataset)
    thread.start()

    # Wait for all actors to start
    ray.get(signal_actor.wait_for_actors_alive.remote(num_nodes))

    # Remove all the nodes
    for node in nodes:
        cluster.remove_node(node)
    nodes.clear()
    ray.get(signal_actor.notify_nodes_removed.remote())

    # Add back all the nodes
    for _ in range(num_nodes):
        nodes.append(cluster.add_node(num_cpus=10, num_gpus=1))
    cluster.wait_for_nodes()
    ray.get(signal_actor.notify_nodes_restarted.remote())

    thread.join()
    assert sorted(res, key=lambda x: x["id"]) == [{"id": i} for i in range(num_items)]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
