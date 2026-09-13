import asyncio
import threading
import time

import pyarrow as pa
import pytest

import ray
from ray._common.test_utils import wait_for_condition
from ray._raylet import GcsClient
from ray.core.generated import autoscaler_pb2
from ray.data._internal.compute import ActorPoolStrategy
from ray.data._internal.execution.interfaces import (
    BlockEntry,
    ExecutionResources,
    RefBundle,
)
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.map_transformer import (
    BlockMapTransformFn,
    MapTransformer,
)
from ray.data._internal.execution.streaming_executor import StreamingExecutor
from ray.data.block import BlockAccessor
from ray.data.context import DataContext
from ray.data.tests.conftest import *  # noqa: F403
from ray.tests.conftest import *  # noqa: F403


def test_removed_nodes_not_added_back(ray_start_cluster, restore_data_context):
    """Test that a dataset with actor pools can finish, when some
    nodes in the cluster are removed and not added back."""
    DataContext.get_current().enable_node_aware_actor_pool = True

    cluster = ray_start_cluster
    cluster.add_node(num_cpus=0)
    ray.init()

    @ray.remote(num_cpus=0)
    class Signal:
        def __init__(self):
            self._num_alive_actors = 0
            self._nodes_removed = False

        async def notify_actor_alive(self):
            self._num_alive_actors += 1

        async def wait_for_actors_alive(self, value):
            while self._num_alive_actors != value:
                await asyncio.sleep(0.01)

        async def notify_nodes_removed(self):
            self._nodes_removed = True

        async def wait_for_nodes_removed(self):
            while not self._nodes_removed:
                await asyncio.sleep(0.01)

    # Create the signal actor on the head node.
    signal_actor = Signal.remote()

    num_nodes = 4
    nodes = []
    for _ in range(num_nodes):
        nodes.append(cluster.add_node(num_cpus=10, num_gpus=1))
    cluster.wait_for_nodes()

    num_items = 100

    class MyUDF:
        def __init__(self, signal_actor):
            self._signal_actor = signal_actor
            self._signal_sent = False

        def __call__(self, batch):
            if not self._signal_sent:
                self._signal_actor.notify_actor_alive.remote()
                # Wait for the driver to remove nodes. This makes sure all
                # actors are running tasks when removing nodes.
                ray.get(self._signal_actor.wait_for_nodes_removed.remote())
                self._signal_sent = True
            time.sleep(0.01)
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
    ray.get(signal_actor.wait_for_actors_alive.remote(num_nodes))
    print("Removing nodes")
    nodes_to_remove = nodes[-num_nodes // 2 :]
    for node in nodes_to_remove:
        cluster.remove_node(node)
    ray.get(signal_actor.notify_nodes_removed.remote())

    thread.join()
    assert sorted(res, key=lambda x: x["id"]) == [{"id": i} for i in range(num_items)]


def test_map_operator_counts_lineage_reconstruction_tasks(
    ray_start_cluster_enabled, disable_timed_cache_fixture, restore_data_context
):
    # the `disable_timed_cache_fixture` is neccessary because this test relies on
    # the most up-to-date values from `get_local_ongoing_lineage_reconstruction_tasks`
    data_context = DataContext.get_current()
    data_context.enable_node_aware_actor_pool = True

    # Create a cluster with a head node and a single worker node.
    cluster = ray_start_cluster_enabled
    cluster.add_node(resources={"head": 1})
    ray.init(address=cluster.address)
    worker = cluster.add_node(resources={"worker": 1})

    # Create an input data operator with a single block as input.
    block = pa.Table.from_pylist([{"data": "\x00" * 128 * 1024 * 1024}])
    block_ref = ray.put(block)
    metadata = BlockAccessor.for_block(block).get_metadata()
    schema = BlockAccessor.for_block(block).schema()
    bundle = RefBundle(
        [BlockEntry(ref=block_ref, metadata=metadata)],
        owns_blocks=False,
        schema=schema,
    )
    input_op = InputDataBuffer(data_context, [bundle])

    # Create a signal actor so the map only finishes when we want it to.
    @ray.remote(num_cpus=0, resources={"head": 1})
    class Signal:
        def __init__(self, is_map_blocked: bool):
            self._is_map_blocked = is_map_blocked

        def block_map(self):
            print("Blocking map function")
            self._is_map_blocked = True

        def unblock_map(self):
            print("Unblocking map function")
            self._is_map_blocked = False

        def is_map_blocked(self) -> bool:
            return self._is_map_blocked

    # Start with the transform function unblocked.
    signal = Signal.remote(False)

    def block_fn(block, _):
        print("Entering block function")

        while ray.get(signal.is_map_blocked.remote()):
            print("Waiting for map to be unblocked")
            time.sleep(0.1)

        print("Exiting block function")
        return block

    transform_fns = [BlockMapTransformFn(block_fn)]
    map_transformer = MapTransformer(transform_fns)
    map_op = MapOperator.create(
        map_transformer,
        input_op,
        data_context,
        compute_strategy=ActorPoolStrategy(size=1),
        ray_remote_args={"resources": {"worker": 1, "num_cpus": 0}},
    )

    output_bundles = []
    executor = StreamingExecutor(data_context)
    for bundle in executor.execute(map_op):
        output_bundles.append(bundle)

    assert map_op.extra_resource_usage() == ExecutionResources.zero()

    # Remove the node to trigger lineage reconstruction.
    ray.get(signal.block_map.remote())
    cluster.remove_node(worker)
    wait_for_condition(lambda: map_op.extra_resource_usage().cpu == 1, timeout=10)

    # Re-add the node and unblock the map function.
    ray.get(signal.unblock_map.remote())
    cluster.add_node(resources={"worker": 1})
    wait_for_condition(
        lambda: map_op.extra_resource_usage() == ExecutionResources.zero(), timeout=10
    )


def test_map_operator_does_not_launch_actor_tasks_on_draining_nodes(
    ray_start_cluster_enabled, disable_timed_cache_fixture, restore_data_context
):
    DataContext.get_current().enable_node_aware_actor_pool = True

    # Create a cluster with a head node and two worker nodes.
    cluster = ray_start_cluster_enabled
    cluster.add_node(resources={"head": 1})
    cluster.wait_for_nodes()
    ray.init(address=cluster.address)

    @ray.remote(num_cpus=0)
    class DrainNodeSignal:
        def __init__(self, waiting_for_num_actors: int):
            self.actor_init_event = asyncio.Event()
            self.node_drained_event = asyncio.Event()
            self.lock = asyncio.Lock()
            self.waiting_for_num_actors = waiting_for_num_actors

        async def send_actor_started(self):
            async with self.lock:
                self.waiting_for_num_actors -= 1
                if self.waiting_for_num_actors == 0:
                    self.actor_init_event.set()

        def send_node_drained(self):
            self.node_drained_event.set()

        async def wait_for_actors_to_start(self):
            await self.actor_init_event.wait()

        async def wait_for_node_drained(self):
            await self.node_drained_event.wait()

    # Make sure signal actor created on the head node
    signal_actor = DrainNodeSignal.remote(2)

    # now add worker nodes
    worker1 = cluster.add_node(resources={"worker": 1}, num_cpus=1)
    cluster.add_node(resources={"worker": 1}, num_cpus=1)
    cluster.wait_for_nodes()

    drained_node_id = worker1.node_id

    class AssertingUDF:
        def __init__(self, signal_actor):
            self._node_id = ray.get_runtime_context().get_node_id()
            self._signal_actor = signal_actor
            ray.get(self._signal_actor.send_actor_started.remote())
            ray.get(self._signal_actor.wait_for_node_drained.remote())

        def __call__(self, batch):
            # Check that this task isn't running on the draining node.
            assert self._node_id != drained_node_id

            return batch

    exception = None

    def run_dataset():
        try:
            ray.data.range(50, override_num_blocks=50).map_batches(
                AssertingUDF,
                fn_constructor_args=[signal_actor],
                concurrency=2,
                num_cpus=0,
                resources={"worker": 1},
            ).take_all()
        except Exception as e:
            nonlocal exception
            exception = e

    # Execute the Dataset in a separate thread
    thread = threading.Thread(target=run_dataset)
    thread.start()

    # Wait for actors to start.
    ray.get(signal_actor.wait_for_actors_to_start.remote())

    # Drain the first worker.
    print("Draining node", drained_node_id)
    gcs_client = GcsClient(address=ray.get_runtime_context().gcs_address)
    deadline_timestamp_ms = (time.time_ns() // 1e6) + (999999 * 1e3)
    is_accepted, _ = gcs_client.drain_node(
        drained_node_id,
        autoscaler_pb2.DrainNodeReason.Value("DRAIN_NODE_REASON_PREEMPTION"),
        "",
        deadline_timestamp_ms,
    )
    assert is_accepted

    ray.get(signal_actor.send_node_drained.remote())

    # Wait for the dataset to finish.
    thread.join()

    assert exception is None, exception


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
