import argparse
import numpy as np
import time
import json
import os

from tabulate import tabulate

import ray
from ray._private.test_utils import wait_for_condition
from ray.util import scheduling_strategies

_RELEASE_TEST_OUTPUT_ENV_VARIABLE_NAME = "TEST_OUTPUT_JSON"

"""
    For a given instance type, this test will try to maximize object transfer throughput
    for fixed-sized objects on two nodes.

    The following environment need to be setup for this test to run properly:
        - RAY_object_spilling_threshold=1
        - RAY_experimental_object_manager_enable_multiple_connections=true

    The test takes the following parameters:
        - num-connections (for reporting)
        - node-num-cpus
        - node-memory-bytes
        - network-bandwidth-bytes
        - num-actors (comma-separated list)
        - object-sizes (comma-separated list)
        - batch-sizes (comma-separated list)

    The test has the following parameters in the code:
        - num_actors = the number of actors created on each node [2, log_2(node-num-cpus)]
        - object_sizes = the different object sizes the test is run on [1M, 64M, 512M, 1024M]
        - batch_size = the number of concurrent objects each actor requests = [2, 4, 8]

    The test has the following stages
        - for each object_size, prepopulate the object store with all fixed size objects
        - for each num_actors, create the actors and assign them disjoint sections of objects to pull from the remote node
        - warm up the actors, run the test with different batch sizes

    The test avoids spilling by carefully sizing the object store, the prefill buffer for pushes, and the buffer used for transfers
    for each possible instance type:

    There are a few tricky edge cases for this test which are worth documenting. How do we avoid spilling and how to do ensure that
    objects are transferred from a remote node and not read from plasma?

    This test is configured for the following instance types with the object store being partitioned into a prefill
    and transfer sections

        +----------------+--------+-------------+-------------------+-------------------------+------------------------------+
        |   Instance     | # vCPUs| Memory (GB) | Object Store (GB) | Object Store Prefill (GB)| Object Store Transfer (GB)  |
        +----------------+--------+-------------+-------------------+-------------------------+------------------------------+
        | m7i.8xlarge    |   32   |     128     |         96        |           72            |             24               |
        | m7i.16xlarge   |   64   |     256     |        192        |          144            |             48               |
        | m7i.24xlarge   |   96   |     384     |        288        |          216            |             72               |
        | m7i.48xlarge   |  192   |     768     |        576        |          432            |            144               |
        +----------------+--------+-------------+-------------------+-------------------------+------------------------------+

    Each test has an upper limit on how many much data can be in scope (and not evicted) at once: num_actors * object_size * batch_size.
    For example:

        8 Actors * 4 Objects per Batch * 512MB Objects = 16GB

    As long as the Object Store Transfer section is larger than 16GB, the node can run this test without spilling.

    This test does not guarantee that a node will always read from Plasma. However, it's highly likely because
        - Each actor gets a disjoint set of remote object references to pull (meaning two actors will not pull the same object)
        - Each actor cycles through its set of object references and they should get garbage collected before an actor returns
        to the same ref.

"""


def kill_all_actors_on_nodes(node_to_actor_refs):
    for node in node_to_actor_refs:
        for actor in node_to_actor_refs[node]:
            # TODO(irabbani): ray.kill is blocking, I could probably make this faster
            actor.delete_remote_object_refs.remote()
            ray.kill(actor)


def get_worker_node_resources():
    """Returns a dict of node_id -> { CPU, memory, object_store_memory } of all worker nodes"""
    head_node_id = ray.get_runtime_context().get_node_id()
    node_resources = {}
    for node in ray.nodes():
        if node["NodeID"] != head_node_id:
            node_resources[node["NodeID"]] = {
                "CPU": node["Resources"]["CPU"],
                "memory": node["Resources"]["memory"],
                "object_store_memory": node["Resources"]["object_store_memory"],
            }
    return node_resources


def assert_worker_nodes_have_same_cpu_memory(node_resources):
    """Asserts worker nodes have the same number of CPUs and memory and returns worker nodes."""
    cpu_memory_set = {(res["CPU"], res["memory"]) for res in node_resources.values()}
    assert (
        len(cpu_memory_set) == 1
    ), f"{__file__} test expects both worker nodes to have the same CPU and memory. Found {cpu_memory_set}"


def gc_and_wait_for_empty_object_store_on_worker_nodes(node_ids):
    """Trigger global GC in the entire cluster and wait for the object store to become empty again."""
    # global_gc()
    # schedule tasks on each node to make sure the node is empty before reusing the node
    node_to_empty_object_store_future = {}
    for node_id in node_ids:
        static_task_scheduling_strategy = (
            scheduling_strategies.NodeAffinitySchedulingStrategy(node_id, soft=False)
        )
        # TODO(irabbani): this can be done asynchronously by returning the node_id")
        node_to_empty_object_store_future[node_id] = ray.get(
            wait_for_empty_object_store.options(
                scheduling_strategy=static_task_scheduling_strategy
            ).remote()
        )


@ray.remote
def prefill_object_store(num_objects: int, obj_size_bytes: int):
    """Prefill the object store on a node with fixed size objects."""
    obj_refs = []
    total_bytes = 0
    for _ in range(num_objects):
        obj_refs.append(ray.put(np.ones(obj_size_bytes, dtype=np.int8)))
        total_bytes += obj_size_bytes
    print(
        f"Finished Prefilling Object Store with {total_bytes=}, {num_objects=}, {obj_size_bytes=}"
    )
    return obj_refs


@ray.remote
def wait_for_empty_object_store():
    core_worker = ray._private.worker.global_worker.core_worker

    def check_empty():
        # object_store_size = core_worker.get_memory_store_size()
        ref_counts = core_worker.get_all_reference_counts()
        # pprint(ref_counts)
        print(f"{len(ref_counts)}")
        return len(ref_counts) == 0

    wait_for_condition(check_empty, timeout=10, retry_interval_ms=1000)
    return True


@ray.remote
class BenchmarkActor:
    """An actor that fetches remote objects indefinitely for a specified period of time"""

    def __init__(self, remote_refs):
        self.object_refs = remote_refs

    def is_created(self):
        return True

    def transfer_objects_benchmark(self, duration_ms, batch_size):
        """Will continuously transfer objects for the duration and return the number of bytes transferred.

        Note: This could be an overestimate because it doesn't check to see if the object is already in plasma. When configured
        correctly, the estimate should be fairly close. It can also be an underestimate the benchmark runs out of time and some
        objects in the batch have been fetched, but not all of them.
        """
        assert batch_size <= len(
            self.object_refs
        ), f"Cannot run benchmark where {batch_size=} > num_total_objects_to_pull={len(self.object_refs)}"
        curr_ns = time.monotonic_ns()
        end_ns = curr_ns + (duration_ms * 1_000_000)
        num_transferred = 0
        curr_index = 0
        total_objs = len(self.object_refs)
        while curr_ns < end_ns:
            next_objs = [
                self.object_refs[(curr_index + i) % total_objs]
                for i in range(batch_size)
            ]
            try:
                ray.get(next_objs, timeout=(end_ns - curr_ns) / 1_000_000_000)
            except ray.exceptions.GetTimeoutError:
                # TODO(irabbani): I can make this more accurate by checking which objects have been fetched successfully.
                # It might make a noticeable difference for large object sizes and batch sizes with small benchmark durations.
                print(f"HELLO: {type(num_transferred)}")
                return num_transferred
            curr_index = (curr_index + batch_size) % total_objs
            num_transferred += batch_size
            curr_ns = time.monotonic_ns()
        print(f"HELLO: {type(num_transferred)}")
        return num_transferred

    def delete_remote_object_refs(self):
        self.object_refs = []


def prefill_object_store_for_nodes(node_id_to_resources, object_size, prefill_pct):
    """Fills the object store on each node with fixed-sized objects.

    Returns a dictionary of node_id -> object ref that resolves to a list of remote object refs
    """
    node_id_to_object_refs = {}
    for node_id in node_id_to_resources:
        static_task_scheduling_strategy = (
            scheduling_strategies.NodeAffinitySchedulingStrategy(node_id, soft=False)
        )
        # TODO(irabbani): this can be done asynchronously by returning the node_id.
        object_store_memory_bytes = node_id_to_resources[node_id]["object_store_memory"]
        num_objects = int(object_store_memory_bytes * prefill_pct) // object_size
        print(f"Calling worker prefill {num_objects=}, {object_store_memory_bytes=}")
        node_id_to_object_refs[node_id] = ray.get(
            prefill_object_store.options(
                scheduling_strategy=static_task_scheduling_strategy
            ).remote(num_objects, object_size)
        )
    return node_id_to_object_refs


def create_actors_for_nodes(node_ids, num_actors, node_id_to_object_refs):
    """Equally distribute remote objects between all actors created on the node."""
    node_id_to_actor_refs = {}
    for node_id in node_ids:
        # create node affinity strategy to pin actor to nodeq
        pin_actor_to_node = scheduling_strategies.NodeAffinitySchedulingStrategy(
            node_id, soft=False
        )
        # remote object_refs
        remote_object_refs = []
        for other_node_id in node_id_to_object_refs:
            if other_node_id != node_id:
                remote_object_refs.extend(node_id_to_object_refs[other_node_id])
        # number of objects alotted to each actor
        num_objs_per_actor: int = len(remote_object_refs) // num_actors
        actor_creation_object_refs = []
        actor_refs = []
        for i in range(num_actors):
            start_index = i * num_objs_per_actor
            end_index = start_index + num_objs_per_actor
            actor_obj_refs = remote_object_refs[start_index:end_index]
            actor = BenchmarkActor.options(
                scheduling_strategy=pin_actor_to_node
            ).remote(actor_obj_refs)
            actor_creation_object_refs.append(actor.is_created.remote())
            actor_refs.append(actor)
        # wait for the objects to be created before running the benchmark
        ray.get(actor_creation_object_refs)
        print(f"Created {len(actor_refs)} Actors for {node_id=}.")
        node_id_to_actor_refs[node_id] = actor_refs
    return node_id_to_actor_refs


def run_benchmark(node_id_to_actor_refs, duration_ms, batch_size):
    """Runs benchmark for all actors for the given duration. Returns dict of node_id -> total_num_objects_transferred"""
    benchmark_futures_and_node_ids = []
    for node_id in node_id_to_actor_refs:
        for actor in node_id_to_actor_refs[node_id]:
            benchmark_futures_and_node_ids.append(
                (
                    node_id,
                    actor.transfer_objects_benchmark.remote(duration_ms, batch_size),
                )
            )
    node_id_to_total_objects_transferred = {}
    # TODO(irabbani): this could be faster with ray.wait + ray.get or by returning the node_id from the remote call.
    for node_id, future in benchmark_futures_and_node_ids:
        node_id_to_total_objects_transferred[node_id] = ray.get(future)
    return node_id_to_total_objects_transferred


class TestResult:
    def __init__(
        self,
        num_connections: int,
        node_num_cpus: int,
        node_memory_bytes: int,
        node_network_bytes: int,
        num_actors: int,
        object_size: int,
        batch_size: int,
        throughput_bytes_s: int,
    ):
        self.num_connections = num_connections
        self.node_num_cpus = node_num_cpus
        self.node_memory_bytes = node_memory_bytes
        self.node_network_bytes = node_network_bytes
        self.num_actors = num_actors
        self.object_size = object_size
        self.batch_size = batch_size
        self.throughput_bytes_s = throughput_bytes_s

    def get_canonical_tuple(self):
        """Returns the values in canonical order to print as a table."""
        return (
            self.num_connections,
            self.node_num_cpus,
            self.node_memory_bytes,
            self.node_network_bytes,
            self.num_actors,
            self.object_size,
            self.batch_size,
            self.throughput_bytes_s,
        )

    @staticmethod
    def get_canonical_headers():
        return [
            "num_connections",
            "node_num_cpus",
            "node_memory_bytes",
            "node_network_bandwidth_bytes_s",
            "object_size",
            "num_actors",
            "batch_size",
            "throughput_bytes_s",
        ]


def main():

    OBJECT_STORE_PREFILL_PCT = 0.65

    WARMUP_DURATION_MS = 60 * 1000
    WARMUP_BATCH_SIZE = 2

    TEST_DURATION_MS = 180 * 1000
    # TEST_NUM_ITERS = 1
    TEST_COOLDOWN_MS = 3 * 1000

    parser = argparse.ArgumentParser(
        description="Parse system configuration arguments."
    )

    parser.add_argument(
        "--num-connections", type=int, required=True, help="Number of connections."
    )
    parser.add_argument(
        "--node-num-cpus", type=int, required=True, help="Number of CPUs per node."
    )
    parser.add_argument(
        "--node-memory-bytes", type=int, required=True, help="Memory per node in bytes."
    )
    parser.add_argument(
        "--node-network-bandwidth-bytes",
        type=int,
        required=True,
        help="Network bandwidth in bytes.",
    )
    parser.add_argument(
        "--num-actors",
        type=str,
        required=True,
        help="Comma-separated list of number of actors.",
    )
    parser.add_argument(
        "--object-sizes",
        type=str,
        required=True,
        help="Comma-separated list of object sizes.",
    )
    parser.add_argument(
        "--batch-sizes",
        type=str,
        required=True,
        help="Comma-separated list of batch sizes.",
    )

    args = parser.parse_args()

    test_num_connections = args.num_connections
    test_node_num_cpus = args.node_num_cpus
    test_node_memory_bytes = args.node_memory_bytes
    test_node_network_bandwidth_bytes = args.node_network_bandwidth_bytes

    # Parse comma-separated lists into lists of integers
    num_actors = [int(x) for x in args.num_actors.split(",") if x]
    object_sizes = [int(x) for x in args.object_sizes.split(",") if x]
    batch_sizes = [int(x) for x in args.batch_sizes.split(",") if x]

    ray.init(address="auto")

    assert (
        len(ray.nodes()) == 3
    ), f"{__file__} requires a 3 node cluster: 1 head node, 2 worker nodes"
    worker_node_id_to_resources = get_worker_node_resources()
    assert_worker_nodes_have_same_cpu_memory(worker_node_id_to_resources)

    results = []

    for object_size in object_sizes:
        obj_refs_by_node = prefill_object_store_for_nodes(
            worker_node_id_to_resources, object_size, OBJECT_STORE_PREFILL_PCT
        )

        for num_actor in num_actors:
            worker_node_id_to_actors = create_actors_for_nodes(
                worker_node_id_to_resources.keys(), num_actor, obj_refs_by_node
            )

            # warm up the object manager
            run_benchmark(
                worker_node_id_to_actors, WARMUP_DURATION_MS, WARMUP_BATCH_SIZE
            )

            for batch_size in batch_sizes:

                node_id_to_total_objects_transferred_per_second = run_benchmark(
                    worker_node_id_to_actors, TEST_DURATION_MS, batch_size
                )
                total_objects_transferred = sum(
                    node_id_to_total_objects_transferred_per_second.values()
                )
                print(total_objects_transferred)
                num_nodes = len(worker_node_id_to_resources.keys())
                throughput_bytes_s = (
                    (total_objects_transferred / num_nodes) * object_size
                ) / (TEST_DURATION_MS / 1000)
                # throughput_gigabytes_s = throughput_bytes_s / (1024**3)
                results.append(
                    TestResult(
                        num_connections=test_num_connections,
                        node_num_cpus=test_node_num_cpus,
                        node_memory_bytes=test_node_memory_bytes,
                        node_network_bytes=test_node_network_bandwidth_bytes,
                        object_size=object_size,
                        num_actors=num_actor,
                        batch_size=batch_size,
                        throughput_bytes_s=throughput_bytes_s,
                    )
                )

                time.sleep(TEST_COOLDOWN_MS // 1000)

            kill_all_actors_on_nodes(worker_node_id_to_actors)
            del worker_node_id_to_actors

        del obj_refs_by_node
        gc_and_wait_for_empty_object_store_on_worker_nodes(
            worker_node_id_to_resources.keys()
        )

    if is_release_test():
        create_release_test_output_file(results)

    print(
        tabulate(
            [result.get_canonical_tuple() for result in results],
            headers=TestResult.get_canonical_headers(),
            tablefmt="grid",
        )
    )


def is_release_test():
    return _RELEASE_TEST_OUTPUT_ENV_VARIABLE_NAME in os.environ


def get_release_test_metric_name(result: TestResult):
    """The metric name format is ObjectSize_NumActors_BatchSize."""
    object_size_mb = int(result.object_size / (1024**2))
    return f"{object_size_mb}MBObjSize_{result.num_actors}Actors_{result.batch_size}BatchSize"


def create_release_test_output_file(results: list[TestResult]):
    with open(os.environ[_RELEASE_TEST_OUTPUT_ENV_VARIABLE_NAME], "w") as out_file:
        perf_metrics = []
        for result in results:
            perf_metrics.append(
                {
                    "perf_metric_name": get_release_test_metric_name(result),
                    "perf_metric_value": result.throughput_bytes_s,
                    "perf_metric_type": "THROUGHPUT",
                }
            )
        json.dump(out_file, {"perf_metrics": perf_metrics})


if __name__ == "__main__":
    # import sys
    # print(__file__)
    # sys.argv = [
    #     __file__,
    #     "--num-connections", "1",
    #     "--node-num-cpus", "64",
    #     "--node-memory-bytes", f"{128 * (1024**3)}",
    #     "--node-network-bandwidth-bytes", f"{int(12.5 * (1024**3)) // 8}",
    #     "--num-actors", "1,2,4",
    #     "--object-sizes", f"{1024**2},{64 * (1024**2)},{512 * (1024**2)},{1024 * (1024**2)}",
    #     # "--object-sizes", f"{64 * (1024**2)}",
    #     "--batch-sizes", "2,4,8"
    # ]
    main()
