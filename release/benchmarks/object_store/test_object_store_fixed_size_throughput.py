import argparse
import numpy as np
import time
import json
import os
from collections import defaultdict

from tabulate import tabulate

import ray
from ray._private.test_utils import wait_for_condition
from ray.util import scheduling_strategies

_RELEASE_TEST_OUTPUT_ENV_VARIABLE_NAME = "TEST_OUTPUT_JSON"

"""
    For a cluster with two identical worker nodes, this test will try to measure the maximum, continuous,
    bidirectional object transfer throughput for fixed-sized objects.

    The following environment need to be setup for this test to run properly:
        - RAY_object_spilling_threshold=1
        - RAY_DEFAULT_OBJECT_STORE_MEMORY_PROPORTION=0.75

    The test has the following parameters:
        - num_actors = the number of actors created on each node.
        - object_sizes = the different object sizes the test is run on.
        - batch_size = the number of concurrent objects each actor requests.

    The test has the following stages
        - for each object_size, prepopulate the object store with all fixed size objects.
        - for each num_actors, create the actors and assign them disjoint sections of objects to pull from the remote node.
        - warm up the nodes with the new objects.
        - run the test with different batch sizes.

    The test avoids spilling by carefully sizing the object store, the prefill buffer for pushes, and the buffer used for transfers
    for each possible instance type:

    This test is configured for the following instance types with the object store being partitioned into a prefill
    and transfer sections:

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
    futures = []
    for node_id in node_ids:
        static_task_scheduling_strategy = (
            scheduling_strategies.NodeAffinitySchedulingStrategy(node_id, soft=False)
        )
        futures.append(
            wait_for_empty_object_store.options(
                scheduling_strategy=static_task_scheduling_strategy
            ).remote()
        )
    ray.get(futures)


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
        ref_counts = core_worker.get_all_reference_counts()
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
                return num_transferred
            curr_index = (curr_index + batch_size) % total_objs
            num_transferred += batch_size
            curr_ns = time.monotonic_ns()
        return num_transferred

    def delete_remote_object_refs(self):
        self.object_refs = []


def prefill_object_store_for_nodes(node_id_to_resources, object_size, prefill_pct):
    """Fills the object store on each node with fixed-sized objects.

    Returns a dictionary of node_id -> object ref that resolves to a list of remote object refs
    """
    node_id_to_futures = {}
    node_id_to_object_refs = {}
    for node_id in node_id_to_resources:
        static_task_scheduling_strategy = (
            scheduling_strategies.NodeAffinitySchedulingStrategy(node_id, soft=False)
        )
        object_store_memory_bytes = node_id_to_resources[node_id]["object_store_memory"]
        num_objects = int(object_store_memory_bytes * prefill_pct) // object_size
        print(f"Calling worker prefill {num_objects=}, {object_store_memory_bytes=}")
        node_id_to_futures[node_id] = prefill_object_store.options(
            scheduling_strategy=static_task_scheduling_strategy
        ).remote(num_objects, object_size)
    for node_id in node_id_to_futures:
        node_id_to_object_refs[node_id] = ray.get(node_id_to_futures[node_id])
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
    node_id_to_total_objects_transferred = defaultdict(int)
    # TODO(irabbani): this could be faster with ray.wait + ray.get or by returning the node_id from the remote call.
    for node_id, future in benchmark_futures_and_node_ids:
        node_id_to_total_objects_transferred[node_id] += ray.get(future)
    return node_id_to_total_objects_transferred


class TestResult:
    def __init__(
        self,
        num_actors: int,
        object_size: int,
        batch_size: int,
        throughput_bytes_s: float,
    ):
        self.num_actors = num_actors
        self.object_size = object_size
        self.batch_size = batch_size
        self.throughput_bytes_s = throughput_bytes_s

    def get_canonical_tuple(self):
        """Returns the values in canonical order to print as a table."""
        return (
            self.object_size,
            self.num_actors,
            self.batch_size,
            self.throughput_bytes_s,
        )

    @staticmethod
    def get_canonical_headers():
        return [
            "object_size",
            "num_actors",
            "batch_size",
            "throughput_bytes_s",
        ]


def main():

    OBJECT_STORE_PREFILL_PCT = 0.65

    WARMUP_DURATION_MS = 30 * 1000
    WARMUP_BATCH_SIZE = 2

    TEST_DURATION_MS = 120 * 1000
    TEST_COOLDOWN_MS = 1 * 1000

    parser = argparse.ArgumentParser(
        description="Parse system configuration arguments."
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
        help="Comma-separated list of batch sizes where each batch size is how many objects an actor pulls concurrently.",
    )

    args = parser.parse_args()

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
                results.append(
                    TestResult(
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
    """Creates a metric for each test results and writes them to a json file."""
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
    main()
