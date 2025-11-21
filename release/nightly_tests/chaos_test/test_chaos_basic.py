import argparse
import json
import logging
import os
import random
import string
import time

import numpy as np

import ray
from ray._common.test_utils import wait_for_condition
from ray._private.test_utils import monitor_memory_usage
from ray.data._internal.progress_bar import ProgressBar
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy


def run_task_workload(total_num_cpus, smoke):
    """Run task-based workload that doesn't require object reconstruction."""

    @ray.remote(num_cpus=1, max_retries=-1)
    def task():
        def generate_data(size_in_kb=10):
            return np.zeros(1024 * size_in_kb, dtype=np.uint8)

        a = ""
        for _ in range(100000):
            a = a + random.choice(string.ascii_letters)
        return generate_data(size_in_kb=50)

    @ray.remote(num_cpus=1, max_retries=-1)
    def invoke_nested_task():
        time.sleep(0.8)
        return ray.get(task.remote())

    multiplier = 75
    # For smoke mode, run fewer tasks
    if smoke:
        multiplier = 1
    TOTAL_TASKS = int(total_num_cpus * 2 * multiplier)

    pb = ProgressBar("Chaos test", TOTAL_TASKS, "task")
    results = [invoke_nested_task.remote() for _ in range(TOTAL_TASKS)]
    pb.block_until_complete(results)
    pb.close()

    # Consistency check.
    wait_for_condition(
        lambda: (
            ray.cluster_resources().get("CPU", 0)
            == ray.available_resources().get("CPU", 0)
        ),
        timeout=60,
    )


def run_actor_workload(total_num_cpus, smoke):
    """Run actor-based workload.

    The test checks if actor restart -1 and task_retries -1 works
    as expected. It basically requires many actors to report the
    seqno to the centralized DB actor while there are failures.
    If at least once is guaranteed upon failures, this test
    shouldn't fail.
    """

    @ray.remote(num_cpus=0)
    class DBActor:
        def __init__(self):
            self.letter_dict = set()

        def add(self, letter):
            self.letter_dict.add(letter)

        def get(self):
            return self.letter_dict

    @ray.remote(num_cpus=1, max_restarts=-1, max_task_retries=-1)
    class ReportActor:
        def __init__(self, db_actor):
            self.db_actor = db_actor

        def add(self, letter):
            ray.get(self.db_actor.add.remote(letter))

    NUM_CPUS = int(total_num_cpus)
    multiplier = 2
    # For smoke mode, run fewer tasks
    if smoke:
        multiplier = 1
    TOTAL_TASKS = int(300 * multiplier)
    head_node_id = ray.get_runtime_context().get_node_id()
    db_actors = [
        DBActor.options(
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                node_id=head_node_id, soft=False
            )
        ).remote()
        for _ in range(NUM_CPUS)
    ]

    pb = ProgressBar("Chaos test", TOTAL_TASKS * NUM_CPUS, "task")
    actors = []
    for db_actor in db_actors:
        actors.append(ReportActor.remote(db_actor))
    results = []
    highest_reported_num = 0
    for a in actors:
        for _ in range(TOTAL_TASKS):
            results.append(a.add.remote(str(highest_reported_num)))
            highest_reported_num += 1
    pb.fetch_until_complete(results)
    pb.close()
    for actor in actors:
        ray.kill(actor)

    # Consistency check
    wait_for_condition(
        lambda: (
            ray.cluster_resources().get("CPU", 0)
            == ray.available_resources().get("CPU", 0)
        ),
        timeout=60,
    )
    letter_set = set()
    for db_actor in db_actors:
        letter_set.update(ray.get(db_actor.get.remote()))
    # Make sure the DB actor didn't lose any report.
    # If this assert fails, that means at least once actor task semantic
    # wasn't guaranteed.
    for i in range(highest_reported_num):
        assert str(i) in letter_set, i


def run_streaming_generator_workload(total_num_cpus, smoke):
    """Run streaming generator workload.

    The test runs 10 concurrent long-running streaming generators pinned to
    different nodes.
    This tests that streaming generators work correctly with retries when
    there are node failures or transient network failures.
    """

    @ray.remote(num_cpus=1, max_retries=-1)
    def streaming_generator(num_items, item_size_mb):
        """Generator that yields large plasma objects."""
        for i in range(num_items):
            data = np.zeros(item_size_mb * 1024 * 1024, dtype=np.uint8)
            yield (i, data)

    @ray.remote(num_cpus=1, max_retries=-1)
    def consume_streaming_generator(num_items, item_size_mb, node_name):
        """Task that spawns and consumes a streaming generator."""
        print(
            f"Starting streaming generator on {node_name}: "
            f"{num_items} items of {item_size_mb}MB each"
        )

        gen = streaming_generator.remote(num_items, item_size_mb)

        count = 0
        total_bytes = 0
        for idx, data in gen:
            count += 1
            total_bytes += data.nbytes

        print(
            f"Completed streaming generator on {node_name}: "
            f"{count} items, {total_bytes / (1024**3):.2f} GB"
        )
        return (count, total_bytes)

    alive_nodes = [n for n in ray.nodes() if n.get("Alive", False)]

    NUM_GENERATORS = len(alive_nodes)
    # For smoke mode, run fewer items
    if smoke:
        ITEMS_PER_GENERATOR = 10
    else:
        ITEMS_PER_GENERATOR = 300
    ITEM_SIZE_MB = 10

    print(
        f"Starting {NUM_GENERATORS} concurrent streaming generators "
        f"({ITEMS_PER_GENERATOR} items of {ITEM_SIZE_MB}MB each)"
    )
    print(
        f"Expected total data: "
        f"{NUM_GENERATORS * ITEMS_PER_GENERATOR * ITEM_SIZE_MB / 1024:.2f} GB"
    )

    # Launch generators on different nodes in parallel
    tasks = []
    for i in range(NUM_GENERATORS):
        node = alive_nodes[i % len(alive_nodes)]
        node_id = node["NodeID"]
        node_name = node.get("NodeName", node_id[:8])

        task = consume_streaming_generator.options(
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                node_id=node_id, soft=True
            )
        ).remote(ITEMS_PER_GENERATOR, ITEM_SIZE_MB, node_name)
        tasks.append(task)

    results = ray.get(tasks)

    total_items = sum(count for count, _ in results)
    total_bytes = sum(bytes_val for _, bytes_val in results)

    print("All generators completed:")
    print(
        f"  Total items: {total_items} (expected {NUM_GENERATORS * ITEMS_PER_GENERATOR})"
    )
    print(f"  Total data: {total_bytes / (1024**3):.2f} GB")

    # Verify all items were received
    assert (
        total_items == NUM_GENERATORS * ITEMS_PER_GENERATOR
    ), f"Expected {NUM_GENERATORS * ITEMS_PER_GENERATOR} items, got {total_items}"

    # Consistency check
    wait_for_condition(
        lambda: (
            ray.cluster_resources().get("CPU", 0)
            == ray.available_resources().get("CPU", 0)
        ),
        timeout=60,
    )


def run_object_ref_borrowing_workload(total_num_cpus, smoke):
    """Run object ref borrowing workload.

    This test checks that borrowed refs
    remain valid even with node failures or transient network failures.
    """

    @ray.remote(num_cpus=1, max_retries=-1)
    def create_object(size_mb):
        data = np.zeros(size_mb * 1024 * 1024, dtype=np.uint8)
        return data

    @ray.remote(num_cpus=1, max_retries=-1)
    def borrow_object(borrowed_refs):
        """Receives a list of borrowed refs, gets the first one, returns size."""
        data = ray.get(borrowed_refs[0])
        return len(data)

    # For smoke mode, run fewer iterations
    if smoke:
        NUM_ITERATIONS = 10
    else:
        NUM_ITERATIONS = 1000
    OBJECT_SIZE_MB = 10

    print(f"Starting {NUM_ITERATIONS} task pairs (A creates, B borrows)")
    print(f"Object size: {OBJECT_SIZE_MB}MB per object")
    print(f"Expected total data: {NUM_ITERATIONS * OBJECT_SIZE_MB / 1024:.2f} GB")

    refs = []
    for i in range(NUM_ITERATIONS):
        ref = create_object.remote(OBJECT_SIZE_MB)
        refs.append(borrow_object.remote([ref]))
    sizes = ray.get(refs)
    num_completed = len(sizes)
    total_bytes = sum(sizes)

    print("All tasks completed:")
    print(f"  Tasks completed: {num_completed} (expected {NUM_ITERATIONS})")
    print(f"  Total data processed: {total_bytes / (1024**3):.2f} GB")

    # Assertions
    assert (
        num_completed == NUM_ITERATIONS
    ), f"Expected {NUM_ITERATIONS} completions, got {num_completed}"
    expected_bytes = NUM_ITERATIONS * OBJECT_SIZE_MB * 1024 * 1024
    assert (
        total_bytes == expected_bytes
    ), f"Expected {expected_bytes} bytes, got {total_bytes}"

    # Consistency check
    wait_for_condition(
        lambda: (
            ray.cluster_resources().get("CPU", 0)
            == ray.available_resources().get("CPU", 0)
        ),
        timeout=60,
    )


def run_placement_group_workload(total_num_cpus, smoke):
    raise NotImplementedError


def parse_script_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--node-kill-interval", type=int, default=60)
    parser.add_argument("--workload", type=str)
    parser.add_argument("--smoke", action="store_true")
    return parser.parse_known_args()


def main():
    """Test task/actor/placement group basic chaos test.

    It tests the following scenarios:
    1. Raylet failures: This is done by an actor calling Raylet's Shutdown RPC.
    2. EC2 instance termination: This is done by an actor terminating
       EC2 instances via AWS SDK.

    Currently, the test runs in 3 steps. Each step records the
    peak memory usage to observe the memory usage while there
    are node failures.

    Step 1: Warm up the cluster. It is needed to pre-start workers
        if necessary.

    Step 2: Start the test without a failure.

    Step 3: Start the test with constant node failures.
    """
    args, _ = parse_script_args()
    logging.info("Received arguments: {}".format(args))
    ray.init(address="auto")
    total_num_cpus = ray.cluster_resources()["CPU"]
    total_nodes = 0
    for n in ray.nodes():
        if n["Alive"]:
            total_nodes += 1
    monitor_actor = monitor_memory_usage()

    workload = None
    if args.workload == "tasks":
        workload = run_task_workload
    elif args.workload == "actors":
        workload = run_actor_workload
    elif args.workload == "pg":
        workload = run_placement_group_workload
    elif args.workload == "streaming":
        workload = run_streaming_generator_workload
    elif args.workload == "borrowing":
        workload = run_object_ref_borrowing_workload
    else:
        assert False

    # Step 1
    print("Warm up... Prestarting workers if necessary.")
    start = time.time()
    workload(total_num_cpus, args.smoke)
    print(f"Runtime when warm up: {time.time() - start}")

    # Step 2
    print("Running without failures")
    start = time.time()
    workload(total_num_cpus, args.smoke)
    print(f"Runtime when there are no failures: {time.time() - start}")
    used_gb, usage = ray.get(monitor_actor.get_peak_memory_info.remote())
    print("Memory usage without failures.")
    print(f"Peak memory usage: {round(used_gb, 2)}GB")
    print(f"Peak memory usage per processes:\n {usage}")

    # Step 3
    print("Running with failures")
    start = time.time()
    node_killer = None
    try:
        node_killer = ray.get_actor(
            "ResourceKiller", namespace="release_test_namespace"
        )
        node_killer.run.remote()
        print("ResourceKiller found and started")
    except ValueError:
        print(
            "ResourceKiller not found - assuming external chaos injection "
            "(e.g., iptables, network failures)"
        )

    workload(total_num_cpus, args.smoke)
    print(f"Runtime when there are many failures: {time.time() - start}")

    if node_killer is not None:
        print(f"Total node failures: {ray.get(node_killer.get_total_killed.remote())}")
        node_killer.stop_run.remote()

    used_gb, usage = ray.get(monitor_actor.get_peak_memory_info.remote())
    print("Memory usage with failures.")
    print(f"Peak memory usage: {round(used_gb, 2)}GB")
    print(f"Peak memory usage per processes:\n {usage}")

    # Report the result.
    ray.get(monitor_actor.stop_run.remote())
    if node_killer is not None:
        print(
            "Total number of killed nodes: "
            f"{ray.get(node_killer.get_total_killed.remote())}"
        )
    with open(os.environ["TEST_OUTPUT_JSON"], "w") as f:
        f.write(
            json.dumps(
                {
                    "_peak_memory": round(used_gb, 2),
                    "_peak_process_memory": usage,
                }
            )
        )


main()
