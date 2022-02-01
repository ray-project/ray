import argparse
import os
import random
import string
import time
import json
import logging

import numpy as np
import ray

from ray.data.impl.progress_bar import ProgressBar
from ray._private.test_utils import monitor_memory_usage, wait_for_condition


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
    # For smoke mode, run less number of tasks
    if smoke:
        multiplier = 1
    TOTAL_TASKS = int(total_num_cpus * 2 * multiplier)

    pb = ProgressBar("Chaos test", TOTAL_TASKS)
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
    # For smoke mode, run less number of tasks
    if smoke:
        multiplier = 1
    TOTAL_TASKS = int(300 * multiplier)
    current_node_ip = ray.worker.global_worker.node_ip_address
    db_actors = [
        DBActor.options(resources={f"node:{current_node_ip}": 0.001}).remote()
        for _ in range(NUM_CPUS)
    ]

    pb = ProgressBar("Chaos test", TOTAL_TASKS * NUM_CPUS)
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

    Currently, it only tests node failures scenario.
    Node failures are implemented by an actor that keeps calling
    Raylet's KillRaylet RPC.

    Ideally, we should setup the infra to cause machine failures/
    network partitions/etc., but we don't do that for now.

    In the short term, we will only test gRPC network delay +
    node failures.

    Currently, the test runs 3 steps. Each steps records the
    peak memory usage to observe the memory usage while there
    are node failures.

    Step 1: Warm up the cluster. It is needed to pre-start workers
        if necessary.

    Step 2: Start the test without a failure.

    Step 3: Start the test with constant node failures.
    """
    args, unknown = parse_script_args()
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
    else:
        assert False

    # Step 1
    print("Warm up... Prestarting workers if necessary.")
    start = time.time()
    workload(total_num_cpus, args.smoke)

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
    node_killer = ray.get_actor("node_killer", namespace="release_test_namespace")
    node_killer.run.remote()
    workload(total_num_cpus, args.smoke)
    print(f"Runtime when there are many failures: {time.time() - start}")
    print(
        f"Total node failures: "
        f"{ray.get(node_killer.get_total_killed_nodes.remote())}"
    )
    node_killer.stop_run.remote()
    used_gb, usage = ray.get(monitor_actor.get_peak_memory_info.remote())
    print("Memory usage with failures.")
    print(f"Peak memory usage: {round(used_gb, 2)}GB")
    print(f"Peak memory usage per processes:\n {usage}")

    # Report the result.
    ray.get(monitor_actor.stop_run.remote())
    print(
        "Total number of killed nodes: "
        f"{ray.get(node_killer.get_total_killed_nodes.remote())}"
    )
    with open(os.environ["TEST_OUTPUT_JSON"], "w") as f:
        f.write(
            json.dumps(
                {
                    "success": 1,
                    "_peak_memory": round(used_gb, 2),
                    "_peak_process_memory": usage,
                }
            )
        )


main()
