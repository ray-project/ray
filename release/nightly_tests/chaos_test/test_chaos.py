import argparse
import json
import logging
import os
import time

import ray
from ray._private.test_utils import monitor_memory_usage

from task_workload import run_task_workload
from actor_workload import run_actor_workload
from streaming_generator_workload import run_streaming_generator_workload
from object_ref_borrowing_workload import run_object_ref_borrowing_workload


def parse_script_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--node-kill-interval", type=int, default=60)
    parser.add_argument("--workload", type=str)
    parser.add_argument("--smoke", action="store_true")
    parser.add_argument("--disable-resource-killer", action="store_true")
    return parser.parse_known_args()


def main():
    """Test task/actor/streaming generator/object ref borrowing chaos test.

    It tests the following scenarios:
    1. Raylet failures: Done by an actor calling Raylet's Shutdown RPC.
    2. EC2 instance termination: Done by an actor terminating
       EC2 instances via AWS SDK.
    3. Network failures: Done by injecting network failures via iptables or env variables.
    """
    args, _ = parse_script_args()
    logging.info("Received arguments: {}".format(args))
    ray.init(address="auto")
    total_num_cpus = ray.cluster_resources()["CPU"]
    monitor_actor = monitor_memory_usage()

    # Select the workload based on the argument
    workload = None
    if args.workload == "tasks":
        workload = run_task_workload
    elif args.workload == "actors":
        workload = run_actor_workload
    elif args.workload == "streaming":
        workload = run_streaming_generator_workload
    elif args.workload == "borrowing":
        workload = run_object_ref_borrowing_workload
    else:
        assert False

    node_killer = None

    if args.disable_resource_killer:
        print("ResourceKiller disabled")
    else:
        node_killer = ray.get_actor(
            "ResourceKiller", namespace="release_test_namespace"
        )
        node_killer.run.remote()
        print("ResourceKiller started")

    start = time.time()
    workload(total_num_cpus, args.smoke)
    runtime_s = time.time() - start
    runtime_s = round(runtime_s, 2)
    print(f"Total runtime: {runtime_s}")

    if node_killer is not None:
        node_killer.stop_run.remote()
        print(f"Total node failures: {ray.get(node_killer.get_total_killed.remote())}")

    used_gb, usage = ray.get(monitor_actor.get_peak_memory_info.remote())
    used_gb = round(used_gb, 2)
    print("Memory usage with failures.")
    print(f"Peak memory usage: {used_gb}GB")
    print(f"Peak memory usage per processes:\n {usage}")

    ray.get(monitor_actor.stop_run.remote())

    results = {
        "time": runtime_s,
        "_peak_memory": used_gb,
        "_peak_process_memory": usage,
    }

    results["perf_metrics"] = [
        {
            "perf_metric_name": f"chaos_{args.workload}_runtime_s",
            "perf_metric_value": runtime_s,
            "perf_metric_type": "LATENCY",
        },
        {
            "perf_metric_name": f"chaos_{args.workload}_peak_memory_gb",
            "perf_metric_value": used_gb,
            "perf_metric_type": "MEMORY",
        },
    ]

    with open(os.environ["TEST_OUTPUT_JSON"], "w") as f:
        json.dump(results, f)


main()
