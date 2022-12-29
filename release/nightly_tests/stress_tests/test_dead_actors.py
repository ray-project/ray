#!/usr/bin/env python

import argparse
import json
import logging
import numpy as np
import os
import sys
import time

import ray

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ray.init(address="auto")


@ray.remote
class Child(object):
    def __init__(self, death_probability):
        self.death_probability = death_probability

    def ping(self):
        # Exit process with some probability.
        exit_chance = np.random.rand()
        if exit_chance > self.death_probability:
            sys.exit(-1)


@ray.remote
class Parent(object):
    def __init__(self, num_children, death_probability):
        self.death_probability = death_probability
        self.children = [Child.remote(death_probability) for _ in range(num_children)]

    def ping(self, num_pings):
        children_outputs = []
        for _ in range(num_pings):
            children_outputs += [child.ping.remote() for child in self.children]
        try:
            ray.get(children_outputs)
        except Exception:
            # Replace the children if one of them died.
            self.__init__(len(self.children), self.death_probability)

    def kill(self):
        # Clean up children.
        ray.get([child.__ray_terminate__.remote() for child in self.children])


def parse_script_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--num-nodes", type=int, default=100)
    parser.add_argument("--num-parents", type=int, default=10)
    parser.add_argument("--num-children", type=int, default=10)
    parser.add_argument("--death-probability", type=int, default=0.95)
    return parser.parse_known_args()


if __name__ == "__main__":
    args, unknown = parse_script_args()
    result = {"success": 0}
    # These numbers need to correspond with the autoscaler config file.
    # The number of remote nodes in the autoscaler should upper bound
    # these because sometimes nodes fail to update.
    num_remote_nodes = args.num_nodes
    num_parents = args.num_parents
    num_children = args.num_children
    death_probability = args.death_probability

    # Wait until the expected number of nodes have joined the cluster.
    num_nodes = len(ray.nodes())
    assert (
        num_nodes >= num_remote_nodes + 1
    ), f"Expect {num_remote_nodes+1}, but only {num_nodes} joined."
    logger.info(
        "Nodes have all joined. There are %s resources.", ray.cluster_resources()
    )

    parents = [
        Parent.remote(num_children, death_probability) for _ in range(num_parents)
    ]

    start = time.time()
    loop_times = []
    for i in range(100):
        loop_start = time.time()
        ray.get([parent.ping.remote(10) for parent in parents])

        # Kill a parent actor with some probability.
        exit_chance = np.random.rand()
        if exit_chance > death_probability:
            parent_index = np.random.randint(len(parents))
            parents[parent_index].kill.remote()
            parents[parent_index] = Parent.remote(num_children, death_probability)

        logger.info("Finished trial %s", i)
        loop_times.append(time.time() - loop_start)

    print("Finished in: {}s".format(time.time() - start))
    print("Average iteration time: {}s".format(sum(loop_times) / len(loop_times)))
    print("Max iteration time: {}s".format(max(loop_times)))
    print("Min iteration time: {}s".format(min(loop_times)))
    result["total_time"] = time.time() - start
    result["avg_iteration_time"] = sum(loop_times) / len(loop_times)
    result["max_iteration_time"] = max(loop_times)
    result["min_iteration_time"] = min(loop_times)
    result["success"] = 1
    if os.environ.get("IS_SMOKE_TEST") != "1":
        result["perf_metrics"] = [
            {
                "perf_metric_name": "avg_iteration_time",
                "perf_metric_value": result["avg_iteration_time"],
                "perf_metric_type": "LATENCY",
            }
        ]
    print("PASSED.")

    with open(os.environ["TEST_OUTPUT_JSON"], "w") as f:
        f.write(json.dumps(result))
