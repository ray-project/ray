"""Test cluster up/down scaling behavior.

This test should run on a cluster with autoscaling enabled. It assumes 1-3 nodes
with 4 CPUs each.

We start a Ray Tune run with 3 trials. Each trial uses 4 CPUs, so fills up a node
completely. This means we will trigger autoscaling after starting up.

The trial on the head node will run for 30 minutes. This is to make sure that
we have enough time that the nodes for the other two trials come up, complete
training, and come down before the first trial finishes.

The other two trials will run once their nodes are up, and take 3 minutes each
to finish. The three minutes have been chosen to make sure that both trials
run in parallel for some time, i.e. to avoid that both additional trials run on
only one node.

We keep track of the number of nodes we observe at any point during the run.

Test owner: krfricke

Acceptance criteria: Should have scaled to 3 nodes at some point during the run.
Should have scaled down to 1 node at the end.
"""
from collections import Counter
import time

import ray

from ray import train, tune


def train(config):
    this_node_ip = ray.util.get_node_ip_address()
    if config["head_node_ip"] == this_node_ip:
        # On the head node, run for 30 minutes
        for i in range(30):
            train.report({"metric": i})
            time.sleep(60)
    else:
        # On worker nodes, run for 3 minutes
        for i in range(3):
            train.report({"metric": i})
            time.sleep(60)


class NodeCountCallback(tune.Callback):
    def __init__(self):
        self.node_counts = []

    def on_step_begin(self, iteration, trials, **info):
        node_count = len([n for n in ray.nodes() if n["Alive"]])
        self.node_counts.append(node_count)


def main():
    ray.init()

    head_node_ip = ray.util.get_node_ip_address()

    assert (
        len([n for n in ray.nodes() if n["Alive"]]) == 1
    ), "Too many nodes available at start of script"

    node_counter = NodeCountCallback()

    tune.run(
        train,
        num_samples=3,
        config={"head_node_ip": head_node_ip},
        callbacks=[node_counter],
        resources_per_trial={"cpu": 4},
    )

    node_counts = Counter(node_counter.node_counts)
    assert node_counts[3] > 0, "Cluster never scaled to 3 nodes"
    assert node_counter.node_counts[-1] == 1, "Cluster didn't scale down to 1 node."


if __name__ == "__main__":
    main()
