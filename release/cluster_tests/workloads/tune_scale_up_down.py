from collections import Counter
import time

import ray

from ray import tune


def train(config):
    this_node_ip = ray.util.get_node_ip_address()
    if config["head_node_ip"] == this_node_ip:
        # On the head node, run for 30 minutes
        for i in range(30):
            tune.report(metric=i)
            time.sleep(60)
    else:
        # On worker nodes, run for 3 minutes
        for i in range(3):
            tune.report(metric=i)
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
