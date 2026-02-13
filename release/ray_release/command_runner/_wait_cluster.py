import argparse

import ray

ray.init(address="auto")

parser = argparse.ArgumentParser()
parser.add_argument(
    "num_nodes",
    type=int,
    help="Wait until at least this many nodes have joined (includes head)",
)

parser.add_argument("max_time_s", type=int, help="Wait for this number of seconds")

args = parser.parse_args()

actual_nodes = ray.wait_for_nodes(num_nodes=args.num_nodes, timeout=args.max_time_s)
print(f"Cluster is up: {actual_nodes} nodes online")
