import argparse
import time

import ray

ray.init(address="auto")

parser = argparse.ArgumentParser()
parser.add_argument(
    "num_nodes", type=int, help="Wait for this number of nodes (includes head)"
)

parser.add_argument("max_time_s", type=int, help="Wait for this number of seconds")

parser.add_argument(
    "--feedback_interval_s",
    type=int,
    default=10,
    help="Wait for this number of seconds",
)

args = parser.parse_args()

curr_nodes = 0
start = time.time()
next_feedback = start
max_time = start + args.max_time_s
while not curr_nodes >= args.num_nodes:
    now = time.time()

    if now >= max_time:
        raise RuntimeError(
            f"Maximum wait time reached, but only "
            f"{curr_nodes}/{args.num_nodes} nodes came up. Aborting."
        )

    if now >= next_feedback:
        passed = now - start
        print(
            f"Waiting for more nodes to come up: "
            f"{curr_nodes}/{args.num_nodes} "
            f"({passed:.0f} seconds passed)"
        )
        next_feedback = now + args.feedback_interval_s

    time.sleep(5)
    curr_nodes = len(ray.nodes())

passed = time.time() - start
print(
    f"Cluster is up: {curr_nodes}/{args.num_nodes} nodes online after "
    f"{passed:.0f} seconds"
)
