# This workload tests many drivers using the same cluster.
import time
import argparse

import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
from ray._private.test_utils import run_string_as_driver, safe_write_to_results_json


def update_progress(result):
    result["last_update"] = time.time()
    safe_write_to_results_json(result)


ray.init()

nodes = [node["NodeID"] for node in ray.nodes()]
assert len(nodes) == 4

# Run the workload.

# Define a driver script that runs a few tasks and actors on each node in the
# cluster.
driver_script = f"""
import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

ray.init()

nodes = "{','.join(nodes)}".split(',')

@ray.remote(num_cpus=1)
def f():
    return 1

@ray.remote(num_cpus=1)
class Actor(object):
    def method(self):
        return 1

for _ in range(5):
    for node in nodes:
        assert ray.get(
            f.options(scheduling_strategy=NodeAffinitySchedulingStrategy(
                node, soft=False)).remote()) == 1
        actor = Actor.options(scheduling_strategy=NodeAffinitySchedulingStrategy(
            node, soft=False)).remote()
        assert ray.get(actor.method.remote()) == 1

print("success")
"""


@ray.remote(num_cpus=0)
def run_driver():
    output = run_string_as_driver(driver_script, encode="utf-8")
    assert "success" in output


iteration = 0
running_ids = [
    run_driver.options(
        scheduling_strategy=NodeAffinitySchedulingStrategy(node, soft=False)
    ).remote()
    for node in nodes
]
start_time = time.time()
previous_time = start_time

parser = argparse.ArgumentParser(prog="Many Drivers long running tests")
parser.add_argument(
    "--iteration-num", type=int, help="How many iterations to run", required=False
)
parser.add_argument(
    "--smoke-test",
    action="store_true",
    help="Whether or not the test is smoke test.",
    default=False,
)
args = parser.parse_args()

iteration_num = args.iteration_num
if args.smoke_test:
    iteration_num = 400
while True:
    if iteration_num is not None and iteration_num < iteration:
        break
    # Wait for a driver to finish and start a new driver.
    [ready_id], running_ids = ray.wait(running_ids, num_returns=1)
    ray.get(ready_id)

    running_ids.append(
        run_driver.options(
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                nodes[iteration % len(nodes)], soft=False
            )
        ).remote()
    )

    new_time = time.time()
    print(
        "Iteration {}:\n"
        "  - Iteration time: {}.\n"
        "  - Absolute time: {}.\n"
        "  - Total elapsed time: {}.".format(
            iteration, new_time - previous_time, new_time, new_time - start_time
        )
    )
    update_progress(
        {
            "iteration": iteration,
            "iteration_time": new_time - previous_time,
            "absolute_time": new_time,
            "elapsed_time": new_time - start_time,
        }
    )
    previous_time = new_time
    iteration += 1
